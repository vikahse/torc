from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, HttpUrl
from typing import Dict, Any, List, Optional, Set, Tuple
import httpx
import asyncio
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime
import json
import os
import logging
from contextlib import asynccontextmanager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('crdt.log')
    ]
)
logger = logging.getLogger("CRDT")


@dataclass
class VectorClock:
    clock: Dict[str, int]

    def __init__(self):
        self.clock = {}

    def increment(self, replica_id: str):
        self.clock[replica_id] = self.clock.get(replica_id, 0) + 1
        logger.debug(f"Vector clock incremented for {replica_id}: {self.clock}")

    def merge(self, other: 'VectorClock'):
        old_clock = self.clock.copy()
        for replica_id, count in other.clock.items():
            self.clock[replica_id] = max(self.clock.get(replica_id, 0), count)
        if old_clock != self.clock:
            logger.debug(f"Vector clock merged: {old_clock} -> {self.clock}")

    def is_concurrent_with(self, other: 'VectorClock') -> bool:
        result = not (self.happens_before(other) or other.happens_before(self))
        if result:
            logger.debug(f"Concurrent operations detected: {self.clock} || {other.clock}")
        return result

    def happens_before(self, other: 'VectorClock') -> bool:
        at_least_one_less = False
        for replica_id, count in self.clock.items():
            other_count = other.clock.get(replica_id, 0)
            if count > other_count:
                return False
            if count < other_count:
                at_least_one_less = True

        for replica_id in other.clock:
            if replica_id not in self.clock:
                at_least_one_less = True

        if at_least_one_less:
            logger.debug(f"Happens before relation: {self.clock} -> {other.clock}")
        return at_least_one_less

    def is_equal(self, other: 'VectorClock') -> bool:
        return self.clock == other.clock

    def copy(self) -> 'VectorClock':
        new_clock = VectorClock()
        new_clock.clock = self.clock.copy()
        return new_clock


@dataclass
class Operation:
    operation_id: str
    replica_id: str
    key: str
    value: Optional[Any]
    vector_clock: VectorClock
    is_delete: bool = False
    dependencies: Set[str] = None

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = set()
        logger.info(f"New operation created: {self.operation_id} - {'DELETE' if self.is_delete else 'UPDATE'} "
                    f"key={self.key}, value={self.value}, replica={self.replica_id}")
        logger.debug(f"Operation details: vc={self.vector_clock.clock}, deps={self.dependencies}")

    def to_dict(self):
        return {
            "operation_id": self.operation_id,
            "replica_id": self.replica_id,
            "key": self.key,
            "value": self.value,
            "vector_clock": self.vector_clock.clock,
            "is_delete": self.is_delete,
            "dependencies": list(self.dependencies)
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'Operation':
        vc = VectorClock()
        vc.clock = data["vector_clock"]
        return Operation(
            operation_id=data["operation_id"],
            replica_id=data["replica_id"],
            key=data["key"],
            value=data["value"],
            vector_clock=vc,
            is_delete=data["is_delete"],
            dependencies=set(data.get("dependencies", []))
        )


class LWWMap:
    def __init__(self, replica_id: str):
        self.replica_id = replica_id
        self.vector_clock = VectorClock()
        self.operations: List[Operation] = []
        self.known_operations: Set[str] = set()
        self.pending_operations: List[Operation] = []
        logger.info(f"New LWWMap replica initialized: {replica_id}")

    def get_state(self) -> Dict[str, Any]:
        result = {}
        for op in self._get_sorted_operations():
            if op.is_delete:
                result.pop(op.key, None)
                logger.debug(f"State update (delete): {op.key} removed")
            else:
                result[op.key] = op.value
                logger.debug(f"State update: {op.key} = {op.value}")
        logger.info(f"Current state: {result}")
        return result

    def _get_sorted_operations(self) -> List[Operation]:
        sorted_ops = []
        visited = set()
        temp_visited = set()

        def visit(op_id: str):
            if op_id in temp_visited:
                logger.error(f"Cycle detected in operation dependencies: {op_id}")
                raise Exception("Cycle detected in operation dependencies")
            if op_id in visited:
                return

            temp_visited.add(op_id)
            op = next(op for op in self.operations if op.operation_id == op_id)

            for dep_id in op.dependencies:
                visit(dep_id)

            temp_visited.remove(op_id)
            visited.add(op_id)
            sorted_ops.append(op)
            logger.debug(f"Operation sorted: {op_id}")

        ops_by_vc = sorted(self.operations,
                           key=lambda op: (sum(op.vector_clock.clock.values()), op.operation_id))

        for op in ops_by_vc:
            if op.operation_id not in visited:
                visit(op.operation_id)

        logger.debug(f"Operations sorted: {[op.operation_id for op in sorted_ops]}")
        return sorted_ops

    def _check_dependencies_satisfied(self, operation: Operation) -> bool:
        satisfied = all(dep in self.known_operations for dep in operation.dependencies)
        if not satisfied:
            missing = [dep for dep in operation.dependencies if dep not in self.known_operations]
            logger.debug(f"Dependencies not satisfied for {operation.operation_id}. Missing: {missing}")
        return satisfied

    def apply_operation(self, operation: Operation) -> bool:
        if operation.operation_id in self.known_operations:
            logger.debug(f"Operation already known: {operation.operation_id}")
            return False

        if not self._check_dependencies_satisfied(operation):
            logger.info(f"Operation {operation.operation_id} added to pending operations")
            self.pending_operations.append(operation)
            return False

        self.operations.append(operation)
        self.known_operations.add(operation.operation_id)
        self.vector_clock.merge(operation.vector_clock)
        logger.info(f"Operation applied: {operation.operation_id}")

        self._process_pending_operations()
        return True

    def _process_pending_operations(self):
        initial_pending = len(self.pending_operations)
        applied = True
        while applied:
            applied = False
            remaining_pending = []
            for op in self.pending_operations:
                if self._check_dependencies_satisfied(op):
                    self.operations.append(op)
                    self.known_operations.add(op.operation_id)
                    self.vector_clock.merge(op.vector_clock)
                    applied = True
                    logger.info(f"Pending operation applied: {op.operation_id}")
                else:
                    remaining_pending.append(op)
            self.pending_operations = remaining_pending

        if initial_pending != len(self.pending_operations):
            logger.info(f"Processed pending operations: {initial_pending} -> {len(self.pending_operations)} remaining")

    def update(self, key: str, value: Any, is_delete: bool = False) -> Operation:
        self.vector_clock.increment(self.replica_id)

        dependencies = {op.operation_id for op in self.operations
                        if op.key == key}

        operation = Operation(
            operation_id=str(uuid.uuid4()),
            replica_id=self.replica_id,
            key=key,
            value=value,
            vector_clock=self.vector_clock.copy(),
            is_delete=is_delete,
            dependencies=dependencies
        )

        self.apply_operation(operation)
        return operation

    def get_operations_since(self, since_vector_clock: Optional[Dict[str, int]] = None) -> List[Operation]:
        if since_vector_clock is None:
            return self.operations

        vc = VectorClock()
        vc.clock = since_vector_clock

        result = [op for op in self.operations
                  if not op.vector_clock.happens_before(vc)]
        logger.debug(f"Getting operations since {since_vector_clock}: found {len(result)} operations")
        return result


class ReplicaInfo(BaseModel):
    url: str
    last_seen: float = 0.0
    vector_clock: Dict[str, int] = {}


class ReplicaState(BaseModel):
    operations: List[Dict[str, Any]]
    vector_clock: Dict[str, int]
    url: Optional[str] = None


class PatchRequest(BaseModel):
    updates: Dict[str, Any]


class RegisterRequest(BaseModel):
    url: str


replica_id = str(uuid.uuid4())
crdt = LWWMap(replica_id)
replicas: Dict[str, ReplicaInfo] = {}
health_check_interval = 5
sync_interval = 10

logger.info(f"Server starting with replica_id: {replica_id}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(periodic_health_check())
    asyncio.create_task(periodic_sync())
    yield


app = FastAPI(lifespan=lifespan)


async def periodic_health_check():
    while True:
        await asyncio.sleep(health_check_interval)
        current_time = datetime.now().timestamp()
        dead_replicas = []

        for replica_url, info in replicas.items():
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(f"{replica_url}/health", timeout=2.0)
                    if response.status_code == 200:
                        replicas[replica_url].last_seen = current_time
                        logger.debug(f"Health check passed for {replica_url}")
                    else:
                        if current_time - info.last_seen > health_check_interval * 3:
                            dead_replicas.append(replica_url)
                            logger.warning(f"Replica not responding: {replica_url}")
            except:
                if current_time - info.last_seen > health_check_interval * 3:
                    dead_replicas.append(replica_url)
                    logger.warning(f"Failed to connect to replica: {replica_url}")

        for url in dead_replicas:
            replicas.pop(url, None)
            logger.info(f"Removed dead replica: {url}")


async def periodic_sync():
    while True:
        await asyncio.sleep(sync_interval)
        for replica_url, info in replicas.items():
            try:
                await sync_with_replica(replica_url, info.vector_clock)
                logger.debug(f"Periodic sync completed with {replica_url}")
            except Exception as e:
                logger.error(f"Failed to sync with replica {replica_url}: {e}")


async def sync_with_replica(replica_url: str, since_vector_clock: Optional[Dict[str, int]] = None):
    operations = crdt.get_operations_since(since_vector_clock)
    if not operations:
        logger.debug(f"No operations to sync with {replica_url}")
        return

    logger.info(f"Syncing {len(operations)} operations with {replica_url}")
    async with httpx.AsyncClient() as client:
        await client.post(
            f"{replica_url}/sync",
            json={
                "operations": [op.to_dict() for op in operations],
                "vector_clock": crdt.vector_clock.clock,
                "url": f"http://localhost:{port}"
            }
        )


@app.post("/register")
async def register_replica(request: RegisterRequest):
    if request.url not in replicas:
        replicas[request.url] = ReplicaInfo(
            url=request.url,
            last_seen=datetime.now().timestamp(),
            vector_clock={}
        )
        logger.info(f"New replica registered: {request.url}")
        await sync_with_replica(request.url)
    return {"status": "success", "replica_id": replica_id}


@app.patch("/update")
async def update_values(patch: PatchRequest):
    logger.info(f"Received update request: {patch.updates}")
    operations = []
    for key, value in patch.updates.items():
        operation = crdt.update(key, value)
        operations.append(operation)

    await broadcast_operations(operations)
    return {"status": "success", "state": crdt.get_state()}


@app.delete("/delete/{key}")
async def delete_value(key: str):
    logger.info(f"Received delete request for key: {key}")
    operation = crdt.update(key, None, is_delete=True)
    await broadcast_operations([operation])
    return {"status": "success", "state": crdt.get_state()}


@app.get("/get/{key}")
async def get_value(key: str):
    logger.info(f"Received get request for key: {key}")
    state = crdt.get_state()
    if key not in state.keys():
        return {"status": "success", "value": None}
    return {"status": "success", "key": key, "value": state[key]}


@app.post("/sync")
async def sync_state(state: ReplicaState):
    logger.info(f"Received sync request with {len(state.operations)} operations from {state.url}")
    for op_dict in state.operations:
        op = Operation.from_dict(op_dict)
        crdt.apply_operation(op)

    if state.vector_clock and state.url:
        replicas[state.url].vector_clock = state.vector_clock
        logger.debug(f"Updated vector clock for {state.url}: {state.vector_clock}")

    return {"status": "success", "state": crdt.get_state()}


@app.get("/state")
async def get_state():
    state = crdt.get_state()
    logger.debug(f"State requested: {state}")
    return state


@app.get("/health")
async def health_check():
    return {"status": "healthy", "replica_id": replica_id}


async def broadcast_operations(operations: List[Operation]):
    logger.info(f"Broadcasting {len(operations)} operations to {len(replicas)} replicas")
    for replica_url in replicas:
        try:
            async with httpx.AsyncClient() as client:
                await client.post(
                    f"{replica_url}/sync",
                    json={
                        "operations": [op.to_dict() for op in operations],
                        "vector_clock": crdt.vector_clock.clock,
                        "url": f"http://localhost:{port}"
                    }
                )
                logger.debug(f"Successfully broadcast to {replica_url}")
        except Exception as e:
            logger.error(f"Failed to broadcast to {replica_url}: {e}")


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    logger.info(f"Starting server on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
