import pytest
import asyncio
import random
from typing import Dict, List, Optional, Any
from server import Operation, VectorClock, LWWMap


class DelayedNetwork:
    def __init__(self, min_delay: float = 0.1, max_delay: float = 2.0, drop_probability: float = 0.1):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.drop_probability = drop_probability
        self.connected = True
        self.message_queue: List[tuple] = []

    async def send(self, source: LWWMap, target: LWWMap, operation: Operation) -> bool:
        if not self.connected or random.random() < self.drop_probability:
            return False

        delay = random.uniform(self.min_delay, self.max_delay)
        await asyncio.sleep(delay)

        return target.apply_operation(operation)


class DelayedReplica:
    def __init__(self, replica_id: str, network: DelayedNetwork):
        self.crdt = LWWMap(replica_id)
        self.network = network
        self.peers: List['DelayedReplica'] = []
        self.pending_sync: List[Operation] = []
        self._sync_task = None
        self._broadcast_task = None
        self._running = True

    async def start(self):
        self._sync_task = asyncio.create_task(self._periodic_sync())
        self._broadcast_task = asyncio.create_task(self._periodic_broadcast())

    async def stop(self):
        self._running = False
        for task in [self._sync_task, self._broadcast_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def _periodic_sync(self):
        while self._running:
            try:
                if self.network.connected and self.pending_sync:
                    await self.sync_after_reconnect()
                await asyncio.sleep(1.0)
            except Exception as e:
                print(f"Error in periodic sync: {e}")

    async def _periodic_broadcast(self):
        while self._running:
            try:
                if self.network.connected:
                    for peer in self.peers:
                        peer_state = peer.get_state()
                        my_state = self.crdt.get_state()

                        for key, value in my_state.items():
                            if key not in peer_state or peer_state[key] != value:
                                matching_ops = [op for op in self.crdt.operations
                                                if op.key == key and op.value == value]
                                if matching_ops:
                                    await self.network.send(self.crdt, peer.crdt, matching_ops[-1])

                        for key, value in peer_state.items():
                            if key not in my_state or my_state[key] != value:
                                await self.sync_after_reconnect()
                                break

                await asyncio.sleep(2.0)
            except Exception as e:
                print(f"Error in periodic broadcast: {e}")

    async def update(self, key: str, value: Any, is_delete: bool = False):
        operation = self.crdt.update(key, value, is_delete)
        if self.network.connected:
            await self.broadcast_operation(operation)
        else:
            self.pending_sync.append(operation)
        return operation

    async def broadcast_operation(self, operation: Operation):
        tasks = []
        for peer in self.peers:
            task = asyncio.create_task(
                self.network.send(self.crdt, peer.crdt, operation)
            )
            tasks.append(task)
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            retries = []
            for i, result in enumerate(results):
                if isinstance(result, Exception) or result is False:
                    retries.append(self.peers[i])
            if retries:
                self.pending_sync.append(operation)

    def get_state(self):
        return self.crdt.get_state()

    def add_peer(self, peer: 'DelayedReplica'):
        if peer not in self.peers:
            self.peers.append(peer)

    async def sync_after_reconnect(self):
        if not self.pending_sync and not self.peers:
            return

        tasks = []

        for operation in self.pending_sync:
            for peer in self.peers:
                task = asyncio.create_task(
                    self.network.send(self.crdt, peer.crdt, operation)
                )
                tasks.append((operation, task))

        for peer in self.peers:
            peer_state = peer.get_state()
            my_state = self.crdt.get_state()

            for key, value in peer_state.items():
                if key not in my_state or my_state[key] != value:
                    peer_ops = [op for op in peer.crdt.operations
                                if op.key == key and op.value == value]
                    if peer_ops:
                        task = asyncio.create_task(
                            self.network.send(peer.crdt, self.crdt, peer_ops[-1])
                        )
                        tasks.append((None, task))

        if tasks:
            remaining_ops = []
            for op, task in tasks:
                try:
                    result = await task
                    if isinstance(result, Exception) or result is False:
                        if op is not None:
                            remaining_ops.append(op)
                except Exception:
                    if op is not None:
                        remaining_ops.append(op)

            self.pending_sync = remaining_ops


async def setup_replicas(*replicas: DelayedReplica):
    for r1 in replicas:
        for r2 in replicas:
            if r1 != r2:
                r1.add_peer(r2)
        await r1.start()


async def cleanup_replicas(*replicas: DelayedReplica):
    for replica in replicas:
        await replica.stop()


@pytest.mark.asyncio
async def test_concurrent_updates_with_delays():
    network = DelayedNetwork(min_delay=0.1, max_delay=0.5)
    replica1 = DelayedReplica("replica1", network)
    replica2 = DelayedReplica("replica2", network)

    await setup_replicas(replica1, replica2)

    try:
        await asyncio.gather(
            replica1.update("key1", "value1"),
            replica2.update("key1", "value2")
        )

        await asyncio.sleep(3.0)
        assert replica1.get_state() == replica2.get_state()
    finally:
        await cleanup_replicas(replica1, replica2)


@pytest.mark.asyncio
async def test_network_partition():
    network = DelayedNetwork(min_delay=0.1, max_delay=0.5)
    replica1 = DelayedReplica("replica1", network)
    replica2 = DelayedReplica("replica2", network)

    replica1.add_peer(replica2)
    replica2.add_peer(replica1)

    await replica1.update("key1", "value1")
    await asyncio.sleep(1.0)
    assert replica1.get_state() == replica2.get_state()

    network.connected = False

    await asyncio.gather(
        replica1.update("key2", "value2"),
        replica2.update("key3", "value3")
    )

    assert replica1.get_state() != replica2.get_state()

    network.connected = True
    await replica1.sync_after_reconnect()
    await replica2.sync_after_reconnect()
    await asyncio.sleep(2.0)

    assert replica1.get_state() == replica2.get_state()
    state = replica1.get_state()
    assert "key1" in state
    assert "key2" in state
    assert "key3" in state


@pytest.mark.asyncio
async def test_message_reordering():
    network = DelayedNetwork(min_delay=0.1, max_delay=1.0)
    replica1 = DelayedReplica("replica1", network)
    replica2 = DelayedReplica("replica2", network)

    await setup_replicas(replica1, replica2)

    try:
        ops = []
        for i in range(5):
            ops.append(replica1.update(f"key{i}", f"value{i}"))

        await asyncio.gather(*ops)
        await asyncio.sleep(2.0)

        assert replica1.get_state() == replica2.get_state()
    finally:
        await cleanup_replicas(replica1, replica2)


@pytest.mark.asyncio
async def test_high_latency():
    network = DelayedNetwork(min_delay=1.0, max_delay=3.0)
    replica1 = DelayedReplica("replica1", network)
    replica2 = DelayedReplica("replica2", network)

    await setup_replicas(replica1, replica2)

    try:
        network.connected = False
        ops1 = []
        for i in range(5):
            op = await replica1.update(f"key{i}", f"value{i}")
            ops1.append(op)

        network.connected = True
        await asyncio.sleep(4.0)

        network.connected = False
        ops2 = []
        for i in range(5):
            op = await replica2.update(f"key{i}", f"new_value{i}")
            ops2.append(op)

        network.connected = True
        await asyncio.sleep(6.0)

        assert replica1.get_state() == replica2.get_state()
        state = replica1.get_state()
        for i in range(5):
            assert state[f"key{i}"] == f"new_value{i}"
    finally:
        await cleanup_replicas(replica1, replica2)


@pytest.mark.asyncio
async def test_packet_loss():
    network = DelayedNetwork(min_delay=0.1, max_delay=0.5, drop_probability=0.5)
    replica1 = DelayedReplica("replica1", network)
    replica2 = DelayedReplica("replica2", network)

    await setup_replicas(replica1, replica2)

    try:
        for i in range(10):
            await replica1.update(f"key{i}", f"value{i}")
            await asyncio.sleep(0.2)

        await asyncio.sleep(5.0)
        assert replica1.get_state() == replica2.get_state()
    finally:
        await cleanup_replicas(replica1, replica2)


@pytest.mark.asyncio
async def test_causal_order_with_delays():
    network = DelayedNetwork(min_delay=0.5, max_delay=2.0)
    replica1 = DelayedReplica("replica1", network)
    replica2 = DelayedReplica("replica2", network)
    replica3 = DelayedReplica("replica3", network)

    await setup_replicas(replica1, replica2, replica3)

    try:
        network.connected = False
        op1 = await replica1.update("key", "value1")
        network.connected = True
        await asyncio.sleep(3.0)

        state = replica1.get_state()
        assert replica2.get_state() == state
        assert replica3.get_state() == state
        assert state["key"] == "value1"

        network.connected = False
        op2 = await replica2.update("key", "value2")
        network.connected = True
        await asyncio.sleep(3.0)

        state = replica2.get_state()
        assert replica1.get_state() == state
        assert replica3.get_state() == state
        assert state["key"] == "value2"

        network.connected = False
        op3 = await replica3.update("key", "value3")
        network.connected = True
        await asyncio.sleep(5.0)

        state = replica3.get_state()
        assert replica1.get_state() == state
        assert replica2.get_state() == state
        assert state["key"] == "value3"

        assert op1.operation_id in op2.dependencies
        assert op2.operation_id in op3.dependencies
    finally:
        await cleanup_replicas(replica1, replica2, replica3)


if __name__ == "__main__":
    pytest.main([__file__])
