import pytest
from server import Operation, VectorClock, LWWMap


@pytest.fixture
def crdt_map1():
    return LWWMap("replica1")


@pytest.fixture
def crdt_map2():
    return LWWMap("replica2")


@pytest.fixture
def crdt_map3():
    return LWWMap("replica3")


def test_concurrent_updates():
    map1 = LWWMap("replica1")
    map2 = LWWMap("replica2")

    op1 = map1.update("key1", "value1")
    op2 = map2.update("key1", "value2")

    map1.apply_operation(op2)
    map2.apply_operation(op1)

    assert map1.get_state() == map2.get_state()


def test_causal_ordering():
    map1 = LWWMap("replica1")
    map2 = LWWMap("replica2")

    op1 = map1.update("key1", "value1")
    map2.apply_operation(op1)
    op2 = map2.update("key1", "value2")

    # зависимость между операциями в контексте каузального порядка
    assert op1.operation_id in op2.dependencies

    map1.apply_operation(op2)

    assert map1.get_state() == map2.get_state()
    assert map1.get_state()["key1"] == "value2"


def test_delete_operation():
    map1 = LWWMap("replica1")
    map2 = LWWMap("replica2")

    op1 = map1.update("key1", "value1")
    map2.apply_operation(op1)

    op2 = map2.update("key1", None, is_delete=True)
    map1.apply_operation(op2)

    assert "key1" not in map1.get_state()
    assert "key1" not in map2.get_state()


def test_concurrent_delete_and_update():
    map1 = LWWMap("replica1")
    map2 = LWWMap("replica2")

    op1 = map1.update("key1", "value1")
    map2.apply_operation(op1)

    op2 = map1.update("key1", None, is_delete=True)
    op3 = map2.update("key1", "value2")

    map1.apply_operation(op3)
    map2.apply_operation(op2)

    assert map1.get_state() == map2.get_state()


def test_multiple_replicas_sync():
    map1 = LWWMap("replica1")
    map2 = LWWMap("replica2")
    map3 = LWWMap("replica3")

    op1 = map1.update("key1", "value1")
    op2 = map2.update("key2", "value2")
    op3 = map3.update("key3", "value3")

    maps = [map1, map2, map3]
    ops = [op1, op2, op3]

    for m in maps:
        for op in ops:
            if op.replica_id != m.replica_id:
                m.apply_operation(op)

    state = map1.get_state()
    assert map2.get_state() == state
    assert map3.get_state() == state
    assert len(state) == 3


def test_vector_clock_properties():
    vc1 = VectorClock()
    vc2 = VectorClock()

    assert not vc1.happens_before(vc2)
    assert not vc2.happens_before(vc1)

    vc1.increment("replica1")
    assert vc1.happens_before(vc2) == False
    assert vc2.happens_before(vc1) == True

    vc2.merge(vc1)
    assert not vc1.happens_before(vc2)
    assert not vc2.happens_before(vc1)


def test_pending_operations():
    map1 = LWWMap("replica1")

    op = Operation(
        operation_id="op2",
        replica_id="replica2",
        key="key1",
        value="value2",
        vector_clock=VectorClock(),
        dependencies={"non-existent-op"}
    )

    success = map1.apply_operation(op)
    assert not success
    assert op in map1.pending_operations

    dep_op = Operation(
        operation_id="non-existent-op",
        replica_id="replica1",
        key="key1",
        value="value1",
        vector_clock=VectorClock(),
        dependencies=set()
    )
    map1.apply_operation(dep_op)

    assert op not in map1.pending_operations
    assert "key1" in map1.get_state()
    assert map1.get_state()["key1"] == "value2"


def test_concurrent_operations_on_one_replica():
    map1 = LWWMap("replica1")

    vc_second = VectorClock()
    vc_second.clock = {"replica1": 2}

    second_op = Operation(
        operation_id="op2",
        replica_id="replica1",
        key="key1",
        value="value2",
        vector_clock=vc_second,
    )

    success = map1.apply_operation(second_op)
    assert success

    vc_first = VectorClock()
    vc_first.clock = {"replica1": 1}

    first_op = Operation(
        operation_id="op1",
        replica_id="replica1",
        key="key1",
        value="value1",
        vector_clock=vc_first,
        dependencies=set()
    )

    success = map1.apply_operation(first_op)
    assert success

    assert "key1" in map1.get_state()
    assert map1.get_state()["key1"] == "value2"


def test_concurrent_operations_on_multiple_replicas():
    map1 = LWWMap("replica1")
    map2 = LWWMap("replica1")

    vc_second = VectorClock()
    vc_second.clock = {"replica1": 2}

    second_op = Operation(
        operation_id="op2",
        replica_id="replica1",
        key="key1",
        value="value2",
        vector_clock=vc_second,
    )

    success = map1.apply_operation(second_op)
    assert success
    success = map2.apply_operation(second_op)
    assert success

    vc_first = VectorClock()
    vc_first.clock = {"replica1": 1}

    first_op = Operation(
        operation_id="op1",
        replica_id="replica1",
        key="key1",
        value="value1",
        vector_clock=vc_first,
        dependencies=set()
    )

    success = map1.apply_operation(first_op)
    assert success
    success = map2.apply_operation(first_op)
    assert success

    assert "key1" in map1.get_state()
    assert "key1" in map2.get_state()
    assert map1.get_state()["key1"] == "value2"
    assert map2.get_state()["key1"] == "value2"
    assert map1.get_state() == map2.get_state()


@pytest.mark.asyncio
async def test_network_partition_simulation():
    map1 = LWWMap("replica1")
    map2 = LWWMap("replica2")

    op1 = map1.update("key1", "value1")
    op2 = map2.update("key1", "value2")

    map1.apply_operation(op2)
    map2.apply_operation(op1)

    assert map1.get_state() == map2.get_state()


def test_strong_eventual_consistency():
    maps = [LWWMap(f"replica{i}") for i in range(3)]
    operations = []

    for i, m in enumerate(maps):
        operations.append(m.update(f"key{i}", f"value{i}"))

    for m in maps:
        for op in reversed(operations):
            if op.replica_id != m.replica_id:
                m.apply_operation(op)

    state = maps[0].get_state()
    for m in maps[1:]:
        assert m.get_state() == state


if __name__ == "__main__":
    pytest.main([__file__])
