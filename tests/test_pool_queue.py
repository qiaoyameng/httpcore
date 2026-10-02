from httpcore._pool import OriginFairQueue, RequestAssignment


def test_origin_fair_queue_rotates_and_skips_capped_origin():
    queue = OriginFairQueue(get_origin=lambda item: item[0], is_queued=lambda item: True)
    requests = [("a", 1), ("a", 2), ("b", 1), ("c", 1)]

    states = iter(
        [
            RequestAssignment.ALLOWED,
            RequestAssignment.ALLOWED,
            RequestAssignment.BLOCKED,
            RequestAssignment.ALLOWED,
        ]
    )

    assert queue.get(requests, lambda request: next(states)) == ("a", 1)
    requests[0] = ("a", 0)
    assert queue.get(requests, lambda request: RequestAssignment.ALLOWED) == ("b", 1)
    requests[2] = ("b", 0)
    assert queue.get(requests, lambda request: RequestAssignment.ALLOWED) == ("c", 1)


def test_origin_fair_queue_timeout_does_not_consume_turn():
    queue = OriginFairQueue(get_origin=lambda item: item[0], is_queued=lambda item: True)
    requests = [("a", True), ("b", True), ("c", True)]

    assert queue.get(requests, lambda request: RequestAssignment.ALLOWED)[0] == "a"
    requests = [("a", True), ("c", True)]

    assert queue.get(requests, lambda request: RequestAssignment.ALLOWED)[0] == "c"
