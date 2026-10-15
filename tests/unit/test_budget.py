from agent.budget import loop_detected


def row(tool="check_api_health", h="abc", ok=True):
    return {"tool": tool, "args_hash": h, "ok": ok}


def test_repeated_successful_reads_are_not_a_loop():
    assert loop_detected({"tool_log": [row()] * 5}) is None


def test_repeated_failures_are_a_loop():
    reason = loop_detected({"tool_log": [row("get_task_log", ok=False)] * 3})
    assert reason and "get_task_log called 3 times" in reason


def test_different_args_are_not_a_loop():
    log = [row("get_task_log", h=str(i), ok=False) for i in range(5)]
    assert loop_detected({"tool_log": log}) is None
