import asyncio

from resumable_eval.harness import CrashTest, crash_test, local_server
from resumable_eval.shared import NO_FAULTS, RunConfig


def run_crash_test(cfg: RunConfig, db, **crash) -> CrashTest:
    async def main() -> CrashTest:
        async with await local_server() as env:
            return await crash_test(env.client, cfg, db, **crash)

    test = asyncio.run(main())
    print(test.line())
    return test


def test_a_killed_worker_is_replaced_and_the_run_ends_as_if_nothing_happened(tmp_path):
    test = run_crash_test(RunConfig("", questions=200, budget=10.0), tmp_path / "results.db", crash_after=80)
    assert test.passed
    assert test.crashed_after >= 80


def test_an_answer_lost_before_it_reached_temporal_is_asked_again_but_counted_once(tmp_path):
    # The worker dies right after the provider answered question 4, before it could report the answer.
    cfg = RunConfig("", questions=10, budget=10.0, faults=NO_FAULTS)
    test = run_crash_test(cfg, tmp_path / "results.db", exit_after_answer=4)
    assert test.passed
    assert test.crashed_after == 4
    assert (test.calls, test.repeated) == (11, 1)
