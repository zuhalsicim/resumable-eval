import asyncio
from dataclasses import replace

from temporalio.api.enums.v1 import TimeoutType

from resumable_eval.faults import planned_fault
from resumable_eval.harness import CrashTest, crash_test, local_server
from resumable_eval.shared import NO_FAULTS, PRIMARY, Faults, RunConfig


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


def test_two_crashes_during_one_questions_retries_do_not_change_its_answer(tmp_path):
    # Every question fails its first one or two attempts. Question q fails two, so it answers on attempt 3.
    faults = Faults(dead_rate=0.0, flaky_rate=1.0, hang_share=0.0)
    cfg = RunConfig("", questions=8, budget=10.0, faults=faults)
    q = next(q for q in range(8) if planned_fault(cfg.seed, PRIMARY.name, q, 2, faults))
    # The worker dies right after the provider answered q on attempt 3, and the next worker after attempt 4.
    test = run_crash_test(cfg, tmp_path / "results.db", exit_after_answer=q, exits=2)
    assert test.passed
    assert [a for question_id, _, a in test.report.calls if question_id == q] == [3, 4, 5]

    # With the four attempts of version 1.0, the same two crashes leave no attempt: q goes to the fallback.
    test = run_crash_test(replace(cfg, max_attempts=4), tmp_path / "results.db", exit_after_answer=q, exits=2)
    assert not test.identical
    assert [r.used_fallback for r in test.report.results if r.question_id == q] == [True]


def test_a_batch_resumes_from_its_last_heartbeat_after_the_worker_dies(tmp_path):
    # One long activity for 10 questions, 0.3 s each, with a heartbeat after each question.
    # The worker dies right after the provider answered question 6.
    cfg = RunConfig("", questions=10, budget=10.0, faults=NO_FAULTS, batch_size=10, seconds_per_batch_question=0.3)
    test = run_crash_test(cfg, tmp_path / "results.db", exit_after_answer=6)
    assert test.passed

    first = {q for q, _, attempt in test.report.calls if attempt == 1}
    second = {q for q, _, attempt in test.report.calls if attempt == 2}
    resumed_at = min(second)
    print(f"batch: attempt 1 answered questions {sorted(first)}, attempt 2 resumed at question {resumed_at}")
    assert first == set(range(7))
    assert 0 < resumed_at <= 6  # not the start of the batch: the heartbeat details carried the answers so far
    assert second == set(range(resumed_at, 10))

    # The server noticed the dead worker by its missing heartbeats, not by the 10-minute start-to-close timeout.
    started = next(e for e in test.report.history.events if e.HasField("activity_task_started_event_attributes"))
    started = started.activity_task_started_event_attributes
    assert started.attempt == 2
    assert started.last_failure.timeout_failure_info.timeout_type == TimeoutType.TIMEOUT_TYPE_HEARTBEAT
    assert test.seconds < 60


def test_two_workers_share_the_run_and_one_of_them_is_killed(tmp_path):
    # Four questions in flight, two worker processes on one task queue; the first one is killed.
    # The result must equal an uninterrupted run that asks one question at a time.
    cfg = RunConfig("", questions=100, budget=10.0, in_flight=4)
    test = run_crash_test(cfg, tmp_path / "results.db", baseline=replace(cfg, in_flight=1), crash_after=40, workers=2)
    assert test.passed
    assert len(test.report.workers) == 2  # both workers answered questions
