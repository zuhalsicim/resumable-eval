import asyncio
import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

from temporalio.client import WorkflowHistory

from resumable_eval import store
from resumable_eval.activities import recorded_answers
from resumable_eval.budget import worst_case
from resumable_eval.faults import planned_fault
from resumable_eval.harness import local_server
from resumable_eval.shared import FALLBACK, NO_FAULTS, PRIMARY, Faults, RunConfig, Summary, cost
from resumable_eval.worker import make_worker
from resumable_eval.workflow import EvalWorkflow


def run_with_history(cfg: RunConfig, db: str) -> tuple[Summary, WorkflowHistory]:
    """Run the workflow on a local server with a worker in this process."""

    async def main() -> tuple[Summary, WorkflowHistory]:
        async with await local_server() as env:
            with ThreadPoolExecutor() as executor:
                async with make_worker(env.client, "tests", db, executor):
                    handle = await env.client.start_workflow(EvalWorkflow.run, cfg, id=cfg.run_id, task_queue="tests")
                    return await handle.result(), await handle.fetch_history()

    return asyncio.run(main())


def run(cfg: RunConfig, db: str) -> Summary:
    return run_with_history(cfg, db)[0]


def rows(db: str, run_id: str) -> list[tuple]:
    return [(r.provider, r.sql, r.cost) for r in store.results(db, run_id)]


def primary_costs(questions: int) -> list[float]:
    recorded = recorded_answers(PRIMARY.name)
    return [cost(recorded[q]["prompt_tokens"], recorded[q]["generated_tokens"], PRIMARY) for q in range(questions)]


def test_failed_calls_are_retried_until_they_answer(tmp_path):
    db = str(tmp_path / "results.db")
    cfg = RunConfig("flaky", questions=6, budget=1.0, faults=Faults(dead_rate=0.0, flaky_rate=1.0, hang_share=0.5))
    summary = run(cfg, db)

    assert summary.status == "completed"
    assert summary.used_fallback == 0
    recorded = recorded_answers(PRIMARY.name)
    for r in store.results(db, "flaky"):
        planned = [planned_fault(cfg.seed, PRIMARY.name, r.question_id, a, cfg.faults) for a in range(1, 6)]
        assert "hang" in planned or "error" in planned
        assert r.attempt == planned.index(None) + 1  # the first attempt without a fault gave the answer
        assert (r.provider, r.sql) == (PRIMARY.name, recorded[r.question_id]["sql"])


def test_the_fallback_answers_when_the_primary_gives_up(tmp_path):
    db = str(tmp_path / "results.db")
    summary = run(RunConfig("dead", questions=3, budget=1.0, faults=Faults(dead_rate=1.0)), db)

    assert summary.status == "completed"
    assert summary.used_fallback == 3
    recorded = recorded_answers(FALLBACK.name)
    for r in store.results(db, "dead"):
        answer = recorded[r.question_id]
        assert (r.provider, r.used_fallback, r.sql) == (FALLBACK.name, True, answer["sql"])
        assert r.cost == cost(answer["prompt_tokens"], answer["generated_tokens"], FALLBACK)


def test_a_question_both_providers_give_up_on_is_recorded_as_unanswered_and_the_run_goes_on(tmp_path):
    db = str(tmp_path / "results.db")
    # The primary is dead on every question; the fallback is dead on half of them.
    cfg = RunConfig("both-dead", questions=4, budget=1.0, first_retry_seconds=0.01, faults=Faults(dead_rate=1.0))
    cfg = replace(cfg, fallback_faults=Faults(dead_rate=0.5, flaky_rate=0.0))
    summary = run(cfg, db)

    gave_up = [q for q in range(4) if planned_fault(cfg.seed, FALLBACK.name, q, 5, cfg.fallback_faults)]
    assert 0 < len(gave_up) < 4
    answered = 4 - len(gave_up)
    assert summary.status == "completed"
    assert (summary.answered, summary.used_fallback, summary.unanswered) == (answered, answered, len(gave_up))
    results = store.results(db, "both-dead")
    assert [r.question_id for r in results] == list(range(4))  # every question has its row, answered or not
    assert {r.question_id: r.error for r in results if r.error} == dict.fromkeys(gave_up, "ProviderError")
    assert all((r.provider, r.sql, r.cost) == ("", "", 0.0) for r in results if r.error)


def test_the_budget_reserves_the_worst_case_and_the_spend_never_passes_it(tmp_path):
    db = str(tmp_path / "results.db")
    costs = primary_costs(20)
    worst = worst_case(RunConfig("", 1, 1.0))
    budget = worst + sum(costs[:5]) - costs[4] / 2  # after 5 answers, the worst case of a sixth no longer fits
    summary = run(RunConfig("budget", questions=20, budget=budget, faults=NO_FAULTS), db)

    assert summary.status == "budget exhausted after question 5"
    assert [r.question_id for r in store.results(db, "budget")] == [0, 1, 2, 3, 4]
    assert math.isclose(summary.spent, sum(costs[:5]))
    assert summary.spent <= budget


def test_questions_in_flight_at_once_give_the_same_results_as_one_at_a_time(tmp_path):
    db = str(tmp_path / "results.db")
    one = run(RunConfig("one-at-a-time", questions=40, budget=1.0), db)
    four = run(RunConfig("four-at-a-time", questions=40, budget=1.0, in_flight=4), db)

    assert replace(four, run_id="") == replace(one, run_id="")
    assert rows(db, "four-at-a-time") == rows(db, "one-at-a-time")


def test_batch_mode_gives_the_same_results_as_one_activity_per_question(tmp_path):
    # Questions that fail inside a batch are asked again on their own, with retries and the fallback.
    db = str(tmp_path / "results.db")
    cfg = RunConfig("per-question", questions=30, budget=1.0, faults=Faults(dead_rate=0.1, flaky_rate=0.3))
    one = run(cfg, db)
    batched = run(replace(cfg, run_id="batched", batch_size=10, seconds_per_batch_question=0.0), db)

    assert batched.used_fallback > 0
    assert replace(batched, run_id="") == replace(one, run_id="")
    assert rows(db, "batched") == rows(db, "per-question")


def test_a_rate_limited_call_is_retried_after_the_wait_the_provider_asked_for(tmp_path):
    db = str(tmp_path / "results.db")
    faults = Faults(dead_rate=0.0, flaky_rate=0.0, rate_limit_rate=1.0, retry_after_seconds=1.5)
    cfg = RunConfig("limited", questions=2, budget=1.0, faults=faults, first_retry_seconds=0.1)
    summary, history = run_with_history(cfg, db)

    assert (summary.status, summary.answered, summary.used_fallback) == ("completed", 2, 0)
    # In the history: each retried call is a rate-limited one, and its second attempt started 1.5 s or
    # more after the call was scheduled, not after the 0.1 s of the retry policy.
    events = history.events
    scheduled = {
        e.event_id: e.event_time.ToDatetime() for e in events if e.HasField("activity_task_scheduled_event_attributes")
    }
    retried = [e for e in events if e.activity_task_started_event_attributes.attempt > 1]
    assert len(retried) == 2
    for e in retried:
        started = e.activity_task_started_event_attributes
        assert started.last_failure.application_failure_info.type == "RateLimited"
        assert (e.event_time.ToDatetime() - scheduled[started.scheduled_event_id]).total_seconds() >= 1.5
