import asyncio
import math
from concurrent.futures import ThreadPoolExecutor

from resumable_eval import store
from resumable_eval.activities import planned_fault, recorded_answers
from resumable_eval.harness import local_server
from resumable_eval.shared import FALLBACK, NO_FAULTS, PRIMARY, Faults, RunConfig, Summary, cost
from resumable_eval.worker import make_worker
from resumable_eval.workflow import EvalWorkflow


def run(cfg: RunConfig, db: str) -> Summary:
    """Run the workflow on a local server with a worker in this process."""

    async def main() -> Summary:
        async with await local_server() as env:
            with ThreadPoolExecutor() as executor:
                async with make_worker(env.client, "tests", db, executor):
                    return await env.client.execute_workflow(EvalWorkflow.run, cfg, id=cfg.run_id, task_queue="tests")

    return asyncio.run(main())


def test_failed_calls_are_retried_until_they_answer(tmp_path):
    db = str(tmp_path / "results.db")
    cfg = RunConfig("flaky", questions=6, budget=1.0, faults=Faults(dead_rate=0.0, flaky_rate=1.0, hang_share=0.5))
    summary = run(cfg, db)

    assert summary.status == "completed"
    assert summary.used_fallback == 0
    recorded = recorded_answers(PRIMARY.name)
    for r in store.results(db, "flaky"):
        planned = [planned_fault(cfg.seed, PRIMARY.name, r.question_id, a, cfg.faults) for a in range(1, 5)]
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


def test_the_budget_stops_the_run_cleanly(tmp_path):
    db = str(tmp_path / "results.db")
    recorded = recorded_answers(PRIMARY.name)
    costs = [cost(recorded[q]["prompt_tokens"], recorded[q]["generated_tokens"], PRIMARY) for q in range(10)]
    budget = sum(costs[:5]) - costs[4] / 2  # used up by the fifth answer
    summary = run(RunConfig("budget", questions=10, budget=budget, faults=NO_FAULTS), db)

    assert summary.status == "budget exhausted after question 5"
    assert summary.answered == 5
    assert [r.question_id for r in store.results(db, "budget")] == [0, 1, 2, 3, 4]
    assert math.isclose(summary.spent, sum(costs[:5]))
