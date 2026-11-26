import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest
from old_workflow import EvalWorkflowV1
from temporalio.worker import Replayer
from temporalio.workflow import NondeterminismError
from workflow_without_patch import EvalWorkflowWithoutPatch

from resumable_eval import store
from resumable_eval.activities import recorded_answers
from resumable_eval.budget import worst_case
from resumable_eval.harness import local_server
from resumable_eval.shared import NO_FAULTS, PRIMARY, RunConfig, cost
from resumable_eval.worker import make_worker
from resumable_eval.workflow import RESERVE_PATCH, EvalWorkflow


def failed_tasks(history) -> int:
    return sum(e.HasField("workflow_task_failed_event_attributes") for e in history.events)


def has_patch_marker(history) -> bool:
    markers = [
        e.marker_recorded_event_attributes for e in history.events if e.HasField("marker_recorded_event_attributes")
    ]
    return any(RESERVE_PATCH.encode() in m.SerializeToString() for m in markers)


def test_a_run_started_on_version_1_0_finishes_on_new_workers_and_new_runs_take_the_new_rule(tmp_path):
    db = str(tmp_path / "results.db")
    recorded = recorded_answers(PRIMARY.name)
    costs = [cost(recorded[q]["prompt_tokens"], recorded[q]["generated_tokens"], PRIMARY) for q in range(12)]
    # With this budget the old rule (check the spend before each question) asks all 12 questions,
    # while the new rule (reserve the worst case) stops after 4.
    budget = worst_case(RunConfig("", 1, 1.0)) + sum(costs[:4]) - costs[3] / 2
    slow = replace(NO_FAULTS, median_latency_seconds=0.2)  # slow calls, so that the run is still going at the handover
    old_cfg = RunConfig("started-on-1.0", questions=12, budget=budget, faults=slow)
    new_cfg = replace(old_cfg, run_id="started-on-1.1")

    async def main():
        async with await local_server() as env:
            client = env.client
            with ThreadPoolExecutor() as executor:
                async with make_worker(client, "versioning", db, executor, workflow=EvalWorkflowV1):
                    old = await client.start_workflow(
                        EvalWorkflow.run, old_cfg, id=old_cfg.run_id, task_queue="versioning"
                    )
                    while store.count_results(db, old_cfg.run_id) < 6:  # past question 4, where the new rule stops
                        await asyncio.sleep(0.02)
                # The worker with the old code is stopped. A worker with the new code takes over the run.
                handed_over_at = store.count_results(db, old_cfg.run_id)
                async with make_worker(client, "versioning", db, executor):
                    old_summary = await asyncio.wait_for(old.result(), timeout=60)
                    new = await client.start_workflow(
                        EvalWorkflow.run, new_cfg, id=new_cfg.run_id, task_queue="versioning"
                    )
                    new_summary = await new.result()
            return handed_over_at, old_summary, await old.fetch_history(), new_summary, await new.fetch_history()

    handed_over_at, old_summary, old_history, new_summary, new_history = asyncio.run(main())
    assert 6 <= handed_over_at < 12

    # The old run finished on the new code, by the old rule, without a single failed workflow task.
    assert (old_summary.status, old_summary.answered) == ("completed", 12)
    assert failed_tasks(old_history) == 0
    assert not has_patch_marker(old_history)

    # A new run takes the new path: it records the patch marker and stops before the budget could be passed.
    assert new_summary.status == "budget exhausted after question 4"
    assert new_summary.spent <= budget
    assert has_patch_marker(new_history)

    async def replay():
        await Replayer(workflows=[EvalWorkflow]).replay_workflow(old_history)
        await Replayer(workflows=[EvalWorkflow]).replay_workflow(new_history)
        # Without workflow.patched, the new code would stop the old run at question 4,
        # where its history goes on to question 5: Temporal refuses that replay.
        with pytest.raises(NondeterminismError):
            await Replayer(workflows=[EvalWorkflowWithoutPatch]).replay_workflow(old_history)

    asyncio.run(replay())
