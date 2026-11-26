import asyncio
from concurrent.futures import ThreadPoolExecutor

from resumable_eval.harness import local_server
from resumable_eval.shared import NO_FAULTS, RunConfig, Summary
from resumable_eval.worker import make_worker
from resumable_eval.workflow import EvalWorkflow


def test_the_run_waits_for_approval_shows_its_summary_meanwhile_and_ends_as_decided(tmp_path):
    db = str(tmp_path / "results.db")

    async def review(client, run_id: str, decision: str | None, timeout: float) -> tuple[Summary, Summary]:
        cfg = RunConfig(run_id, questions=3, budget=1.0, faults=NO_FAULTS, approval_timeout_seconds=timeout)
        handle = await client.start_workflow(EvalWorkflow.run, cfg, id=run_id, task_queue="approval")
        while (waiting := await handle.query(EvalWorkflow.current_summary)).approval != "waiting":
            await asyncio.sleep(0.1)
        if decision == "approve":
            await handle.signal(EvalWorkflow.approve)
        if decision == "reject":
            await handle.signal(EvalWorkflow.reject)
        return waiting, await handle.result()

    async def main():
        async with await local_server() as env:
            with ThreadPoolExecutor() as executor:
                async with make_worker(env.client, "approval", db, executor):
                    return [
                        await review(env.client, "approve-me", "approve", timeout=60),
                        await review(env.client, "reject-me", "reject", timeout=60),
                        await review(env.client, "forget-me", None, timeout=1),
                    ]

    (waiting, approved), (_, rejected), (_, forgotten) = asyncio.run(main())

    # While it waits, the query shows the finished summary to the person who decides.
    assert (waiting.status, waiting.answered, waiting.approval) == ("completed", 3, "waiting")
    assert waiting.spent > 0
    assert approved.approval == "approved"
    assert rejected.approval == "rejected"
    assert forgotten.approval == "not approved"  # nobody decided within the timeout
