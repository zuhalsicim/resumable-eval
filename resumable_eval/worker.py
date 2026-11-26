"""A worker: the process that runs the workflow and activity code.

It asks the Temporal server for tasks on one task queue. It keeps no state of its own
that matters: kill it, start another one, and the run goes on. Several workers can poll
the same queue; the server hands each task to one of them.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from temporalio.client import Client
from temporalio.worker import Worker

from resumable_eval.activities import Activities
from resumable_eval.workflow import EvalWorkflow


def make_worker(
    client: Client,
    task_queue: str,
    db_path: str,
    executor: ThreadPoolExecutor,
    exit_after_answer: int | None = None,
    workflow: type = EvalWorkflow,
) -> Worker:
    activities = Activities(db_path, exit_after_answer)
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[workflow],
        activities=[activities.ask_provider, activities.ask_batch, activities.save_result],
        activity_executor=executor,  # the activities are plain functions, run in threads
        sticky_queue_schedule_to_start_timeout=timedelta(seconds=2),  # hand work to another worker sooner
    )


async def run_worker(address: str, task_queue: str, db_path: str, exit_after_answer: int | None = None) -> None:
    client = await Client.connect(address)
    with ThreadPoolExecutor(max_workers=8) as executor:
        await make_worker(client, task_queue, db_path, executor, exit_after_answer).run()
