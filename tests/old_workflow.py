"""workflow.py as it was in version 1.0 (commit 71a8b36), to test that a run started on it can finish on new code.

Apart from this docstring, two lines differ from that file: the class name and the name in @workflow.defn,
which registers it under the same workflow type, EvalWorkflow.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from resumable_eval.activities import Activities
    from resumable_eval.shared import NO_FAULTS, Answer, Faults, Provider, Question, Result, RunConfig, Summary, cost


@workflow.defn(name="EvalWorkflow")
class EvalWorkflowV1:
    @workflow.run
    async def run(self, cfg: RunConfig) -> Summary:
        status = "completed"
        answered = correct = used_fallback = 0
        spent = 0.0
        for question_id in range(cfg.questions):
            if spent >= cfg.budget:
                status = f"budget exhausted after question {answered}"
                break
            answer, provider, fell_back = await self.answer(cfg, question_id)
            result = Result(
                run_id=cfg.run_id,
                question_id=question_id,
                provider=provider.name,
                attempt=answer.attempt,
                used_fallback=fell_back,
                sql=answer.sql,
                correct=answer.correct,
                cost=cost(answer.prompt_tokens, answer.generated_tokens, provider),
            )
            await workflow.execute_activity_method(
                Activities.save_result, result, start_to_close_timeout=timedelta(seconds=10)
            )
            answered += 1
            spent += result.cost
            correct += answer.correct
            used_fallback += fell_back
        return Summary(cfg.run_id, status, answered, correct, used_fallback, spent)

    async def answer(self, cfg: RunConfig, question_id: int) -> tuple[Answer, Provider, bool]:
        try:
            return await self.ask(cfg, cfg.primary, cfg.faults, question_id), cfg.primary, False
        except ActivityError:
            workflow.logger.warning(f"question {question_id}: {cfg.primary.name} gave up, asking {cfg.fallback.name}")
            return await self.ask(cfg, cfg.fallback, NO_FAULTS, question_id), cfg.fallback, True

    async def ask(self, cfg: RunConfig, provider: Provider, faults: Faults, question_id: int) -> Answer:
        return await workflow.execute_activity_method(
            Activities.ask_provider,
            Question(cfg.run_id, cfg.seed, question_id, provider.name, faults),
            start_to_close_timeout=timedelta(seconds=cfg.call_timeout_seconds),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=cfg.first_retry_seconds),
                backoff_coefficient=2.0,
                maximum_attempts=cfg.max_attempts,
            ),
        )
