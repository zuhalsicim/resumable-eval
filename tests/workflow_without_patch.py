"""Version 1.1 as it would be without workflow.patched: every run reserves the worst case, old runs too.

Only for the versioning test, which shows that replaying a run started on version 1.0 with this
code fails with a non-determinism error.
"""

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from resumable_eval.shared import RunConfig, Summary
    from resumable_eval.workflow import EvalWorkflow


@workflow.defn(name="EvalWorkflow")
class EvalWorkflowWithoutPatch(EvalWorkflow):
    @workflow.run
    async def run(self, cfg: RunConfig) -> Summary:
        return await super().run(cfg)

    def reserves_budget(self) -> bool:
        return True
