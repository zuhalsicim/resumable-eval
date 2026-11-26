import asyncio

import pytest

from resumable_eval.server_crash import find_cli, server_crash_test
from resumable_eval.shared import RunConfig


@pytest.mark.skipif(find_cli() is None, reason="needs the Temporal CLI: set TEMPORAL_CLI or put temporal on the PATH")
def test_a_killed_server_restarts_from_its_file_and_the_run_ends_as_if_nothing_happened(tmp_path):
    cfg = RunConfig("", questions=60, budget=10.0)
    test = asyncio.run(server_crash_test(find_cli(), cfg, tmp_path, crash_after=25))
    print(test.line())
    assert test.passed
    assert test.crashed_after >= 25
