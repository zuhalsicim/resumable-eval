from dataclasses import replace

from resumable_eval import store
from resumable_eval.activities import planned_fault
from resumable_eval.shared import Faults, Result

FAULTS = Faults(dead_rate=0.1, flaky_rate=0.3)


def plan(seed: int, question_id: int) -> list[str | None]:
    return [planned_fault(seed, "q4_k_m", question_id, attempt, FAULTS) for attempt in range(1, 5)]


def test_the_same_seed_plans_the_same_faults():
    assert [plan(7, q) for q in range(300)] == [plan(7, q) for q in range(300)]
    assert [plan(7, q) for q in range(300)] != [plan(8, q) for q in range(300)]


def test_a_question_that_can_be_answered_needs_at_most_three_attempts():
    # The retry policy allows four, so one attempt lost in a crash does not change the outcome.
    plans = [plan(7, q) for q in range(1034)]
    assert all(all(p) or not any(p[2:]) for p in plans)
    assert any(all(p) for p in plans), "some questions are dead"
    assert any(p[1] and not all(p) for p in plans), "some questions fail twice, then answer"


def test_saving_a_result_twice_keeps_one_row(tmp_path):
    db = str(tmp_path / "results.db")
    first = Result("run", 3, "q4_k_m", 1, False, "SELECT 1", True, 0.001)
    store.save_result(db, first)
    store.save_result(db, replace(first, attempt=2))
    assert store.results(db, "run") == [first]
