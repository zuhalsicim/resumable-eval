import random
import statistics
from dataclasses import replace

from resumable_eval import store
from resumable_eval.activities import recorded_answers
from resumable_eval.budget import Budget, worst_case
from resumable_eval.faults import LATENCY_CAP_SECONDS, planned_fault, planned_latency
from resumable_eval.shared import FALLBACK, PRICES, PRIMARY, Faults, Result, RunConfig, cost

FAULTS = Faults(dead_rate=0.1, flaky_rate=0.3)


def plan(seed: int, question_id: int, faults: Faults = FAULTS) -> list[str | None]:
    return [planned_fault(seed, "q4_k_m", question_id, attempt, faults) for attempt in range(1, 6)]


def test_the_same_seed_plans_the_same_faults():
    assert [plan(7, q) for q in range(300)] == [plan(7, q) for q in range(300)]
    assert [plan(7, q) for q in range(300)] != [plan(8, q) for q in range(300)]


def test_two_attempts_lost_in_crashes_cannot_send_an_answerable_question_to_the_fallback():
    # An answerable question fails at most twice. A lost attempt that would have failed changes nothing;
    # one that would have answered moves the same answer to the next attempt. So the worst case uses
    # 2 failures + 2 lost attempts + 1 answer = 5 attempts, and the retry policy allows 5.
    plans = [plan(7, q) for q in range(1034)]
    most_failures = max(sum(1 for f in p if f) for p in plans if not all(p))
    assert most_failures == 2
    assert most_failures + 2 + 1 <= RunConfig("", 1, 1.0).max_attempts
    assert any(all(p) for p in plans), "some questions are dead"


def test_rate_limits_only_refuse_the_first_attempt_and_move_no_other_fault():
    limited = Faults(dead_rate=0.1, flaky_rate=0.3, rate_limit_rate=0.5)
    for q in range(300):
        before, after = plan(7, q), plan(7, q, limited)
        assert after[1:] == before[1:]
        assert after[0] in (before[0], "rate limit")
    assert sum(plan(7, q, limited)[0] == "rate limit" for q in range(300)) > 100


def test_latency_is_drawn_per_call_from_the_seed_and_stays_below_the_timeout():
    slow = Faults(median_latency_seconds=0.05)
    draws = [planned_latency(7, "q4_k_m", q, 1, slow) for q in range(1000)]
    assert draws == [planned_latency(7, "q4_k_m", q, 1, slow) for q in range(1000)]
    assert 0.04 < statistics.median(draws) < 0.06
    assert max(draws) <= LATENCY_CAP_SECONDS < RunConfig("", 1, 1.0).call_timeout_seconds
    assert planned_latency(7, "q4_k_m", 0, 1, Faults()) == 0.0  # off by default


def test_saving_a_result_twice_keeps_the_first_row(tmp_path):
    db = str(tmp_path / "results.db")
    first = Result("run", 3, "q4_k_m", 1, False, "SELECT 1", True, 0.001)
    store.save_result(db, first)
    store.save_result(db, replace(first, attempt=2, sql="SELECT 2"))
    assert store.results(db, "run") == [first]


def test_the_prices_come_from_the_config_file():
    assert (PRIMARY.input_price, PRIMARY.output_price) == (PRICES["q4_k_m"]["input"], PRICES["q4_k_m"]["output"])
    assert (FALLBACK.input_price, FALLBACK.output_price) == (PRICES["f16"]["input"], PRICES["f16"]["output"])


def test_no_recorded_answer_costs_more_than_the_worst_case():
    cfg = RunConfig("", 1034, 1.0)
    for provider in (PRIMARY, FALLBACK):
        for row in recorded_answers(provider.name).values():
            assert row["prompt_tokens"] <= cfg.max_prompt_tokens and row["generated_tokens"] <= cfg.max_tokens
            assert cost(row["prompt_tokens"], row["generated_tokens"], provider) <= worst_case(cfg)


def test_reserving_the_worst_case_never_lets_the_spend_pass_the_limit():
    rng = random.Random(1)
    for _ in range(200):
        budget = Budget(limit=rng.uniform(0.0, 0.1), worst_case=0.01)
        while taken := budget.take(rng.randint(1, 4)):  # several questions in flight at once
            for _ in range(taken):
                budget.settle(rng.uniform(0.0, budget.worst_case))
                assert budget.spent <= budget.limit
        assert budget.spent + budget.worst_case > budget.limit  # it stopped only when the next one did not fit


def test_the_old_budget_rule_could_pass_the_limit_by_one_answer():
    old = Budget(limit=0.01, worst_case=0.01, reserve=False)
    while old.take(1):
        old.settle(0.004)
    assert old.spent > old.limit  # 0.012: why version 1.1 reserves before asking
