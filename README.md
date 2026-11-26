# resumable-eval

[![ci](https://github.com/zuhalsicim/resumable-eval/actions/workflows/ci.yml/badge.svg)](https://github.com/zuhalsicim/resumable-eval/actions/workflows/ci.yml)

An LLM benchmark run as a [Temporal](https://temporal.io) workflow, with retries, a fallback model and a cost budget. Kill the worker halfway and the run still ends with exactly the results it would have had without the crash.

## The question

A long LLM job fails in the middle: the provider times out, returns errors, or the machine running the job dies.
How do you make a run of 1,034 model calls retry what failed, switch to a fallback model when the main one keeps failing, stop when the money runs out, and resume after a crash without losing an answer or counting one twice?
This repo answers that with Temporal, at the smallest size that still has every one of those parts.

## How it works

1. One run is one Temporal workflow ([`workflow.py`](resumable_eval/workflow.py)): it goes through the questions in order and asks the primary provider each one in an activity with a 1-second timeout and a retry policy of 4 attempts (waits of 0.1, 0.2 and 0.4 s).
2. When all 4 attempts fail, the workflow asks the fallback provider the same question and records that it did.
3. Each answer costs its recorded token counts times an illustrative price; the workflow adds up the spend and stops with `budget exhausted after question N` once the spend has reached the budget.
4. A second activity writes each result to SQLite under the key (run, question), so writing it again changes nothing, and the provider activity adds every call that returned an answer to a ledger.
5. Temporal stores the result of every finished step in the run's event history, so a new worker that takes over after a crash replays the workflow against that history and goes on at the first step that had not finished.

**No model is called.** The two providers return the answers that Qwen2.5-Coder-1.5B-Instruct really gave on the 1,034 questions of the Spider development set in my benchmark [sql-under-compression](https://github.com/zuhalsicim/sql-under-compression): `q4_k_m` is the primary, `f16` the fallback, each answer with its token counts and whether it was correct ([data/PROVENANCE.md](data/PROVENANCE.md)).
Faults are injected on purpose, planned by a seed so that every run with that seed meets the same ones: 2% of questions fail on every attempt, 8% fail on their first one or two attempts, and a quarter of those failures hang past the timeout instead of raising an error.
The point is the orchestration (retries, fallback, budget, resume), not model quality.

## The crash test

The crash test starts a local Temporal server and runs the same benchmark twice: once without interruption, and once with the worker in its own process killed hard (SIGKILL) after K results are written, then replaced by a fresh worker.
It checks that the crashed run completes, that every question has exactly one result, that nothing was counted twice (the run's own count and spend equal the result rows, and the ledger shows how many provider calls really returned an answer), and that the results equal those of the uninterrupted run.

Numbers from CI run [45824885832](https://github.com/zuhalsicim/resumable-eval/actions/runs/45824885832) (GitHub Actions, Ubuntu, seed 7). The 200-question test is part of `pytest`, the 1,034-question one is a separate CI step:

| | 200 questions | all 1,034 questions |
|---|---|---|
| Worker killed after | 80 results | 400 results |
| Status of the crashed run | completed | completed |
| Result rows | 200, none missing | 1,034, none missing |
| Answers counted twice | 0 | 0 |
| Spend equals the sum of the rows | yes | yes |
| Provider calls that returned an answer (ledger) | 200 | 1,034 |
| Same results and summary as the uninterrupted run | yes | yes |
| Events in the run's history | 2,420 | 12,536 |
| Seconds, uninterrupted / with the crash | 34 / 36 | 250 / 252 |

On all 1,034 questions the seed plans 20 dead and 87 flaky questions for the primary, with 187 errors and 28 hangs.
The run answers 20 questions with the fallback, gets 616 of 1,034 right (the primary alone: 614) and spends $0.5492 (`python -m resumable_eval run --questions 1034 --crash-after 400`).

A second test makes the worker die at the worst moment: right after the provider answered question 4, before the worker could report the answer.
Temporal never received it, so after the 1-second timeout it ran the call again on the new worker.
The ledger shows 11 provider answers for 10 questions, while the results and the spend count question 4 once.
Temporal runs an activity at least once, not exactly once, and that is why the writes are idempotent.

## Run it

Python 3.12. The first run downloads the Temporal dev server; the Temporal SDK does that by itself.

```
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

python -m resumable_eval run --questions 200 --crash-after 80     # kill the worker after 80 results
python -m resumable_eval run --questions 200 --budget 0.05        # stop at 5 cents
python -m resumable_eval run --questions 20 --ui                  # keep the server open and look at the run in the web UI
python -m resumable_eval crash-test --questions 1034 --crash-after 400
pytest -v -s
```

What a run prints (on Windows 11; the seconds differ by machine):

```
$ python -m resumable_eval run --questions 200 --crash-after 80
run-7973d10f: completed
answered 200 of 200, 102 correct, 2 by the fallback
spent $0.0855 of $1.00
faults planned for q4_k_m: dead questions 2, flaky questions 12, errors 23, hangs 3
worker killed after 81 results, a new worker took over
provider answers in ledger: 200, result rows: 200 (in runs\results.db)
history events: 2417, seconds: 45.9

$ python -m resumable_eval run --questions 200 --budget 0.05
run-1ec5f934: budget exhausted after question 120
answered 120 of 200, 62 correct, 1 by the fallback
spent $0.0504 of $0.05
faults planned for q4_k_m: dead questions 1, flaky questions 8, errors 15, hangs 2
provider answers in ledger: 120, result rows: 120 (in runs\results.db)
history events: 1451, seconds: 21.5
```

## Files

| File | What it does |
|---|---|
| [`resumable_eval/workflow.py`](resumable_eval/workflow.py) | The workflow: the loop over the questions, the budget check, the fallback. No clock, no random numbers, no I/O, so it can be replayed. |
| [`resumable_eval/activities.py`](resumable_eval/activities.py) | The provider activity (recorded answers and planned faults) and the activity that saves a result. |
| [`resumable_eval/store.py`](resumable_eval/store.py) | SQLite: the results table keyed by run and question, and the ledger of provider calls. |
| [`resumable_eval/worker.py`](resumable_eval/worker.py) | The worker that runs the workflow and activity code. |
| [`resumable_eval/harness.py`](resumable_eval/harness.py) | Starts workers as separate processes, kills them, and compares a crashed run with an uninterrupted one. |
| [`resumable_eval/shared.py`](resumable_eval/shared.py) | The messages passed between the parts, the illustrative prices and the cost rule. |
| [`tests/`](tests) | The two crash tests, and tests for the fault plan, idempotent writes, retries, the fallback and the budget. |

## Limits

- The model calls are replayed. There is no real latency, no rate limit and no answer that changes on a retry. That is what lets the test compare the crashed run with the uninterrupted one exactly; with a real provider you would compare the stored answers instead.
- The fault plan leaves one spare attempt: a question that can be answered fails at most twice and the retry policy allows four, so one attempt lost in the crash cannot send it to the fallback. Two crashes during the same question could.
- Only the worker is crashed, not the Temporal server, and everything runs on one machine with the dev server, which keeps its state in memory.
- The workflow asks one question at a time. Asking many at once would be faster; the budget would then have to reserve the cost of a call before making it.
- The budget is checked before each question, so the spend can pass the budget by one answer.
- Retries add no events to the history, but each question adds about 12, so a run of 1,034 questions has 12,536 events. Temporal allows 51,200 events per run, which this design reaches at about 4,200 questions; a longer run would start a fresh history every few thousand questions (continue-as-new) or give each batch its own child workflow.
- Not tested: a crash of the server, two workers running at once, changing the workflow code while a run is in progress (versioning), heartbeats for long calls, and a fallback that fails too (the run then fails with that error).
- The prices are made up, not anyone's price list.

## Licence

MIT
