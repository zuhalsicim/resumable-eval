# Where these files come from

`q4_k_m.json` and `f16.json` hold the recorded answers of the benchmark
[zuhalsicim/sql-under-compression](https://github.com/zuhalsicim/sql-under-compression), commit `1dfa47d9e649bb76e8549eb45c7ada8430b7e790`, folder `results-full-dev/`.

That benchmark ran Qwen2.5-Coder-1.5B-Instruct, compressed with pruna 0.3.5 and run with llama.cpp, on all 1,034 questions of the Spider development set and scored every answer by running it against the database (execution accuracy).
Here are two of its four precisions: `q4_k_m` (614 of 1,034 correct) plays the primary provider, `f16` (647 of 1,034 correct) plays the fallback.

One row per question, with only the fields this project uses:

| Field | Meaning |
|---|---|
| `question_id` | position of the question in the Spider development set (`index` in the source) |
| `db_id` | the Spider database the question is about |
| `sql` | the SQL the model wrote |
| `correct` | whether that SQL returned the same result as the reference query |
| `prompt_tokens`, `generated_tokens` | token counts of the call, used for the illustrative cost |

The files hold no Spider questions and no reference queries.

To copy them again:

```
python scripts/copy_data.py ../sql-under-compression
```
