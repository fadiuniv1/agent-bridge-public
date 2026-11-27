# Website-build performance notes (2026-11-27)

Measured from the live `chat_requests` table (43 completed jobs whose prompt mentions
a website/site):

| Metric | Value |
| :--- | :--- |
| Median wall time | 511 s |
| p90 | 1,701 s |
| Max | 2,344 s |
| Model on every one of the 10 slowest jobs | `qwen3.5:4b` (strong route) |

Key observation: most of the slowest jobs ended with `execution_status: not_executed`
and only 1–3 tool calls. In other words, several minutes were spent generating text
without changing any files. The wall time is dominated by local model generation and
no-op recovery turns, not by file I/O.

## Levers, in order of impact

1. **Select `cloud:strong` in the app for long website builds.** Already supported;
   a free Groq key is configured on the host. This is the only option that turns a
   ~500–2,300 s job into seconds. Watch the free-tier daily request limit.
2. **Early no-op abort.** If a website job has produced no mutating tool call after the
   first model turn, stop after one bounded recovery turn and ask a clarifying question
   instead of running further long generations. Needs care to avoid false aborts.
3. **Trim the tool schema for simple website requests.** The full tool list is sent on
   every turn; a task-specific schema could cut prompt-eval time substantially.
4. **A larger local model** would help quality but the host has ~5.5 GB RAM free;
   it would trade against the bridge and Ollama runtime.

## Acceptance criteria for a real fix

- Median wall time for a standard small website build under 120 s on this host.
- `not_executed` website jobs return a clarifying question in under 60 s.
- 87+ unit tests and 11/11 smoke checks stay green.
