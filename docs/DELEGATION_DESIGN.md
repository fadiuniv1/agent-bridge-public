# Full/durable delegation — design proposal (2026-11-27)

Inline chat with direct tools remains the default. Full delegation is opt-in.

## Scope

Only tasks that declare a workspace, a short objective, acceptance criteria and
verification commands may be delegated. Privileged package/service operations require
the existing `confirm=true` gate; no arbitrary root shells.

## Checkpoints

Persist a `delegation_state` record per task: objective, workspace, allowed tools,
completed steps with diffs, remaining steps, last successful checkpoint, retry count.
Checkpoint after every mutating tool call, so a phone restart or lost SSE stream can
resume from the last verified step rather than re-running the whole job.

## Failure recovery

- Bounded retries per step (2), with exponential backoff for transient model errors.
- On a repeated failure, pause the task, surface the exact error and ask the user;
  never silently claim success (the existing `execution_status` rules stay in force).
- Stale heartbeats mark a task interrupted; the supervisor resumes only from a
  checkpoint, never from scratch.

## Opt-in and UI

Add an `Allow autonomous tasks` switch in Settings, default off. The app shows a
task card with step progress, pause/resume and a diff review before finalizing.

## Test plan

- Unit tests: checkpoint serialization, resume idempotency, retry bounds, pause/resume.
- Integration: kill and restart the bridge mid-task; verify resume from checkpoint.
- Regression: 87+ unit tests, 11/11 smoke checks, and the existing inline chat flow.

## Pilot

Run one small task (create a static page plus README in a scratch folder) with the
switch enabled, review every step diff, then decide whether to promote the mode.
