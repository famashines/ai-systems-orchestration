# Changelog

## Unreleased

- Run process manager and Task aggregate (task M3-02), contracts v0.4.1:
  all task and subtask states of the status table, `StartRun` from an
  approved plan, DAG scheduling with at most `MAX_PARALLEL_SUBTASKS`
  subtasks (default 3), one `CallRequested` (`kind` `subtask`) for each
  try, 3 tries with backoff (`SUBTASK_RETRY_DELAYS_MS`), then
  `SubtaskFailed` and `RunBlocked` `subtask_failed`. Checkpoints for
  `needsApproval` (`Approval needed for <title>`, deadline
  `CHECKPOINT_TIMEOUT_MS`); a refused or expired checkpoint fails the
  subtask and blocks the run. Pause, resume, cancel, `TaskCancelled`,
  retry of a subtask, resolve of a checkpoint (domain inputs for M3-04).
- `CallSettled` for each final attempt of a call with a `kind`, with the
  call totals (or the values of that attempt). The task cost and
  `maxCostMicroUsd` come from `CallSettled`. Runaway rules give
  `SubtaskBlocked` and `RunBlocked` `runaway_stopped`.
- Subtask inputs as data blocks: the `owner_message` block, one for each
  steer, and one `subtask_output` block for each dependency with the
  `callId` of its call as the block id.
- A `steer` for a planned run with a subtask that has not started gives
  `SteeringMessageAdded`. The stale `RunFailed` of a chat task also gives
  `CallCancelRequested`.
- Timers from the `SubtaskRetrying` and `CheckpointOpened` events and the
  `processed` markers (no new collection), new migration `001-timers`,
  `TIMER_INTERVAL_MS`.
- `RunCompleted` of a planned run has the outputs in plan order (M3-03
  changes it to the aggregate call).
- Subtask agents are read by id from agent-manager
  (`/internal/agents/:agentId/active-version`, not there yet).
- A chat `new_task` stays the M2 one-agent run, with no `CallSettled`.
- Deploy with the strategy `Recreate`, after conversation-hub,
  projections and ai-proxy are on v0.4.1 (contracts CHANGELOG 0.4.1).

- One agent run for a chat message (task M2-06): a consumer of
  `conversation.OwnerMessagePosted` gives `TaskSubmitted` (with
  `conversationId` and `messageId`), `RunStarted` and `CallRequested` for
  the active `writer` agent version and its prompt version. A consumer of
  `llm.*` gives `RunCompleted`, `RunFailed` (also `empty_output`) or
  `RunBlocked` (`budget_refused`). `maxCostMicroUsd` is the rest of the
  task allowance. Base runaway rules give `RunBlocked` `runaway_stopped`.
- `steer` and `answer` messages give `MessageRefused`. A steer or an
  answer for a call older than 15 minutes ends that run with `RunFailed`
  first. A late call event for a closed task gives a warn log.
- agent-manager and prompt-studio reads: 3 tries for a 5xx, a network
  error or a timeout, then `RunFailed` `unavailable`. One read takes up to
  about 12 s, the two reads up to about 25 s.
- Another 4xx (not 404) from agent-manager or prompt-studio gives
  `RunFailed` `internal` with no retry (task M2-19).
- Exactly-once handling: the kit `processed` marker is in the transaction
  of the append. New migration `000-processed`.
- New env `AGENT_MANAGER_URL` and `PROMPT_STUDIO_URL`.
- Contracts v0.3.0 and kit v0.3.0. Orchestration does not use the budget
  rpc (ADR 0027).
- Tests: `test:e2e` runs the real budget, agent-manager, prompt-studio and
  ai-proxy with the ai-proxy mock OpenRouter (no cap, a reached cap, and
  an answer); a CI job `e2e`.

- Release workflow: image, CycloneDX SBOM, grype scan, push to GHCR
  by tag and digest, and a release pull request in `famashines/gitops`.

## 0.1.0

- Task aggregate: `SubmitTask` gives `orchestration.TaskSubmitted`
  (schema version 1) on the stream `task-<taskId>`.
- `POST /internal/commands/submit-task` with command idempotency by
  `commandId`: 201 the first time, 200 with the same answer on a replay,
  409 on a conflict, 400 problem JSON on a bad body, 503 when MongoDB is
  down. Only an `owner` actor in M1.
- `GET /internal/events`: the replay API in position order, at most 1000
  events in one page.
- `/health`, `/ready` (MongoDB and migrations), `/metrics`.
- Outbox relay to the stream `ORCHESTRATION`, graceful shutdown.
- Container image on Node 26 slim, uid 1000, read-only root file system.
