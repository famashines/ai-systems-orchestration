# Changelog

## Unreleased

- One agent run for a chat message (task M2-06): a consumer of
  `conversation.OwnerMessagePosted` gives `TaskSubmitted` (with
  `conversationId` and `messageId`), `RunStarted` and `CallRequested` for
  the active `writer` agent version and its prompt version. A consumer of
  `llm.*` gives `RunCompleted`, `RunFailed` (also `empty_output`) or
  `RunBlocked` (`budget_refused`). `maxCostMicroUsd` is the rest of the
  task allowance. Base runaway rules give `RunBlocked` `runaway_stopped`.
- `steer` and `answer` messages give `MessageRefused`. A steer for a call
  older than 15 minutes ends that run with `RunFailed` first.
- Exactly-once handling: the kit `processed` marker is in the transaction
  of the append. New migration `000-processed`.
- New env `AGENT_MANAGER_URL` and `PROMPT_STUDIO_URL`.
- Contracts v0.3.0 and kit v0.3.0. Orchestration does not use the budget
  rpc (ADR 0027).
- Tests: `test:e2e` runs the real budget, agent-manager, prompt-studio and
  ai-proxy with the ai-proxy mock OpenRouter; a CI job `e2e`.

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
