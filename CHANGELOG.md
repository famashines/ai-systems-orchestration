# Changelog

## Unreleased

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
