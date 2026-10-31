# ai-systems-orchestration

The Task Orchestration service of ai-systems. It takes tasks and keeps them
as event-sourced streams `task-<taskId>` in MongoDB. The `events`
collection is the outbox: a relay publishes each event to the NATS
JetStream stream `ORCHESTRATION`. The service is internal: only
api-gateway (commands) and projections (replay) call it. It has no
Ingress.

M1 has one command, `SubmitTask`, and one event,
`orchestration.TaskSubmitted` (schema version 1). Plans, runs and the
consumers of `CONVERSATION` and `LLM` come in M2.

```
api-gateway --POST /internal/commands/submit-task--> orchestration
                                                       | one transaction
                                                       v
                                       MongoDB orchestration: events, commands
                                                       | outbox relay, Nats-Msg-Id = eventId
                                                       v
                                       NATS stream ORCHESTRATION (orchestration.>)
projections --GET /internal/events--> orchestration (rebuild only)
```

Architecture and reasons: `ai-systems-docs` (`ARCHITECTURE.md` 5 to 8,
ADR 0006, 0007, 0023).

## API

| Method | Path                                           | Does                                                                                                                                   |
| ------ | ---------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| POST   | `/internal/commands/submit-task`               | Submit a new task                                                                                                                      |
| GET    | `/internal/events?afterPosition=<n>&limit=<m>` | Replay API: all events in position order                                                                                               |
| GET    | `/health`                                      | Liveness: 200 while the process runs                                                                                                   |
| GET    | `/ready`                                       | Readiness: 200 when MongoDB answers and the migrations are done, else 503 with the names of the failing checks (`mongo`, `migrations`) |
| GET    | `/metrics`                                     | Prometheus metrics                                                                                                                     |

Errors are RFC 9457 problem JSON with a `code` (`@famashines/ai-systems-kit/http`).

### `POST /internal/commands/submit-task`

The body is a `CommandEnvelopeV1` with a `SubmitTaskCommandPayloadV1`
(`@famashines/ai-systems-contracts` v0.1.0):

```json
{
  "commandId": "6f1c2b9e-1d1a-4c55-9b0e-3a7f0d5e8c21",
  "type": "orchestration.SubmitTask",
  "actor": { "kind": "owner", "id": "<owner id>" },
  "payload": { "text": "Write a poem", "mode": "auto" }
}
```

- `commandId` is the idempotency key: a lower-case UUID v4. api-gateway
  takes it from the `Idempotency-Key` header.
- `type` must be `orchestration.SubmitTask`.
- `expectedVersion` is optional. If it is there, it must be `0`.
- `actor.kind` must be `owner` in M1. api-gateway sends the owner.
- `mode` is `auto` or `approval` (the default).

| Status   | When                                | Body                                             |
| -------- | ----------------------------------- | ------------------------------------------------ |
| 201      | A new task                          | `{ "taskId": "<uuid>", "version": 1 }`           |
| 200      | The same `commandId` came before    | The first answer again. No new event.            |
| 400      | The body is not valid               | Problem JSON with `errors` (paths, never values) |
| 409      | The stream exists (a task id clash) | Problem JSON, `code` `conflict`                  |
| 413, 415 | The body is too large, or not JSON  | Problem JSON                                     |
| 503      | MongoDB does not answer in 5 s      | Problem JSON, `code` `unavailable`, no detail    |

The service makes the task id (UUID v4). It appends with
`expectedVersion: 0`, so it never reads the stream first. The event has
`correlationId` = the task id, `causationId` = the `commandId`, and the
`actor` of the command. The event and the stored answer are in one
MongoDB transaction.

### `GET /internal/events`

| Query           | Default | Rule               |
| --------------- | ------- | ------------------ |
| `afterPosition` | `0`     | integer, 0 or more |
| `limit`         | `100`   | integer, 1 to 1000 |

Only the digits 0 to 9 are permitted. An empty value, a space, a sign, a
hex value, an exponent or a decimal point gives 400. MongoDB down gives 503.

The answer is `{ "events": [ ... ] }`. Each item is an `EventEnvelopeV1`
with its `position` (1, 2, 3 ... in commit order). Take the `position` of
the last item as the next `afterPosition`. An empty list is the end.

## Configuration

Environment variables (`.env.example` has all of them):

| Variable                                                            | Default         | Content                                   |
| ------------------------------------------------------------------- | --------------- | ----------------------------------------- |
| `MONGODB_URI`                                                       | required        | MongoDB connection string (replica set)   |
| `MONGODB_DATABASE`                                                  | `orchestration` | Database name                             |
| `NATS_SERVERS`                                                      | required        | NATS URLs, comma-separated; at least one  |
| `NATS_USER`                                                         | required        | `ai-systems-orchestration` on the cluster |
| `NATS_PASSWORD`                                                     | required        | From the SealedSecret                     |
| `PORT`                                                              | `8080`          | HTTP port                                 |
| `LOG_LEVEL`                                                         | `info`          | pino level                                |
| `RELEASE`                                                           | `local`         | Release id in logs and metrics            |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | not set         | Traces go over OTLP only when one is set  |

A bad value stops the process at start with the variable name. An empty
value is a value: delete the line to get the default. The kit service
name is fixed to `orchestration`, so the NATS inbox prefix is
`_INBOX_orchestration` and the grants of ADR 0023 apply.

At start the service connects to MongoDB (it logs "connecting to
MongoDB"), runs the migration `000-event-store`, connects to NATS (it logs
"connecting to NATS"), starts the relay, and listens. A SIGTERM or SIGINT
before the server listens stops the process at once with exit code 143.
After that, the graceful shutdown of `kit/http` applies.

NATS and the relay are not readiness checks (ARCHITECTURE 14). When NATS
is down or the stream `ORCHESTRATION` does not exist, the service still
takes commands, and the events wait in the outbox. The relay publishes
them later. The service does not make streams: the gitops stream Job makes
`ORCHESTRATION`. A stuck relay shows in the metrics
`outbox_oldest_pending_seconds` and `outbox_publish_failures_total`. Put
an alert on the gauge. NATS must be available at start, because the kit
waits for the first NATS connection.

## Run locally

Node 26 (`nvm use`), and a MongoDB replica set and NATS with JetStream and
the stream `ORCHESTRATION`. The `compose.yaml` in `ai-systems-kit` starts
them (task M1-05).

```sh
npm ci
cp .env.example .env
node --env-file=.env src/main.ts
```

In `.env`, fill in the required values (`MONGODB_URI`, `NATS_SERVERS`,
`NATS_USER`, `NATS_PASSWORD`) and delete the lines that you do not set.
The service refuses an empty value; it does not use the default for it.

## Container image

```sh
docker build --build-arg RELEASE=$(git rev-parse HEAD) -t ai-systems-orchestration .
```

Node 26 slim, pinned by digest, with no npm, no npx and no test packages
(`npm ci --omit=dev --omit=optional`). The entrypoint is
`node src/main.ts` (type stripping) as uid 1000, with no shell script. It
writes nothing to disk, so a read-only root file system works. There is no
`HEALTHCHECK`; Kubernetes uses `/health` and `/ready`.

Release: when `ci` passes on a push to `main`, `.github/workflows/release.yml`
builds the image of that commit, makes a CycloneDX SBOM (a workflow
artifact), and scans the image with grype. A HIGH or CRITICAL finding that
has a fix stops the release. Findings with no fix do not stop it; they are
in the job log. Then it pushes `ghcr.io/famashines/ai-systems-orchestration:sha-<commit>`
and opens or updates the pull request from the branch
`release/ai-systems-orchestration-prod` in `famashines/gitops`. That pull request sets the
tag and the digest in `kubernetes/ai-systems/ai-systems-orchestration/prod`. The owner
merges it. The job needs the org secret `GITOPS_TOKEN` with access to this
repo. A manual run (`workflow_dispatch`) works only on `main`.

## Test

| Command                    | Does                                                 |
| -------------------------- | ---------------------------------------------------- |
| `npm run verify`           | type check, lint, unit tests with the coverage gates |
| `npm run test:integration` | MongoDB and NATS in Testcontainers (needs Docker)    |

The integration test uses the NATS grants of the user
`ai-systems-orchestration` from gitops, and an admin user that makes the
stream. Commands for agents: `CLAUDE.md`.
