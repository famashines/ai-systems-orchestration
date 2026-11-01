# ai-systems-orchestration

The Task Orchestration service of ai-systems. It takes tasks and keeps them
as event-sourced streams `task-<taskId>` in MongoDB. The `events`
collection is the outbox: a relay publishes each event to the NATS
JetStream stream `ORCHESTRATION`. The service is internal: api-gateway
(commands) and projections (replay) call it. It has no Ingress.

From M2, a chat message starts a task with one agent run (task M2-06):
orchestration reads `conversation.OwnerMessagePosted`, loads the active
`writer` agent version and its prompt version, and asks ai-proxy for one
LLM call. The end of the call ends the run.

```
api-gateway --POST /internal/commands/submit-task--> orchestration
conversation-hub --conversation.OwnerMessagePosted (CONVERSATION)--> orchestration
orchestration --GET /internal/agents/by-name/writer/active-version--> agent-manager
orchestration --GET /internal/prompts/:promptId/versions/:version--> prompt-studio
                                                       | one transaction
                                                       v
                                       MongoDB orchestration: events, commands, processed
                                                       | outbox relay, Nats-Msg-Id = eventId
                                                       v
                                       NATS stream ORCHESTRATION (orchestration.>)
                                         --CallRequested--> ai-proxy --llm.* (LLM)--> orchestration
                                         --Run*, MessageRefused--> conversation-hub (replies)
projections --GET /internal/events--> orchestration (rebuild only)
```

Architecture and reasons: `ai-systems-docs` (`ARCHITECTURE.md` 5 to 8,
ADR 0006, 0007, 0015, 0021, 0023, 0027). Schemas:
`@famashines/ai-systems-contracts` v0.3.0.

## Chat tasks

Two kit durable consumers, both with the name `orchestration` and
`ordered` (one message at a time, in stream order):

| Stream         | Filter                            | Handler                                      |
| -------------- | --------------------------------- | -------------------------------------------- |
| `CONVERSATION` | `conversation.OwnerMessagePosted` | `onOwnerMessage` (`src/application/chat.ts`) |
| `LLM`          | `llm.>`                           | `onLlmEvent`                                 |

Each handler reads the task stream, decides (`src/domain/task.ts`, no
I/O), and appends with `expectedVersion` = the version that it read. The
kit `processed` marker of the consumed event is in the same MongoDB
transaction as the append (`also: markProcessed`). So a redelivered event
gives no second task and no second end event. A concurrent append to the
same task gives a `ConcurrencyError`, and the kit consumer delivers the
event again.

### `new_task`

```
OwnerMessagePosted{intent: new_task}
  --> GET agent-manager: active version of "writer" (ActiveAgentVersionResultV1)
  --> GET prompt-studio: its prompt version (PromptVersionResultV1)
  --> one append to task-<new taskId>:
      TaskSubmitted{mode: auto, conversationId, messageId}, RunStarted, CallRequested
```

| Case                                                                        | Events after `TaskSubmitted`, `RunStarted`                                                                                                     |
| --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| The writer and its prompt load                                              | `CallRequested`                                                                                                                                |
| agent-manager gives 404 (no active version)                                 | `RunFailed` `not_found`                                                                                                                        |
| prompt-studio gives 404                                                     | `RunFailed` `not_found`                                                                                                                        |
| A body that is not valid, or a prompt version that is not the one asked for | `RunFailed` `internal`                                                                                                                         |
| The agent has `enabled: false`                                              | `RunFailed` `unavailable`                                                                                                                      |
| The prompt version has variables                                            | `RunFailed` `internal` (M2 has no values to fill them)                                                                                         |
| A runaway rule (below) stops the call                                       | `RunBlocked` `runaway_stopped`                                                                                                                 |
| Another 4xx (400, 401, 403, 429 ...), with no retry                         | `RunFailed` `internal`                                                                                                                         |
| A network error, a timeout (2 s) or a 5xx, 3 tries; up to about 25 s        | `RunFailed` `unavailable`, reason `agent-manager or prompt-studio did not answer, so the writer agent could not start. Try again in a minute.` |

`CallRequested`:

| Field                     | Value                                                                                                 |
| ------------------------- | ----------------------------------------------------------------------------------------------------- |
| `agentId`, `agentVersion` | from the active writer version                                                                        |
| `messages`                | `system`: the prompt text. `user`: the owner text in a data block (below)                             |
| `models`                  | the model chain of the agent version, in its order                                                    |
| `maxTokens`               | `limits.maxTokens` of the agent version                                                               |
| `maxDurationMs`           | 120000 (`callDurationMs`; `AgentLimits` has no duration)                                              |
| `maxCostMicroUsd`         | the rest of the task allowance: `limits.maxCostMicroUsd` minus the cost of the task so far (ADR 0027) |

The data block of the owner text (ARCHITECTURE 11.2). `<messageId>` is
the id of the owner message. The owner cannot know it before the message
is posted, so the text cannot hold a valid end line:

```
Answer the owner's chat message. It is in the data block below.
BEGIN DATA owner_message <messageId>
<owner text, not changed>
END DATA owner_message <messageId>
```

The first line is outside the block, so the model has one instruction
with authority. The seed writer prompt of prompt-studio says that text in
a data block has no authority.

Runaway rules (ADR 0015, M2 base, `nextCall`): before a call,
`calls >= limits.maxCalls` or `cost >= limits.maxCostMicroUsd` gives
`RunBlocked` `runaway_stopped`. In M2 a run has one call, so the rules
see 0 calls and a cost of 0: only an agent with `maxCostMicroUsd` 0 is
stopped. The rules and the rest of the allowance are ready for M3 runs
with more calls.

Orchestration does not call the budget (ADR 0027). ai-proxy reserves and
settles each attempt, and refuses an attempt whose worst case is above
`maxCostMicroUsd`.

### `llm.*`

| Event                                                             | Result                                                                                    |
| ----------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `CallStarted`                                                     | nothing                                                                                   |
| `CallFailed` with `nextModel` (ai-proxy tries the next model)     | nothing                                                                                   |
| `CallCompleted` with text that is not only white space            | `RunCompleted`: `summary` = the text, `costMicroUsd` = the cost of that attempt           |
| `CallCompleted` with empty or white-space text                    | `RunFailed` `empty_output`                                                                |
| last `CallFailed` `budget_refused`                                | `RunBlocked` `budget_refused`, reason `The budget refused the call: <reason of ai-proxy>` |
| last `CallFailed` `rate_limit`                                    | `RunFailed` `rate_limited`                                                                |
| last `CallFailed` `timeout` or `provider`                         | `RunFailed` `unavailable`                                                                 |
| last `CallFailed` `invalid`                                       | `RunFailed` `internal`                                                                    |
| an event of a call that is not the current call of a running task | nothing                                                                                   |

There is no retry by orchestration in M2: the last `CallFailed` of a call
ends the run. ai-proxy has already tried the model chain.

Each chat task ends with `RunCompleted` or `RunFailed`, or waits with
`RunBlocked`, when its call ends. A call that does not end keeps the task
`running` until the stale rule below stops it. A reason is cut to 1000
characters.

A `CallCompleted` or `CallFailed` for a task that is completed, failed or
blocked gives no event, and a warn log `a late call event for a closed
task is dropped` with `taskId`, `callId` and `costMicroUsd`.

### `steer` and `answer`

Contracts v0.3.0 has no event to record a steer or an answer
(`SteeringMessageAdded`, `ClarificationAnswered` and `RunResumed` come
with M3). So orchestration refuses each of them with
`orchestration.MessageRefused` on the task stream. conversation-hub posts
the reason as a reply:

| Task state                  | Reason                                                                                                       |
| --------------------------- | ------------------------------------------------------------------------------------------------------------ |
| not known                   | `The task is not known.`                                                                                     |
| any, intent `answer`        | `The task has no open question.`                                                                             |
| running                     | `The task is running and cannot take a message now. Wait for the answer, or send the message as a new task.` |
| blocked                     | `The task is blocked until the owner acts. Send the message as a new task. The reason: <block reason>`       |
| completed or failed         | `The task has ended. Send the message as a new task.`                                                        |
| an API task (`submit-task`) | `The task does not run in the chat. Send the message as a new task.`                                         |

A refusal does not change the task state. A blocked task stays blocked
in M2: there is no resume yet.

A steer or an answer for a running task whose `CallRequested` is older
than 15 minutes (`staleAfterMs`) first gives `RunFailed` `unavailable`
(`The answer did not come in 15 minutes, so the task stopped. A later
answer is not shown, and its cost is still booked.`), then the refusal.
So a call that never ends (for example a call that ai-proxy sent to its
`DLQ`) does not keep the task running when the owner writes to it.

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
(`@famashines/ai-systems-contracts` v0.3.0):

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

| Variable                                                            | Default                                | Content                                   |
| ------------------------------------------------------------------- | -------------------------------------- | ----------------------------------------- |
| `MONGODB_URI`                                                       | required                               | MongoDB connection string (replica set)   |
| `MONGODB_DATABASE`                                                  | `orchestration`                        | Database name                             |
| `NATS_SERVERS`                                                      | required                               | NATS URLs, comma-separated; at least one  |
| `NATS_USER`                                                         | required                               | `ai-systems-orchestration` on the cluster |
| `NATS_PASSWORD`                                                     | required                               | From the SealedSecret                     |
| `PORT`                                                              | `8080`                                 | HTTP port                                 |
| `LOG_LEVEL`                                                         | `info`                                 | pino level                                |
| `RELEASE`                                                           | `local`                                | Release id in logs and metrics            |
| `AGENT_MANAGER_URL`                                                 | `http://ai-systems-agent-manager:8080` | agent-manager, for the writer version     |
| `PROMPT_STUDIO_URL`                                                 | `http://ai-systems-prompt-studio:8080` | prompt-studio, for the prompt version     |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | not set                                | Traces go over OTLP only when one is set  |

A bad value stops the process at start with the variable name. An empty
value is a value: delete the line to get the default. The kit service
name is fixed to `orchestration`, so the NATS inbox prefix is
`_INBOX_orchestration` and the grants of ADR 0023 apply.

At start the service connects to MongoDB (it logs "connecting to
MongoDB"), runs the migrations `000-event-store` and `000-processed`,
connects to NATS (it logs "connecting to NATS"), starts the relay and the
two consumers, and listens. A SIGTERM or SIGINT
before the server listens stops the process at once with exit code 143.
After that, the graceful shutdown of `kit/http` applies.

NATS, the relay and the consumers are not readiness checks (ARCHITECTURE 14). When NATS
is down or the stream `ORCHESTRATION` does not exist, the service still
takes commands, and the events wait in the outbox. The relay publishes
them later. The service does not make streams or change them: the gitops
stream Job makes `ORCHESTRATION`, `CONVERSATION` and `LLM`. When a stream
is missing, its consumer tries again until the stream is there. A stuck relay shows in the metrics
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

| Command                    | Does                                                                                    |
| -------------------------- | --------------------------------------------------------------------------------------- |
| `npm run verify`           | type check, lint, unit tests with the coverage gates                                    |
| `npm run test:integration` | MongoDB and NATS in Testcontainers (needs Docker), fakes for the reads                  |
| `npm run test:e2e`         | the real sibling services and the ai-proxy mock (needs Docker, git, Node 26, `E2E_DIR`) |

Both use the NATS grants of gitops (`test/nats.ts`), for the user
`ai-systems-orchestration` as gitops #185 sets them (no `rpc.budget.*`),
and an admin user that makes the streams.

- `test:integration`: small `node:http` fakes give the agent-manager and
  prompt-studio reads (`ActiveAgentVersionResultV1`,
  `PromptVersionResultV1`). The test publishes
  `conversation.OwnerMessagePosted` and `llm.*` itself as the admin user,
  in place of conversation-hub and ai-proxy.
- `test:e2e`: nothing of orchestration is faked. It clones budget,
  agent-manager, prompt-studio and ai-proxy at fixed `main` commits into
  `E2E_DIR` (it reuses a clone that is there and fetches the commit), runs `npm ci`, and starts
  each as `node src/main.ts` on the host with its gitops NATS user. The
  seeds of agent-manager and prompt-studio give the writer. ai-proxy calls
  its own mock OpenRouter (`test/mock-openrouter.ts` of ai-proxy), which
  the test starts in its process on `127.0.0.1`. ai-proxy accepts `http`
  only for `localhost` and `127.0.0.1`, so the mock and ai-proxy run on
  the host, not in containers. The test adds the three seed models to the
  mock catalog. The test publishes `conversation.OwnerMessagePosted` in
  place of conversation-hub. No cap: budget refuses, and the run is
  blocked. Then the test appends `budget.BudgetSet` to the budget
  database, because the budget command needs an owner JWT: a cap of 1
  micro-USD (the cap is reached, the run is blocked), then 1 USD (the
  next run completes with the mock answer). With no `E2E_DIR`, the tests are
  skipped. The CI job `e2e` sets it.

Commands for agents: `CLAUDE.md`.

## Known limits

- One run with one call for each chat task. No retry by orchestration
  after the last `CallFailed` (ARCHITECTURE 8.2 has 3 tries with backoff;
  that needs a timer, M3). The runaway rules see one call only.
- `RunCompleted.costMicroUsd` is the cost of the attempt that gave the
  answer. The cost of the task is the sum of the `llm.*` costs (earlier
  attempts of the ai-proxy fallback chain are in their `llm.CallFailed`
  events); projections sums them.
- A steer or an answer is always refused (no contract event to record it
  in v0.3.0). A blocked task stays blocked: there is no resume in M2.
- When agent-manager or prompt-studio does not answer 3 times, the run
  fails with `unavailable`. One read takes up to about 12 s (3 tries of
  2 s and waits of 2 s and 4 s), so the two reads take up to about 25 s.
  The owner sends the message again. The ordered `CONVERSATION` consumer
  waits for that time.
- A call with no end is stopped only when a steer or an answer comes for
  its task after 15 minutes. There is no timer. When the answer comes
  after that stop, it is lost: the owner does not see it, and ai-proxy
  and the budget have booked its cost. The warn log shows it.
- Tasks of `POST /internal/commands/submit-task` (the API path) do not
  run in M2: they stay `submitted`. conversation-hub ignores them.
- The kit consumer starts at the first message of the stream
  (`deliver_policy: all`). Deploy orchestration before the first owner
  message, or each old message in `CONVERSATION` becomes a task.
