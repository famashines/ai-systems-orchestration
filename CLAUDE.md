# CLAUDE.md for ai-systems-orchestration

Rules for all ai-systems repos: `ai-systems-docs/CLAUDE.md`
(local: `../../docs/CLAUDE.md`). What the service does: `README.md`.

## Commands

| Command                    | Does                                                             |
| -------------------------- | ---------------------------------------------------------------- |
| `npm ci`                   | install from the lock file (builds kit and contracts on install) |
| `npm run check`            | `tsc --noEmit` (TypeScript 7)                                    |
| `npm run lint`             | ESLint (strict type-checked) and Prettier check                  |
| `npm test`                 | unit tests, two coverage gates (see below)                       |
| `npm run verify`           | check, lint and test                                             |
| `npm run test:integration` | `test/integration` with Testcontainers (needs Docker)            |
| `npm run test:e2e`         | `test/e2e`; set `E2E_DIR` to a scratch folder, else it skips     |
| `npm start`                | `node src/main.ts`                                               |

## Layout

```
src
├── domain/state.ts            task state, evolve (the status table), runaway rules, no I/O
├── domain/run.ts              run process manager: decideRun, schedule, subtask calls, no I/O
├── domain/task.ts             decide (M2 chat path and dispatch), steer and refusals, no I/O
├── application/run.ts         act (read, decide, load agents, schedule, append), onTimer
├── application/submit-task.ts the command handler
├── application/chat.ts        the handlers of OwnerMessagePosted and llm.*
├── adapters/http.ts           routes, zod schemas of the bodies and queries
├── adapters/timers.ts         timer loop over SubtaskRetrying and CheckpointOpened, migration 001-timers
├── adapters/writer.ts         reads of agent-manager (by name, by id) and prompt-studio
└── main.ts                    config, wiring by constructor, start, shutdown
test
├── nats.ts                    gitops NATS grants and streams for the tests
├── unit                       node:test, kit in-memory event store
├── integration                MongoDB and NATS in Testcontainers, fakes for the reads
└── e2e                        the real sibling services (E2E_DIR), the ai-proxy mock
```

Orchestration has only the kit collections (`events`, `commands`,
`counters`, `processed`; ARCHITECTURE 7). Its one own migration,
`001-timers`, adds two partial indexes on `events` for the timers; it is
in `src/adapters/timers.ts` and in the `runMigrations` list in `main.ts`.
The timers use no new collection: a fired timer is a `processed` marker
with the `eventId` of its `SubtaskRetrying` or `CheckpointOpened`.

## Coverage gates (ADR 0017)

`npm test` runs the unit tests two times, because node:test thresholds are
for all included files together:

- `test:domain`: `src/domain`, 90% lines and 85% branches.
- `test:unit`: `src` without `main.ts`, 70% lines.

`test/unit/run.test.ts` is the table test of the run process manager:
one row for each line of the contracts status table, and a test for each
input in each state. Add a row or a test for each new event or input.

`main.ts` is wiring. The integration test covers it through `start()`.

## Integration tests

`test/nats.ts` has the grants of the users from gitops
(`kubernetes/infrastructure/nats/values.yaml`; orchestration as gitops
#185 sets it). Change them together with gitops. `test/e2e` pins the
commits of the sibling services; move them when a sibling changes a
contract that orchestration uses. Wait with `waitFor`, never with a fixed sleep. Pass
`drainDelayMs: 0` to `start()`. The restart test calls `start()` two times
in one process, so `startTelemetry` runs two times. This is only in the
tests.

The CI job `integration` runs on `ubuntu-22.04`: `mongo:8.0.34` does not
start on the kernel of `ubuntu-24.04`. See the kit `CLAUDE.md`.

## Dependencies

- Kit and contracts come from git tags. A new tag needs a new
  `allowScripts` entry with the commit of the tag
  (`npm install-scripts approve <package>`).
- `mongodb` and `zod` have the exact versions that the kit pins, so there
  is one copy of each. Check with `npm ls zod mongodb`.
- TypeScript 6 (`typescript`, for ESLint) and 7 (`typescript-7`, for
  `check`): ADR 0022. Never run `npx tsc`.
- The Docker build stage uses the full Node image, because npm needs `git`
  to install the kit and contracts. The runtime stage is slim.
