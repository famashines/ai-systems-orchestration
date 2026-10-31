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
| `npm start`                | `node src/main.ts`                                               |

## Layout

```
src
├── domain/task.ts             decide, evolve, no I/O
├── application/submit-task.ts the command handler
├── adapters/http.ts           routes, zod schemas of the bodies and queries
└── main.ts                    config, wiring by constructor, start, shutdown
test
├── unit                       node:test, kit in-memory event store
└── integration                MongoDB and NATS in Testcontainers
```

No own migrations yet: orchestration has only the kit collections
(ARCHITECTURE 7). An own index script goes in `src/migrations/NNN-name.ts`
and into the `runMigrations` list in `main.ts`.

## Coverage gates (ADR 0017)

`npm test` runs the unit tests two times, because node:test thresholds are
for all included files together:

- `test:domain`: `src/domain`, 90% lines and 85% branches.
- `test:unit`: `src` without `main.ts`, 70% lines.

`main.ts` is wiring. The integration test covers it through `start()`.

## Integration tests

The NATS config in the test has the grants of the user
`ai-systems-orchestration` and `ai-systems-admin` from gitops
(`kubernetes/infrastructure/nats/values.yaml`). Change them together with
gitops. Wait with `waitFor`, never with a fixed sleep. Pass
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
