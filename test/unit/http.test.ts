import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import type { JwtVerifier } from "@famashines/ai-systems-kit/auth";
import { ConcurrencyError } from "@famashines/ai-systems-kit/event-store";
import { createService, ForbiddenError, UnauthorizedError } from "@famashines/ai-systems-kit/http";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import { createMemoryEventStore, fakeClock, fakeIds } from "@famashines/ai-systems-kit/testing";
import { commandPath, routes } from "../../src/adapters/http.ts";
import { event, type TaskEvent } from "../../src/domain/task.ts";

const clock = fakeClock();
const store = createMemoryEventStore({ now: clock.now, newId: fakeIds(1) });
const taskIds = fakeIds(2);
let nextTaskId = taskIds;
const logger = createLogger({ service: "orchestration", release: "test", level: "silent" });
const tokens: Record<string, JwtVerifier> = {
  owner: () => Promise.resolve({ sub: "owner-1", roles: ["owner"], exp: 0 }),
  guest: () => Promise.reject(new ForbiddenError("The role is missing")),
};
const verify: JwtVerifier = (token) =>
  (tokens[token] ?? (() => Promise.reject(new UnauthorizedError())))(token);
const agentId = "00000000-0000-4000-8000-00000000a005";
let failingAppends = 0;
const downAgent = "00000000-0000-4000-8000-00000000a0dd";
const unknownAgent = "00000000-0000-4000-8000-00000000a0ee";
const app = createService({
  logger,
  routes: routes({
    store: {
      ...store,
      append: (input) => {
        if (failingAppends > 0) {
          failingAppends -= 1;
          return Promise.reject(new ConcurrencyError());
        }
        return store.append(input);
      },
    },
    newId: () => nextTaskId(),
    now: clock.now,
    logger,
    settings: {
      maxParallel: 3,
      maxCallsPerTask: 50,
      retryDelaysMs: [5000],
      checkpointTimeoutMs: 1,
    },
    loadAgentByName: () => Promise.resolve({ code: "not_found", reason: "No agent." }),
    loadAgent: (id) =>
      Promise.resolve(
        id === downAgent
          ? { code: "unavailable" as const, reason: "No answer." }
          : id === unknownAgent
            ? { code: "not_found" as const, reason: "No agent." }
            : {
                agent: {
                  agentId,
                  name: "writer",
                  enabled: true,
                  version: 1,
                  prompt: { promptId: agentId, version: 1 },
                  models: ["openai/gpt-4.1-nano"],
                  limits: { maxCalls: 10, maxTokens: 1000, maxCostMicroUsd: 50_000 },
                },
                prompt: {
                  promptId: agentId,
                  name: "writer",
                  version: 1,
                  text: "You write.",
                  variables: [],
                },
              },
      ),
    verify,
  }),
  drainDelayMs: 0,
});
let base: string;

before(async () => {
  const { port } = await app.listen(0, "127.0.0.1");
  base = `http://127.0.0.1:${String(port)}`;
});

after(async () => {
  await app.close();
});

const commandIds = fakeIds(3);
const owner = { kind: "owner", id: "owner-1" } as const;

const envelope = (overrides: Record<string, unknown> = {}) => ({
  commandId: commandIds(),
  type: "orchestration.SubmitTask",
  actor: owner,
  payload: { text: "Write a poem", mode: "auto" },
  ...overrides,
});

const submit = (body: unknown, token: string | null = "owner") =>
  fetch(`${base}/internal/commands/submit-task`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(token === null ? {} : { authorization: `Bearer ${token}` }),
    },
    body: JSON.stringify(body),
  });

const events = async (query: string) => {
  const response = await fetch(`${base}/internal/events${query}`);
  return { status: response.status, body: (await response.json()) as Record<string, unknown> };
};

test("SubmitTask gives 201 with the task id, and appends TaskSubmitted", async () => {
  const command = envelope();
  const response = await submit(command);
  assert.equal(response.status, 201);
  const answer = (await response.json()) as { taskId: string; version: number };
  assert.equal(answer.version, 1);
  const stream = await store.readStream(`task-${answer.taskId}`);
  assert.equal(stream.length, 1);
  assert.deepEqual(stream[0], {
    eventId: stream[0]?.eventId,
    type: "orchestration.TaskSubmitted",
    schemaVersion: 1,
    streamId: `task-${answer.taskId}`,
    version: 1,
    occurredAt: clock.now().toISOString(),
    correlationId: answer.taskId,
    causationId: command.commandId,
    actor: owner,
    payload: { taskId: answer.taskId, text: "Write a poem", mode: "auto" },
    position: stream[0]?.position,
  });
});

test("the same commandId gives 200 with the same answer and no second event", async () => {
  const command = envelope();
  const first = await submit(command);
  const before = (await store.readAll({ limit: 1000 })).length;
  const second = await submit(command);
  assert.equal(first.status, 201);
  assert.equal(second.status, 200);
  assert.deepEqual(await second.json(), await first.json());
  assert.equal((await store.readAll({ limit: 1000 })).length, before);
});

test("mode is approval when the payload has no mode", async () => {
  const response = await submit(envelope({ payload: { text: "Plan a trip" } }));
  const { taskId } = (await response.json()) as { taskId: string };
  const [event] = await store.readStream(`task-${taskId}`);
  assert.equal(event?.type, "orchestration.TaskSubmitted");
  assert.equal(event.payload.mode, "approval");
});

test("SubmitTask on an existing stream gives 409", async () => {
  const taskId = taskIds();
  nextTaskId = () => taskId;
  assert.equal((await submit(envelope())).status, 201);
  const response = await submit(envelope());
  nextTaskId = taskIds;
  assert.equal(response.status, 409);
  assert.equal(response.headers.get("content-type"), "application/problem+json");
  assert.equal(((await response.json()) as { code: string }).code, "conflict");
});

for (const [name, body, path] of [
  ["no text", envelope({ payload: {} }), "body.payload.text"],
  ["a blank text", envelope({ payload: { text: "  " } }), "body.payload.text"],
  ["a bad mode", envelope({ payload: { text: "a", mode: "fast" } }), "body.payload.mode"],
  ["a bad commandId", envelope({ commandId: "not-a-uuid" }), "body.commandId"],
  [
    "an upper-case commandId",
    envelope({ commandId: "0000000A-0000-4000-8000-00000000000B" }),
    "body.commandId",
  ],
  ["another command type", envelope({ type: "orchestration.CancelRun" }), "body.type"],
  ["an expectedVersion that is not 0", envelope({ expectedVersion: 3 }), "body.expectedVersion"],
  ["no actor", envelope({ actor: undefined }), "body.actor"],
  ["an agent actor", envelope({ actor: { kind: "agent", id: "agent-1" } }), "body.actor.kind"],
  ["a system actor", envelope({ actor: { kind: "system", id: "cron" } }), "body.actor.kind"],
] as const) {
  test(`SubmitTask with ${name} gives 400 problem JSON`, async () => {
    const response = await submit(body);
    assert.equal(response.status, 400);
    assert.equal(response.headers.get("content-type"), "application/problem+json");
    const problem = (await response.json()) as { code: string; errors: { path: string }[] };
    assert.equal(problem.code, "validation_failed");
    assert.ok(problem.errors.some((issue) => issue.path === path));
  });
}

test("SubmitTask with expectedVersion 0 gives 201", async () => {
  assert.equal((await submit(envelope({ expectedVersion: 0 }))).status, 201);
});

test("the replay API gives events in position order, in pages", async () => {
  const all = await events("?limit=1000");
  assert.equal(all.status, 200);
  const list = all.body.events as { position: number; eventId: string }[];
  assert.ok(list.length >= 3);
  assert.deepEqual(
    list.map((event) => event.position),
    list.map((_, index) => index + 1),
  );
  const page = await events("?afterPosition=1&limit=2");
  assert.deepEqual(page.body.events, list.slice(1, 3));
  const end = await events(`?afterPosition=${String(list.length)}`);
  assert.deepEqual(end.body.events, []);
});

for (const query of [
  "?limit=1001",
  "?limit=0",
  "?limit=",
  "?limit=%2010",
  "?limit=1e2",
  "?limit=0x10",
  "?limit=1.5",
  "?afterPosition=-1",
  "?afterPosition=-0",
  "?afterPosition=x",
  "?afterPosition=",
  "?afterPosition=99999999999999999999",
]) {
  test(`the replay API with ${query} gives 400`, async () => {
    assert.equal((await events(query)).status, 400);
  });
}

const ownerPaths = [
  "approve-plan",
  "edit-plan",
  "reject-plan",
  "pause-run",
  "resume-run",
  "cancel-run",
  "cancel-task",
  "retry-subtask",
  "reassign-subtask",
  "set-task-mode",
  "resolve-checkpoint",
];

test("each owner command has a kebab-case path", () => {
  assert.equal(commandPath("ApprovePlan"), "/internal/commands/approve-plan");
  assert.equal(commandPath("ResolveCheckpoint"), "/internal/commands/resolve-checkpoint");
});

const command = (path: string, body: unknown, token: string | null = "owner") =>
  fetch(`${base}/internal/commands/${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(token === null ? {} : { authorization: `Bearer ${token}` }),
    },
    body: JSON.stringify(body),
  });

const problemOf = async (response: Response) => ({
  status: response.status,
  code: ((await response.json()) as { code: string }).code,
});

const seed = async (): Promise<string> => {
  const taskId = taskIds();
  const subtaskId = taskIds();
  const history: TaskEvent[] = [
    event("orchestration.TaskSubmitted", { taskId, text: "Write.", mode: "approval" }),
    event("orchestration.PlanProposed", {
      taskId,
      planVersion: 1,
      subtasks: [
        {
          subtaskId,
          title: "A",
          instructions: "Do it.",
          agentId,
          dependsOn: [],
          needsApproval: false,
        },
      ],
      awaitsApproval: true,
    }),
  ];
  await store.append({
    streamId: `task-${taskId}`,
    expectedVersion: 0,
    events: history.map((item) => ({
      ...item,
      correlationId: taskId,
      causationId: taskId,
      actor: { kind: "system", id: "test" },
    })),
  });
  return taskId;
};

const approve = (taskId: string, overrides: Record<string, unknown> = {}) => ({
  commandId: commandIds(),
  type: "orchestration.ApprovePlan",
  actor: owner,
  payload: { taskId, planVersion: 1 },
  ...overrides,
});

test("ApprovePlan gives 201, the owner is the actor of the command events, and the same commandId gives 200", async () => {
  const taskId = await seed();
  const body = approve(taskId);
  const first = await command("approve-plan", body);
  assert.equal(first.status, 201);
  const answer = (await first.json()) as { taskId: string; version: number };
  const stream = await store.readStream(`task-${taskId}`);
  assert.deepEqual(answer, { taskId, version: stream.length });
  assert.deepEqual(
    stream.slice(2).map((item) => [item.type, item.actor.kind, item.causationId]),
    [
      ["orchestration.PlanApproved", "owner", body.commandId],
      ["orchestration.RunStarted", "owner", body.commandId],
      ["orchestration.SubtaskQueued", "owner", body.commandId],
      ["orchestration.SubtaskStarted", "system", body.commandId],
      ["orchestration.CallRequested", "system", body.commandId],
    ],
  );
  const again = await command("approve-plan", body);
  assert.equal(again.status, 200);
  assert.deepEqual(await again.json(), answer);
  assert.equal((await store.readStream(`task-${taskId}`)).length, stream.length);
  const pause = { ...approve(taskId), type: "orchestration.PauseRun", payload: { taskId } };
  assert.equal((await command("pause-run", pause)).status, 201);
  assert.deepEqual(await (await command("pause-run", pause)).json(), {
    taskId,
    version: stream.length + 1,
  });
});

test("each owner command checks the token before the body: 401 with no token, 403 with no owner role", async () => {
  for (const path of ownerPaths) {
    assert.deepEqual(await problemOf(await command(path, {}, null)), {
      status: 401,
      code: "unauthorized",
    });
    assert.deepEqual(await problemOf(await command(path, {}, "guest")), {
      status: 403,
      code: "forbidden",
    });
    assert.deepEqual(await problemOf(await command(path, {})), {
      status: 400,
      code: "validation_failed",
    });
  }
});

test("an actor that is not the subject of the token gives 403 with no event", async () => {
  const taskId = await seed();
  const body = approve(taskId, { actor: { kind: "owner", id: "someone-else" } });
  assert.deepEqual(await problemOf(await command("approve-plan", body)), {
    status: 403,
    code: "forbidden",
  });
  assert.equal((await store.readStream(`task-${taskId}`)).length, 2);
});

test("a bad command body gives 400 with the path", async () => {
  const taskId = await seed();
  for (const [body, path] of [
    [approve(taskId, { type: "orchestration.PauseRun" }), "body.type"],
    [approve(taskId, { expectedVersion: 2 }), "body.expectedVersion"],
    [approve(taskId, { actor: { kind: "system", id: "owner-1" } }), "body.actor.kind"],
    [approve(taskId, { payload: { taskId } }), "body.payload.planVersion"],
    [approve(taskId, { payload: { taskId: "x", planVersion: 1 } }), "body.payload.taskId"],
  ] as const) {
    const response = await command("approve-plan", body);
    assert.equal(response.status, 400);
    const problem = (await response.json()) as { errors: { path: string }[] };
    assert.ok(
      problem.errors.some((issue) => issue.path === path),
      `${path} ${JSON.stringify(problem.errors)}`,
    );
  }
});

test("an unknown task gives 404; an old plan version or a wrong state gives 409", async () => {
  assert.deepEqual(await problemOf(await command("approve-plan", approve(taskIds()))), {
    status: 404,
    code: "not_found",
  });
  const taskId = await seed();
  assert.deepEqual(
    await problemOf(
      await command("approve-plan", approve(taskId, { payload: { taskId, planVersion: 2 } })),
    ),
    { status: 409, code: "conflict" },
  );
  const pause = { ...approve(taskId), type: "orchestration.PauseRun", payload: { taskId } };
  const response = await command("pause-run", pause);
  assert.equal(response.status, 409);
  assert.equal(((await response.json()) as { detail: string }).detail, "The run is not running.");
  assert.equal((await store.readStream(`task-${taskId}`)).length, 2);
});

test("SubmitTask checks the owner token before the body, and the actor must be the subject", async () => {
  const before = (await store.readAll({ limit: 1000 })).length;
  assert.deepEqual(await problemOf(await submit({}, null)), { status: 401, code: "unauthorized" });
  assert.deepEqual(await problemOf(await submit({}, "guest")), { status: 403, code: "forbidden" });
  assert.deepEqual(
    await problemOf(await submit(envelope({ actor: { kind: "owner", id: "someone-else" } }))),
    { status: 403, code: "forbidden" },
  );
  assert.equal((await store.readAll({ limit: 1000 })).length, before);
});

test("an owner command reads and decides again after a concurrent append, at most 3 times", async () => {
  const taskId = await seed();
  failingAppends = 2;
  const approved = await command("approve-plan", approve(taskId));
  assert.equal(approved.status, 201);
  failingAppends = 3;
  const pause = { ...approve(taskId), type: "orchestration.PauseRun", payload: { taskId } };
  assert.deepEqual(await problemOf(await command("pause-run", pause)), {
    status: 409,
    code: "conflict",
  });
  assert.equal(failingAppends, 0);
  assert.equal((await command("pause-run", pause)).status, 201);
});

test("EditPlan: a plan that the plan rules refuse gives 400 with the reason; agent-manager down gives 503", async () => {
  const taskId = await seed();
  const edit = (agent: string) => ({
    ...approve(taskId),
    type: "orchestration.EditPlan",
    payload: {
      taskId,
      planVersion: 1,
      subtasks: [
        {
          subtaskId: taskIds(),
          title: "A",
          instructions: "Do it.",
          agentId: agent,
          dependsOn: [],
          needsApproval: false,
        },
      ],
    },
  });
  const refused = await command("edit-plan", edit(unknownAgent));
  assert.equal(refused.status, 400);
  const problem = (await refused.json()) as { code: string; detail: string; errors: unknown[] };
  assert.equal(problem.code, "validation_failed");
  assert.equal(problem.detail, "Subtask 1 names an agent that has no active version.");
  assert.deepEqual(problem.errors, [
    {
      path: "body.payload",
      code: "custom",
      message: "Subtask 1 names an agent that has no active version.",
    },
  ]);
  assert.deepEqual(await problemOf(await command("edit-plan", edit(downAgent))), {
    status: 503,
    code: "unavailable",
  });
  assert.equal((await store.readStream(`task-${taskId}`)).length, 2);
});
