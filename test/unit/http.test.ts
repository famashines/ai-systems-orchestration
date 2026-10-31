import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { createService } from "@famashines/ai-systems-kit/http";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import { createMemoryEventStore, fakeClock, fakeIds } from "@famashines/ai-systems-kit/testing";
import { routes } from "../../src/adapters/http.ts";

const clock = fakeClock();
const store = createMemoryEventStore({ now: clock.now, newId: fakeIds(1) });
const taskIds = fakeIds(2);
let nextTaskId = taskIds;
const app = createService({
  logger: createLogger({ service: "orchestration", release: "test", level: "silent" }),
  routes: routes({ store, newId: () => nextTaskId() }),
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

const submit = (body: unknown) =>
  fetch(`${base}/internal/commands/submit-task`, {
    method: "POST",
    headers: { "content-type": "application/json" },
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
  assert.equal(event?.payload.mode, "approval");
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
