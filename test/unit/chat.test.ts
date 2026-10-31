import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import type { RegisteredEvent } from "@famashines/ai-systems-contracts";
import type { HandlerContext } from "@famashines/ai-systems-kit/messaging";
import { createMemoryEventStore, fakeIds } from "@famashines/ai-systems-kit/testing";
import { ReadFailedError, writerLoader } from "../../src/adapters/writer.ts";
import { onLlmEvent, onOwnerMessage, type ChatDeps } from "../../src/application/chat.ts";
import type { Writer } from "../../src/domain/task.ts";

const promptId = "cc74090a-534e-4072-8bfd-e6c5b9f22f41";
const writer: Writer = {
  agent: {
    agentId: "00000000-0000-4000-8000-00000000a002",
    name: "writer",
    enabled: true,
    version: 1,
    prompt: { promptId, version: 1 },
    models: ["google/gemini-2.5-flash-lite"],
    limits: { maxCalls: 10, maxTokens: 4096, maxCostMicroUsd: 50_000 },
  },
  prompt: { promptId, name: "writer", version: 1, text: "You are the writer.", variables: [] },
};

const ids = fakeIds(9);
const owner = { kind: "owner", id: "owner-1" } as const;
const conversationId = "00000000-0000-4000-8000-0000000000c1";

const posted = (payload: Record<string, unknown>): RegisteredEvent =>
  ({
    eventId: ids(),
    type: "conversation.OwnerMessagePosted",
    schemaVersion: 1,
    streamId: `conversation-${conversationId}`,
    version: 2,
    occurredAt: "2026-11-01T10:00:00.000Z",
    correlationId: conversationId,
    causationId: conversationId,
    actor: owner,
    payload: { conversationId, messageId: ids(), text: "Write a poem", ...payload },
  }) as RegisteredEvent;

const context = () => {
  const marked: number[] = [];
  const value = {
    session: {},
    markProcessed: () => {
      marked.push(1);
      return Promise.resolve();
    },
  } as unknown as HandlerContext;
  return { value, marked };
};

const warnings: { details: object; message: string }[] = [];
const logger = {
  warn: (details: object, message: string) => {
    warnings.push({ details, message });
  },
};

const setup = (loadWriter: ChatDeps["loadWriter"] = () => Promise.resolve(writer)) => {
  const store = createMemoryEventStore({ newId: fakeIds(1) });
  const deps: ChatDeps = { store, newId: fakeIds(2), loadWriter, now: () => new Date(), logger };
  return { store, deps };
};

test("a new_task message gives one task stream with the source as cause, and marks it", async () => {
  const { store, deps } = setup();
  const message = posted({ intent: "new_task" });
  const { value, marked } = context();
  await onOwnerMessage(deps, message, value);
  const [event] = await store.readAll({ limit: 10 });
  assert.equal(event?.type, "orchestration.TaskSubmitted");
  const stream = await store.readStream(event.streamId);
  assert.deepEqual(
    stream.map((item) => [item.type, item.causationId, item.actor.kind]),
    [
      ["orchestration.TaskSubmitted", message.eventId, "owner"],
      ["orchestration.RunStarted", message.eventId, "system"],
      ["orchestration.CallRequested", message.eventId, "system"],
    ],
  );
  assert.equal(event.correlationId, event.payload.taskId);
  assert.equal(marked.length, 1);
});

test("a CallCompleted of the task ends the run; a CallStarted gives nothing", async () => {
  const { store, deps } = setup();
  await onOwnerMessage(deps, posted({ intent: "new_task" }), context().value);
  const [, , requested] = await store.readAll({ limit: 10 });
  assert.equal(requested?.type, "orchestration.CallRequested");
  const { taskId, callId } = requested.payload;
  const base = {
    eventId: ids(),
    schemaVersion: 1,
    streamId: `call-${callId}`,
    occurredAt: "2026-11-01T10:00:01.000Z",
    correlationId: taskId,
    causationId: requested.eventId,
    actor: { kind: "system", id: "ai-proxy" },
  };
  const attempt = { taskId, callId, attempt: 1, model: "google/gemini-2.5-flash-lite" };
  const startedCall = {
    ...base,
    version: 1,
    type: "llm.CallStarted",
    payload: { ...attempt, agentId: ids(), agentVersion: 1, reservationId: ids() },
  } as RegisteredEvent;
  const done = {
    ...base,
    eventId: ids(),
    version: 2,
    type: "llm.CallCompleted",
    payload: {
      ...attempt,
      text: "A poem.",
      finishReason: "stop",
      inputTokens: 5,
      outputTokens: 2,
      cachedTokens: 0,
      costMicroUsd: 7,
      latencyMs: 9,
      bodyId: ids(),
    },
  } as RegisteredEvent;
  const { value, marked } = context();
  await onLlmEvent(deps, startedCall, value);
  assert.equal(marked.length, 0);
  await onLlmEvent(deps, done, value);
  await onLlmEvent(deps, done, value);
  const stream = await store.readStream(`task-${taskId}`);
  assert.deepEqual(
    stream.map((item) => item.type),
    [
      "orchestration.TaskSubmitted",
      "orchestration.RunStarted",
      "orchestration.CallRequested",
      "orchestration.RunCompleted",
    ],
  );
  assert.equal(marked.length, 1);
  const failedCall = {
    ...done,
    type: "llm.CallFailed",
    payload: { ...attempt, errorClass: "provider", reason: "HTTP 500", costMicroUsd: 0 },
  } as RegisteredEvent;
  warnings.length = 0;
  await onLlmEvent(deps, failedCall, value);
  assert.equal((await store.readStream(`task-${taskId}`)).length, 4);
  assert.deepEqual(warnings, [
    {
      details: { taskId, callId, costMicroUsd: 0, type: "llm.CallFailed" },
      message: "a late call event for a closed task is dropped",
    },
  ]);
});

test("a steer for a running task gives MessageRefused on the task stream", async () => {
  const { store, deps } = setup();
  await onOwnerMessage(deps, posted({ intent: "new_task" }), context().value);
  const [submitted] = await store.readAll({ limit: 1 });
  const taskId = submitted?.type === "orchestration.TaskSubmitted" ? submitted.payload.taskId : "";
  await onOwnerMessage(deps, posted({ intent: "steer", taskId }), context().value);
  const stream = await store.readStream(`task-${taskId}`);
  assert.equal(stream.at(-1)?.type, "orchestration.MessageRefused");
  assert.equal(stream.at(-1)?.actor.kind, "system");
});

test("a dependency outage after the retries gives TaskSubmitted and RunFailed unavailable", async (t) => {
  const server = await serve({ [agentPath]: [503, {}] });
  t.after(server.close);
  const { store, deps } = setup(
    writerLoader({
      agentManagerUrl: server.url,
      promptStudioUrl: server.url,
      logger,
      retryDelaysMs: [1, 1],
    }),
  );
  await onOwnerMessage(deps, posted({ intent: "new_task" }), context().value);
  assert.equal(server.requests(), 3);
  const events = await store.readAll();
  assert.deepEqual(
    events.map((event) => event.type),
    ["orchestration.TaskSubmitted", "orchestration.RunStarted", "orchestration.RunFailed"],
  );
  const failed = events[2];
  assert.equal(failed?.type, "orchestration.RunFailed");
  assert.equal(failed.payload.code, "unavailable");
  assert.match(failed.payload.reason, /Try again/);
});

test("other event types give nothing", async () => {
  const { store, deps } = setup();
  const other = { ...posted({ intent: "new_task" }), type: "conversation.ConversationStarted" };
  await onOwnerMessage(deps, other as RegisteredEvent, context().value);
  assert.equal((await store.readAll()).length, 0);
});

const serve = async (routes: Record<string, [number, unknown]>) => {
  let requests = 0;
  const server = createServer((request, response) => {
    requests += 1;
    const [status, body] = routes[request.url ?? ""] ?? [404, {}];
    response.writeHead(status, { "content-type": "application/json" });
    response.end(typeof body === "string" ? body : JSON.stringify(body));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const url = `http://127.0.0.1:${String((server.address() as AddressInfo).port)}`;
  return {
    url,
    requests: () => requests,
    close: () => new Promise((resolve) => server.close(resolve)),
  };
};

const agentPath = "/internal/agents/by-name/writer/active-version";
const promptPath = `/internal/prompts/${promptId}/versions/1`;

test("writerLoader reads the active writer version and its prompt version", async (t) => {
  const cases: [Record<string, [number, unknown]>, unknown][] = [
    [{ [agentPath]: [200, writer.agent], [promptPath]: [200, writer.prompt] }, writer],
    [{}, { code: "not_found", reason: "The writer agent has no active version." }],
    [
      { [agentPath]: [200, { name: "writer" }] },
      { code: "internal", reason: "agent-manager gave a writer agent that is not valid." },
    ],
    [
      { [agentPath]: [200, writer.agent] },
      { code: "not_found", reason: "The prompt version of the writer agent is not there." },
    ],
    [
      { [agentPath]: [200, writer.agent], [promptPath]: [200, { ...writer.prompt, version: 2 }] },
      { code: "internal", reason: "prompt-studio gave a writer prompt that is not valid." },
    ],
    [
      { [agentPath]: [200, writer.agent], [promptPath]: [200, "not json"] },
      { code: "internal", reason: "prompt-studio gave a writer prompt that is not valid." },
    ],
  ];
  for (const [routes, expected] of cases) {
    const server = await serve(routes);
    t.after(server.close);
    const load = writerLoader({ agentManagerUrl: server.url, promptStudioUrl: server.url, logger });
    assert.deepEqual(await load(), expected);
  }
});

test("writerLoader tries a 5xx or a network error 3 times, then gives unavailable with the cause logged", async (t) => {
  const server = await serve({ [agentPath]: [200, writer.agent], [promptPath]: [502, {}] });
  t.after(server.close);
  const retryDelaysMs = [1, 1];
  const unavailable = {
    code: "unavailable",
    reason:
      "agent-manager or prompt-studio did not answer, so the writer agent could not start. Try again in a minute.",
  };
  assert.deepEqual(
    await writerLoader({
      agentManagerUrl: server.url,
      promptStudioUrl: server.url,
      logger,
      retryDelaysMs,
    })(),
    unavailable,
  );
  assert.equal(server.requests(), 4);
  warnings.length = 0;
  assert.deepEqual(
    await writerLoader({
      agentManagerUrl: "http://127.0.0.1:1",
      promptStudioUrl: server.url,
      logger,
      timeoutMs: 500,
      retryDelaysMs,
    })(),
    unavailable,
  );
  const [warning] = warnings;
  assert.equal(warning?.message, "the writer read failed");
  const { err, attempts } = warning.details as { err: Error; attempts: number };
  assert.equal(attempts, 3);
  assert.ok(err instanceof ReadFailedError);
  assert.ok(err.cause instanceof Error);
});
