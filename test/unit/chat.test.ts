import assert from "node:assert/strict";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import type { RegisteredEvent } from "@famashines/ai-systems-contracts";
import type { HandlerContext } from "@famashines/ai-systems-kit/messaging";
import { createMemoryEventStore, fakeIds } from "@famashines/ai-systems-kit/testing";
import { agentByIdLoader, agentByNameLoader, ReadFailedError } from "../../src/adapters/writer.ts";
import { onTimer, type TaskDeps } from "../../src/application/run.ts";
import { onLlmEvent, onOwnerMessage } from "../../src/application/chat.ts";
import { AgentUnavailableError, type Writer } from "../../src/domain/task.ts";

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

const orchestrator: Writer = {
  agent: {
    ...writer.agent,
    agentId: "00000000-0000-4000-8000-00000000a001",
    name: "orchestrator",
  },
  prompt: { ...writer.prompt, name: "orchestrator", text: "You are the orchestrator." },
};

const byName = (name: string) =>
  Promise.resolve(
    name === "orchestrator"
      ? orchestrator
      : name === "writer"
        ? writer
        : { code: "not_found" as const, reason: "No agent." },
  );
const byId = (agentId: string) =>
  Promise.resolve([orchestrator, writer].find((item) => item.agent.agentId === agentId) ?? writer);

const setup = (loadAgentByName: TaskDeps["loadAgentByName"] = byName) => {
  const store = createMemoryEventStore({ newId: fakeIds(1) });
  const deps: TaskDeps = {
    store,
    newId: fakeIds(2),
    loadAgentByName,
    loadAgent: byId,
    now: () => new Date(),
    logger,
    settings: {
      maxParallel: 3,
      maxCallsPerTask: 50,
      retryDelaysMs: [5000, 30000],
      checkpointTimeoutMs: 60_000,
      planAgents: ["writer"],
    },
  };
  return { store, deps };
};

const typesOf = async (store: TaskDeps["store"], taskId: string) =>
  (await store.readStream(`task-${taskId}`)).map((item) => item.type.replace("orchestration.", ""));

const lastCall = async (store: TaskDeps["store"], taskId: string) => {
  const stream = await store.readStream(`task-${taskId}`);
  const call = stream.findLast((item) => item.type === "orchestration.CallRequested");
  assert.equal(call?.type, "orchestration.CallRequested");
  return call;
};

const llmEvent = (
  type: "llm.CallCompleted" | "llm.CallFailed" | "llm.CallStarted",
  taskId: string,
  callId: string,
  extra: Record<string, unknown>,
) =>
  ({
    eventId: ids(),
    type,
    schemaVersion: 1,
    streamId: `call-${callId}`,
    version: 1,
    occurredAt: "2026-11-01T10:00:01.000Z",
    correlationId: taskId,
    causationId: callId,
    actor: { kind: "system", id: "ai-proxy" },
    payload: { taskId, callId, attempt: 1, model: "google/gemini-2.5-flash-lite", ...extra },
  }) as RegisteredEvent;

const answer = (taskId: string, callId: string, text: string) =>
  llmEvent("llm.CallCompleted", taskId, callId, {
    text,
    finishReason: "stop",
    inputTokens: 5,
    outputTokens: 2,
    cachedTokens: 0,
    costMicroUsd: 7,
    latencyMs: 9,
    bodyId: ids(),
  });

const newTask = async (deps: TaskDeps, store: TaskDeps["store"], extra = {}) => {
  const message = posted({ intent: "new_task", ...extra });
  const { value, marked } = context();
  await onOwnerMessage(deps, message, value);
  const [first] = await store.readAll({ limit: 1 });
  assert.equal(first?.type, "orchestration.TaskSubmitted");
  return { message, marked, taskId: first.payload.taskId };
};

const reply = async (deps: TaskDeps, store: TaskDeps["store"], taskId: string, text: string) => {
  const call = await lastCall(store, taskId);
  await onLlmEvent(deps, answer(taskId, call.payload.callId, text), context().value);
  return call;
};

const onePlan = JSON.stringify({
  subtasks: [{ title: "Poem", instructions: "Write the poem.", agent: "writer" }],
});

test("a new_task message gives TaskSubmitted and a plan call of the orchestrator, and marks it", async () => {
  const { store, deps } = setup();
  const { message, marked, taskId } = await newTask(deps, store);
  const stream = await store.readStream(`task-${taskId}`);
  assert.deepEqual(
    stream.map((item) => [item.type, item.causationId, item.actor.kind]),
    [
      ["orchestration.TaskSubmitted", message.eventId, "owner"],
      ["orchestration.CallRequested", message.eventId, "system"],
    ],
  );
  const [submitted, call] = stream;
  assert.equal(
    submitted?.type === "orchestration.TaskSubmitted" && submitted.payload.mode,
    "approval",
  );
  assert.equal(call?.type === "orchestration.CallRequested" && call.payload.kind, "plan");
  assert.equal(
    call?.type === "orchestration.CallRequested" && call.payload.agentId,
    orchestrator.agent.agentId,
  );
  assert.equal(submitted?.correlationId, taskId);
  assert.equal(marked.length, 1);
});

test("a new_task message with a model and an effort gives them to TaskSubmitted and the plan call", async () => {
  const { store, deps } = setup();
  const model = "anthropic/claude-opus-4.8";
  const { taskId } = await newTask(deps, store, { model, effort: "high" });
  const [submitted, call] = await store.readStream(`task-${taskId}`);
  assert.equal(submitted?.type, "orchestration.TaskSubmitted");
  assert.equal(submitted.payload.model, model);
  assert.equal(submitted.payload.effort, "high");
  assert.equal(call?.type, "orchestration.CallRequested");
  assert.deepEqual(call.payload.models, [model]);
  assert.equal(call.payload.effort, "high");
});

test("approval mode: a valid plan gives PlanProposed that awaits approval, with CallSettled", async () => {
  const { store, deps } = setup();
  const { taskId } = await newTask(deps, store);
  await reply(deps, store, taskId, onePlan);
  assert.deepEqual(await typesOf(store, taskId), [
    "TaskSubmitted",
    "CallRequested",
    "CallSettled",
    "PlanProposed",
  ]);
  const proposed = (await store.readStream(`task-${taskId}`))[3];
  assert.equal(proposed?.type, "orchestration.PlanProposed");
  assert.equal(proposed.payload.awaitsApproval, true);
  assert.equal(proposed.payload.subtasks[0]?.agentId, writer.agent.agentId);
  assert.equal(proposed.payload.subtasks[0].estimatedCostMicroUsd, 5000);
});

test("auto mode: the run plans, runs the subtask, aggregates, checks and completes with the answer", async () => {
  const { store, deps } = setup();
  const { taskId } = await newTask(deps, store, { mode: "auto" });
  await reply(deps, store, taskId, onePlan);
  const subtask = await lastCall(store, taskId);
  assert.equal(subtask.payload.kind, "subtask");
  await reply(deps, store, taskId, "The sea is wide.");
  const aggregate = await lastCall(store, taskId);
  assert.equal(aggregate.payload.kind, "aggregate");
  assert.equal(aggregate.payload.agentId, orchestrator.agent.agentId);
  await reply(deps, store, taskId, "The answer (step 1): The sea is wide.");
  const check = await lastCall(store, taskId);
  assert.equal(check.payload.kind, "check");
  await reply(deps, store, taskId, '{"pass": true}');
  assert.deepEqual(await typesOf(store, taskId), [
    "TaskSubmitted",
    "CallRequested",
    "CallSettled",
    "PlanProposed",
    "PlanApproved",
    "RunStarted",
    "SubtaskQueued",
    "SubtaskStarted",
    "CallRequested",
    "CallSettled",
    "SubtaskCompleted",
    "CallRequested",
    "CallSettled",
    "CallRequested",
    "CallSettled",
    "RunCompleted",
  ]);
  const end = (await store.readStream(`task-${taskId}`)).at(-1);
  assert.equal(end?.type, "orchestration.RunCompleted");
  assert.equal(end.payload.summary, "The answer (step 1): The sea is wide.");
  assert.equal(end.payload.costMicroUsd, 7);
});

test("an agent-manager outage when the plan answer comes rejects the event and appends nothing", async () => {
  const outage = (name: string) =>
    name === "writer"
      ? Promise.resolve({ code: "unavailable" as const, reason: "No answer." })
      : byName(name);
  const { store, deps } = setup(outage);
  const { taskId } = await newTask(deps, store);
  const call = await lastCall(store, taskId);
  const { value, marked } = context();
  await assert.rejects(
    onLlmEvent(deps, answer(taskId, call.payload.callId, onePlan), value),
    AgentUnavailableError,
  );
  assert.equal(marked.length, 0);
  assert.deepEqual(await typesOf(store, taskId), ["TaskSubmitted", "CallRequested"]);
});

test("clarify: questions give ClarificationRequested, and the answer gives a new plan call", async () => {
  const { store, deps } = setup();
  const { taskId } = await newTask(deps, store);
  await reply(deps, store, taskId, '{"questions": ["About what?"]}');
  assert.equal((await typesOf(store, taskId)).at(-1), "ClarificationRequested");
  await onOwnerMessage(
    deps,
    posted({ intent: "answer", taskId, text: "About the sea." }),
    context().value,
  );
  assert.deepEqual((await typesOf(store, taskId)).slice(-2), [
    "ClarificationAnswered",
    "CallRequested",
  ]);
  const call = await lastCall(store, taskId);
  assert.equal(call.payload.kind, "plan");
  const contents = call.payload.messages.map((item) => item.content).join("\n");
  assert.match(contents, /BEGIN DATA clarifying_questions \S+\nAbout what\?\n/);
  assert.match(contents, /BEGIN DATA owner_message \S+\nAbout the sea\.\n/);
  assert.match(contents, /Do not ask questions\./);
});

test("a steer before the run is refused on the task stream", async () => {
  const { store, deps } = setup();
  const { taskId } = await newTask(deps, store);
  await onOwnerMessage(deps, posted({ intent: "steer", taskId }), context().value);
  const stream = await store.readStream(`task-${taskId}`);
  assert.equal(stream.at(-1)?.type, "orchestration.MessageRefused");
  assert.equal(stream.at(-1)?.actor.kind, "system");
});

test("a late call event for a closed task is dropped with a warn log; a CallStarted gives nothing", async () => {
  const { store, deps } = setup();
  const { taskId } = await newTask(deps, store);
  const call = await lastCall(store, taskId);
  const { value, marked } = context();
  await onLlmEvent(deps, llmEvent("llm.CallStarted", taskId, call.payload.callId, {}), value);
  assert.equal(marked.length, 0);
  const failedCall = llmEvent("llm.CallFailed", taskId, call.payload.callId, {
    errorClass: "budget_refused",
    reason: "No monthly cap is set",
    costMicroUsd: 0,
  });
  await onLlmEvent(deps, failedCall, value);
  assert.deepEqual((await typesOf(store, taskId)).slice(-2), ["CallSettled", "RunFailed"]);
  warnings.length = 0;
  await onLlmEvent(deps, failedCall, value);
  assert.equal((await store.readStream(`task-${taskId}`)).length, 4);
  assert.deepEqual(warnings, [
    {
      details: { taskId, callId: call.payload.callId, costMicroUsd: 0, type: "llm.CallFailed" },
      message: "a late call event for a closed task is dropped",
    },
  ]);
});

test("a dependency outage after the retries gives TaskSubmitted and RunFailed unavailable", async (t) => {
  const server = await serve({ [orchestratorPath]: [503, {}] });
  t.after(server.close);
  const { store, deps } = setup(
    agentByNameLoader({
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
    ["orchestration.TaskSubmitted", "orchestration.RunFailed"],
  );
  const failed = events[1];
  assert.equal(failed?.type, "orchestration.RunFailed");
  assert.equal(failed.payload.code, "unavailable");
  assert.match(failed.payload.reason, /orchestrator agent could not start\. Try again/);
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
const orchestratorPath = "/internal/agents/by-name/orchestrator/active-version";
const promptPath = `/internal/prompts/${promptId}/versions/1`;

test("agentByNameLoader reads the active version by name and its prompt version", async (t) => {
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
    const load = agentByNameLoader({
      agentManagerUrl: server.url,
      promptStudioUrl: server.url,
      logger,
    });
    assert.deepEqual(await load("writer"), expected);
  }
});

test("agentByNameLoader tries a 5xx or a network error 3 times, then gives unavailable with the cause logged", async (t) => {
  const server = await serve({ [agentPath]: [200, writer.agent], [promptPath]: [502, {}] });
  t.after(server.close);
  const retryDelaysMs = [1, 1];
  const unavailable = {
    code: "unavailable",
    reason:
      "agent-manager or prompt-studio did not answer, so the writer agent could not start. Try again in a minute.",
  };
  assert.deepEqual(
    await agentByNameLoader({
      agentManagerUrl: server.url,
      promptStudioUrl: server.url,
      logger,
      retryDelaysMs,
    })("writer"),
    unavailable,
  );
  assert.equal(server.requests(), 4);
  warnings.length = 0;
  assert.deepEqual(
    await agentByNameLoader({
      agentManagerUrl: "http://127.0.0.1:1",
      promptStudioUrl: server.url,
      logger,
      timeoutMs: 500,
      retryDelaysMs,
    })("writer"),
    unavailable,
  );
  const [warning] = warnings;
  assert.equal(warning?.message, "the writer read failed");
  const { err, attempts } = warning.details as { err: Error; attempts: number };
  assert.equal(attempts, 3);
  assert.ok(err instanceof ReadFailedError);
  assert.ok(err.cause instanceof Error);
});

test("agentByNameLoader gives internal with no retry for a 4xx that is not 404", async (t) => {
  const cases: [Record<string, [number, unknown]>, number, unknown][] = [
    [
      { [agentPath]: [403, {}] },
      1,
      { code: "internal", reason: "agent-manager refused the read of the writer agent." },
    ],
    [
      { [agentPath]: [401, {}] },
      1,
      { code: "internal", reason: "agent-manager refused the read of the writer agent." },
    ],
    [
      { [agentPath]: [200, writer.agent], [promptPath]: [400, {}] },
      2,
      { code: "internal", reason: "prompt-studio refused the read of the writer prompt." },
    ],
    [
      { [agentPath]: [200, writer.agent], [promptPath]: [429, {}] },
      2,
      { code: "internal", reason: "prompt-studio refused the read of the writer prompt." },
    ],
  ];
  for (const [routes, requests, expected] of cases) {
    const server = await serve(routes);
    t.after(server.close);
    warnings.length = 0;
    const load = agentByNameLoader({
      agentManagerUrl: server.url,
      promptStudioUrl: server.url,
      logger,
      retryDelaysMs: [1, 1],
    });
    assert.deepEqual(await load("writer"), expected);
    assert.equal(server.requests(), requests);
    const { err, attempts } = warnings[0]?.details as { err: ReadFailedError; attempts: number };
    assert.equal(attempts, 1);
    assert.equal(err.status, Object.values(routes).at(-1)?.[0]);
  }
});

test("agentByIdLoader reads the active version of an agent by id, and refuses another agent", async (t) => {
  const { agentId } = writer.agent;
  const byId = `/internal/agents/${agentId}/active-version`;
  const server = await serve({ [byId]: [200, writer.agent], [promptPath]: [200, writer.prompt] });
  t.after(server.close);
  const load = agentByIdLoader({
    agentManagerUrl: server.url,
    promptStudioUrl: server.url,
    logger,
  });
  assert.deepEqual(await load(agentId), writer);
  const other = "00000000-0000-4000-8000-00000000a009";
  assert.deepEqual(await load(other), {
    code: "not_found",
    reason: `The subtask agent ${other} has no active version.`,
  });
  const wrong = await serve({ [`/internal/agents/${other}/active-version`]: [200, writer.agent] });
  t.after(wrong.close);
  const loadWrong = agentByIdLoader({
    agentManagerUrl: wrong.url,
    promptStudioUrl: wrong.url,
    logger,
  });
  assert.deepEqual(await loadWrong(other), {
    code: "internal",
    reason: `agent-manager gave a subtask agent ${other} that is not valid.`,
  });
});

test("onTimer starts a due retry in one append with the marker, and gives false when the timer has nothing to do", async () => {
  const { store, deps } = setup();
  const taskId = "00000000-0000-4000-8000-0000000000aa";
  const runId = "00000000-0000-4000-8000-0000000000ab";
  const subtaskId = "00000000-0000-4000-8000-0000000000ac";
  const ids = { taskId, runId, subtaskId };
  const subtask = {
    subtaskId,
    title: "Step",
    instructions: "Do it.",
    agentId: writer.agent.agentId,
    dependsOn: [],
    needsApproval: false,
  };
  const now = deps.now();
  const history = [
    ["orchestration.TaskSubmitted", { taskId, text: "Report", mode: "auto" }],
    [
      "orchestration.PlanProposed",
      { taskId, planVersion: 1, subtasks: [subtask], awaitsApproval: false },
    ],
    ["orchestration.PlanApproved", { taskId, planVersion: 1 }],
    ["orchestration.RunStarted", { taskId, runId }],
    ["orchestration.SubtaskQueued", ids],
    [
      "orchestration.SubtaskFailed",
      {
        ...ids,
        tryNumber: 1,
        code: "unavailable",
        reason: "x",
        inputTokens: 0,
        outputTokens: 0,
        costMicroUsd: 0,
      },
    ],
    ["orchestration.SubtaskRetrying", { ...ids, tryNumber: 2, retryAt: now.toISOString() }],
  ] as const;
  await store.append({
    streamId: `task-${taskId}`,
    events: history.map(([type, payload]) => ({
      type,
      schemaVersion: 1,
      payload,
      correlationId: taskId,
      causationId: taskId,
      actor: owner,
    })) as never,
  });
  const timer = { _id: ids.runId, type: "orchestration.SubtaskRetrying", payload: ids };
  let marked = 0;
  const also = () => {
    marked += 1;
    return Promise.resolve();
  };
  assert.equal(await onTimer(deps, timer, also), true);
  assert.equal(marked, 1);
  const stream = await store.readStream(`task-${taskId}`);
  assert.deepEqual(
    stream.slice(-2).map((item) => [item.type, item.causationId]),
    [
      ["orchestration.SubtaskStarted", ids.runId],
      ["orchestration.CallRequested", ids.runId],
    ],
  );
  const due = {
    ...timer,
    type: "orchestration.CheckpointOpened",
    payload: { ...ids, checkpointId: runId },
  };
  assert.equal(await onTimer(deps, due, also), false);
  assert.equal(marked, 1);
});
