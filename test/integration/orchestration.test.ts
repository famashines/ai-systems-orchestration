import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import { jetstream, jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";
import { loadConfig } from "@famashines/ai-systems-kit/config";
import { createEventStore } from "@famashines/ai-systems-kit/event-store";
import { connectNats, type Nats } from "@famashines/ai-systems-kit/messaging";
import { connectMongo, type Mongo } from "@famashines/ai-systems-kit/mongo";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import {
  createStreams,
  startMongo,
  startNats,
  waitFor,
  type Started,
} from "@famashines/ai-systems-kit/testing";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import { envSchema, start } from "../../src/main.ts";
import { natsConfig, password, streams } from "../nats.ts";

const promptId = "cc74090a-534e-4072-8bfd-e6c5b9f22f41";
const writerAgent = {
  agentId: "00000000-0000-4000-8000-00000000a002",
  name: "writer",
  enabled: true,
  version: 1,
  prompt: { promptId, version: 1 },
  models: ["google/gemini-2.5-flash-lite", "openai/gpt-4.1-nano"],
  limits: { maxCalls: 10, maxTokens: 4096, maxCostMicroUsd: 50_000 },
};
const writerPrompt = {
  promptId,
  name: "writer",
  version: 1,
  text: "You are the writer agent.",
  variables: [],
};
const plannerPromptId = "a0d53ff1-1eda-4636-bb2f-880df141bcb9";
const orchestratorAgent = {
  ...writerAgent,
  agentId: "00000000-0000-4000-8000-00000000a001",
  name: "orchestrator",
  prompt: { promptId: plannerPromptId, version: 1 },
};
const orchestratorPrompt = {
  ...writerPrompt,
  promptId: plannerPromptId,
  name: "orchestrator",
  text: "You are the orchestrator.",
};
let agentAnswer: [number, unknown] = [200, orchestratorAgent];
const issuer = "https://auth.example.test/realms/ai-systems";
const keys = await generateKeyPair("RS256", { extractable: true });
const jwk = { ...(await exportJWK(keys.publicKey)), kid: "k1", alg: "RS256", use: "sig" };
const ownerToken = () =>
  new SignJWT({ typ: "Bearer", azp: "ai-systems-app", realm_access: { roles: ["owner"] } })
    .setProtectedHeader({ alg: "RS256", kid: "k1" })
    .setIssuer(issuer)
    .setAudience("ai-systems-app")
    .setSubject("owner-1")
    .setIssuedAt()
    .setExpirationTime("5m")
    .sign(keys.privateKey);
const fakes = await (async () => {
  const routes: Record<string, () => [number, unknown]> = {
    "/certs": () => [200, { keys: [jwk] }],
    "/internal/agents/by-name/orchestrator/active-version": () => agentAnswer,
    "/internal/agents/by-name/writer/active-version": () => [200, writerAgent],
    [`/internal/agents/${writerAgent.agentId}/active-version`]: () => [200, writerAgent],
    [`/internal/agents/${orchestratorAgent.agentId}/active-version`]: () => [
      200,
      orchestratorAgent,
    ],
    [`/internal/prompts/${promptId}/versions/1`]: () => [200, writerPrompt],
    [`/internal/prompts/${plannerPromptId}/versions/1`]: () => [200, orchestratorPrompt],
  };
  const server = createServer((request, response) => {
    const [status, body] = routes[request.url ?? ""]?.() ?? [404, {}];
    response.writeHead(status, { "content-type": "application/json" });
    response.end(JSON.stringify(body));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    url: `http://127.0.0.1:${String((server.address() as AddressInfo).port)}`,
    close: () => new Promise((resolve) => server.close(resolve)),
  };
})();

const logger = createLogger({ service: "test", release: "test", level: "silent" });
let mongoContainer: Started;
let natsContainer: Started;
let mongo: Mongo;
let admin: Nats;
let manager: JetStreamManager;
let running: Awaited<ReturnType<typeof start>> | undefined;
let base = "";

const startService = async (): Promise<void> => {
  const config = loadConfig(envSchema, {
    MONGODB_URI: mongoContainer.url,
    NATS_SERVERS: natsContainer.url,
    NATS_USER: "ai-systems-orchestration",
    NATS_PASSWORD: password("orchestration"),
    AGENT_MANAGER_URL: fakes.url,
    PROMPT_STUDIO_URL: fakes.url,
    OIDC_ISSUER: issuer,
    OIDC_JWKS_URL: `${fakes.url}/certs`,
    LOG_LEVEL: "silent",
  });
  running = await start({ ...config, PORT: 0 }, { drainDelayMs: 0 });
  base = `http://127.0.0.1:${String(running.address.port)}`;
  await waitFor(async () => (await fetch(`${base}/ready`)).ok);
};

const stopService = async (): Promise<void> => {
  const stopping = running;
  running = undefined;
  if (stopping) assert.equal(await stopping.app.close(), true);
};

before(async () => {
  [mongoContainer, natsContainer] = await Promise.all([
    startMongo(),
    startNats({ config: natsConfig(["admin", "orchestration"]) }),
  ]);
  mongo = await connectMongo({ url: mongoContainer.url, database: "orchestration" });
  admin = await connectNats({
    servers: natsContainer.url,
    user: "ai-systems-admin",
    pass: password("admin"),
    service: "admin",
    logger,
  });
  manager = await jetstreamManager(admin.connection);
  await createStreams(admin.connection, streams);
  await startService();
});

after(async () => {
  await stopService();
  await admin.close();
  await mongo.close();
  await fakes.close();
  await Promise.all([mongoContainer.stop(), natsContainer.stop()]);
});

const events = () => mongo.db.collection<{ _id: string; publishedAt: Date | null }>("events");

const submit = async (commandId: string, text = "Write a poem") =>
  fetch(`${base}/internal/commands/submit-task`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${await ownerToken()}` },
    body: JSON.stringify({
      commandId,
      type: "orchestration.SubmitTask",
      actor: { kind: "owner", id: "owner-1" },
      payload: { text, mode: "auto" },
    }),
  });

const streamMessages = async (eventId: string) => {
  const { state } = await manager.streams.info("ORCHESTRATION");
  const found = [];
  for (let seq = state.first_seq; seq <= state.last_seq && state.messages > 0; seq += 1) {
    const message = await manager.streams.getMessage("ORCHESTRATION", { seq });
    if (message?.header.get("Nats-Msg-Id") === eventId) found.push(message);
  }
  return found;
};

const published = async (eventId: string): Promise<void> => {
  await waitFor(async () => (await events().findOne({ _id: eventId }))?.publishedAt != null);
};

test("the service is ready when MongoDB answers and the migrations are done", async () => {
  const response = await fetch(`${base}/ready`);
  assert.equal(response.status, 200);
  await mongo.db.collection<{ _id: string }>("migrations").deleteOne({ _id: "000-event-store" });
  const missing = await fetch(`${base}/ready`);
  assert.equal(missing.status, 503);
  assert.deepEqual(((await missing.json()) as { failing: string[] }).failing, ["migrations"]);
  await mongo.db
    .collection<{ _id: string; appliedAt: Date }>("migrations")
    .insertOne({ _id: "000-event-store", appliedAt: new Date() });
  assert.equal((await fetch(`${base}/ready`)).status, 200);
});

test("a submitted task is in MongoDB once and in ORCHESTRATION once", async () => {
  const response = await submit(randomUUID());
  assert.equal(response.status, 201);
  const { taskId } = (await response.json()) as { taskId: string };
  const stored = await events()
    .find({ streamId: `task-${taskId}` })
    .toArray();
  assert.equal(stored.length, 1);
  const eventId = stored[0]?._id ?? "";
  await published(eventId);
  const messages = await streamMessages(eventId);
  assert.equal(messages.length, 1);
  const [message] = messages;
  assert.ok(message);
  assert.equal(message.subject, "orchestration.TaskSubmitted");
  const envelope = message.json<{ eventId: string; payload: { taskId: string } }>();
  assert.equal(envelope.eventId, eventId);
  assert.equal(envelope.payload.taskId, taskId);
  assert.equal(message.header.get("correlation-id"), taskId);
});

test("the same commandId twice gives one event and the same answer", async () => {
  const commandId = randomUUID();
  const first = await submit(commandId);
  const second = await submit(commandId, "Another text");
  assert.equal(first.status, 201);
  assert.equal(second.status, 200);
  const answer = (await first.json()) as { taskId: string };
  assert.deepEqual(await second.json(), answer);
  assert.equal(await events().countDocuments({ causationId: commandId }), 1);
  const [stored] = await events().find({ causationId: commandId }).toArray();
  await published(stored?._id ?? "");
  assert.equal((await streamMessages(stored?._id ?? "")).length, 1);
});

test("the replay API gives a page of events in position order", async () => {
  await submit(randomUUID());
  const all = (await (await fetch(`${base}/internal/events?limit=1000`)).json()) as {
    events: { position: number; eventId: string }[];
  };
  assert.ok(all.events.length >= 3);
  assert.deepEqual(
    all.events.map((event) => event.position),
    all.events.map((_, index) => index + 1),
  );
  const page = (await (await fetch(`${base}/internal/events?afterPosition=1&limit=2`)).json()) as {
    events: unknown[];
  };
  assert.deepEqual(page.events, all.events.slice(1, 3));
});

test("without the stream the service stays ready and the event waits in the outbox", async () => {
  await manager.streams.delete("ORCHESTRATION");
  const response = await submit(randomUUID());
  assert.equal(response.status, 201);
  const { taskId } = (await response.json()) as { taskId: string };
  assert.equal((await fetch(`${base}/ready`)).status, 200);
  const [stored] = await events()
    .find({ streamId: `task-${taskId}` })
    .toArray();
  const eventId = stored?._id ?? "";
  assert.equal(stored?.publishedAt, null);
  await createStreams(admin.connection, [{ name: "ORCHESTRATION", subjects: ["orchestration.>"] }]);
  await published(eventId);
  assert.equal((await streamMessages(eventId)).length, 1);
});

test("after a restart the service publishes the pending events", async () => {
  await stopService();
  const store = createEventStore(mongo.db);
  const taskId = randomUUID();
  const { events: appended } = await store.append({
    streamId: `task-${taskId}`,
    expectedVersion: 0,
    events: [
      {
        type: "orchestration.TaskSubmitted",
        schemaVersion: 1,
        correlationId: taskId,
        causationId: randomUUID(),
        actor: { kind: "owner", id: "owner-1" },
        payload: { taskId, text: "Written before the restart", mode: "auto" },
      },
    ],
  });
  const eventId = appended[0]?.eventId ?? "";
  assert.equal((await events().findOne({ _id: eventId }))?.publishedAt, null);
  await startService();
  await published(eventId);
  assert.equal((await streamMessages(eventId)).length, 1);
});

type Stored = {
  _id: string;
  type: string;
  streamId: string;
  causationId: string;
  payload: Record<string, unknown>;
};
const stored = () => mongo.db.collection<Stored>("events");
const conversationId = randomUUID();

const publish = async (
  subject: string,
  streamId: string,
  correlationId: string,
  payload: object,
  eventId: string = randomUUID(),
  msgID = eventId,
): Promise<string> => {
  const envelope = {
    eventId,
    type: subject,
    schemaVersion: 1,
    streamId,
    version: 1,
    occurredAt: new Date().toISOString(),
    correlationId,
    causationId: correlationId,
    actor: { kind: "owner", id: "owner-1" },
    payload,
  };
  await jetstream(admin.connection).publish(subject, JSON.stringify(envelope), { msgID });
  return eventId;
};

const post = (payload: object, eventId?: string, msgID?: string) =>
  publish(
    "conversation.OwnerMessagePosted",
    `conversation-${conversationId}`,
    conversationId,
    { conversationId, text: "Write a poem", ...payload },
    eventId,
    msgID,
  );

const taskOf = async (messageId: string): Promise<string> => {
  await waitFor(
    async () =>
      (await stored().countDocuments({ "payload.messageId": messageId, type: /TaskSubmitted$/ })) >
      0,
  );
  const task = await stored().findOne({ "payload.messageId": messageId });
  return String(task?.payload.taskId);
};

const streamOf = (taskId: string) =>
  stored()
    .find({ streamId: `task-${taskId}` }, { sort: { version: 1 } })
    .toArray();

const until = async (taskId: string, type: string) => {
  await waitFor(async () => (await streamOf(taskId)).some((event) => event.type === type));
  return streamOf(taskId);
};

const callOf = async (taskId: string) => {
  const events = await until(taskId, "orchestration.CallRequested");
  const call = events.find((event) => event.type === "orchestration.CallRequested");
  return { callId: String(call?.payload.callId), requestedId: String(call?._id) };
};

const llm = (
  type: string,
  taskId: string,
  callId: string,
  payload: object,
  eventId?: string,
  msgID?: string,
) =>
  publish(
    `llm.${type}`,
    `call-${callId}`,
    taskId,
    { taskId, callId, attempt: 1, model: "google/gemini-2.5-flash-lite", ...payload },
    eventId,
    msgID,
  );

const consumed = (stream: string) =>
  waitFor(async () => {
    const info = await manager.consumers.info(stream, "orchestration");
    return info.num_pending === 0 && info.num_ack_pending === 0;
  });

const completedCall = (text: string) => ({
  text,
  finishReason: "stop",
  inputTokens: 12,
  outputTokens: 3,
  cachedTokens: 0,
  costMicroUsd: 21,
  latencyMs: 150,
  bodyId: randomUUID(),
});

const lastCall = async (taskId: string, count: number) => {
  await waitFor(
    async () =>
      (await streamOf(taskId)).filter((event) => event.type === "orchestration.CallRequested")
        .length >= count,
  );
  const call = (await streamOf(taskId)).findLast(
    (event) => event.type === "orchestration.CallRequested",
  );
  assert.ok(call);
  return call;
};

const onePlan = JSON.stringify({
  subtasks: [{ title: "Poem", instructions: "Write the poem.", agent: "writer" }],
});

test("an auto new_task is planned, runs its subtask, aggregates, checks and completes", async () => {
  const messageId = randomUUID();
  const sourceId = await post({ messageId, intent: "new_task", mode: "auto" });
  const taskId = await taskOf(messageId);
  const events = await until(taskId, "orchestration.CallRequested");
  assert.deepEqual(
    events.map((event) => [event.type, event.causationId]),
    [
      ["orchestration.TaskSubmitted", sourceId],
      ["orchestration.CallRequested", sourceId],
    ],
  );
  assert.deepEqual(events[0]?.payload, {
    taskId,
    text: "Write a poem",
    mode: "auto",
    conversationId,
    messageId,
  });
  const planCall = events[1]?.payload ?? {};
  assert.equal(planCall.kind, "plan");
  assert.equal(planCall.agentId, orchestratorAgent.agentId);
  assert.equal(planCall.maxCostMicroUsd, 50_000);
  const [system, user] = planCall.messages as { content: string }[];
  assert.equal(system?.content, "You are the orchestrator.");
  const blockId = /BEGIN DATA owner_message (\S+)\n/.exec(user?.content ?? "")?.[1];
  assert.ok(blockId !== undefined && blockId !== messageId);
  assert.ok(
    user?.content.endsWith(
      `BEGIN DATA owner_message ${blockId}\nWrite a poem\nEND DATA owner_message ${blockId}`,
    ),
  );
  await published(String(events[1]?._id));
  assert.equal((await streamMessages(String(events[1]?._id))).length, 1);
  const answers = [onePlan, "The poem.", "The answer (step 1): the poem.", '{"pass": true}'];
  const kinds: unknown[] = [];
  for (const [index, text] of answers.entries()) {
    const call = await lastCall(taskId, index + 1);
    kinds.push(call.payload.kind);
    await llm("CallStarted", taskId, String(call.payload.callId), {
      agentId: call.payload.agentId,
      agentVersion: 1,
      reservationId: randomUUID(),
    });
    await llm("CallCompleted", taskId, String(call.payload.callId), completedCall(text));
  }
  assert.deepEqual(kinds, ["plan", "subtask", "aggregate", "check"]);
  const done = await until(taskId, "orchestration.RunCompleted");
  assert.equal(done.at(-1)?.payload.summary, "The answer (step 1): the poem.");
  assert.equal(done.at(-1)?.payload.costMicroUsd, 21);
  assert.equal(done.filter((event) => event.type === "orchestration.CallSettled").length, 4);
});

test("approval is the default mode, and questions wait for the answer of the owner", async () => {
  const messageId = randomUUID();
  await post({ messageId, intent: "new_task" });
  const taskId = await taskOf(messageId);
  const first = await lastCall(taskId, 1);
  await llm(
    "CallCompleted",
    taskId,
    String(first.payload.callId),
    completedCall('{"questions": ["About what?"]}'),
  );
  await until(taskId, "orchestration.ClarificationRequested");
  const answerId = randomUUID();
  await post({ messageId: answerId, intent: "answer", taskId, text: "The sea." });
  const second = await lastCall(taskId, 2);
  await llm("CallCompleted", taskId, String(second.payload.callId), completedCall(onePlan));
  const proposed = await until(taskId, "orchestration.PlanProposed");
  assert.deepEqual(
    proposed.map((event) => event.type.replace("orchestration.", "")),
    [
      "TaskSubmitted",
      "CallRequested",
      "CallSettled",
      "ClarificationRequested",
      "ClarificationAnswered",
      "CallRequested",
      "CallSettled",
      "PlanProposed",
    ],
  );
  assert.equal(proposed.at(-1)?.payload.awaitsApproval, true);
  assert.equal(proposed[4]?.payload.messageId, answerId);
});

test("a budget refusal of the plan call fails the task, and a later steer is refused", async () => {
  const messageId = randomUUID();
  await post({ messageId, intent: "new_task" });
  const taskId = await taskOf(messageId);
  const { callId } = await callOf(taskId);
  await llm("CallFailed", taskId, callId, {
    errorClass: "budget_refused",
    reason: "No monthly cap is set",
    costMicroUsd: 0,
    reservationId: randomUUID(),
  });
  const failed = await until(taskId, "orchestration.RunFailed");
  assert.equal(failed.at(-1)?.payload.code, "budget_refused");
  assert.equal(
    failed.at(-1)?.payload.reason,
    "The budget refused the plan call: No monthly cap is set",
  );
  const steerId = randomUUID();
  await post({ messageId: steerId, intent: "steer", taskId, text: "Go on" });
  const refused = await until(taskId, "orchestration.MessageRefused");
  assert.equal(
    refused.at(-1)?.payload.reason,
    "The task has ended. Send the message as a new task.",
  );
});

test("a failover attempt gives nothing; an empty last answer gives one more plan call", async () => {
  const messageId = randomUUID();
  await post({ messageId, intent: "new_task" });
  const taskId = await taskOf(messageId);
  const { callId } = await callOf(taskId);
  await llm("CallFailed", taskId, callId, {
    errorClass: "provider",
    reason: "HTTP 500, provider",
    costMicroUsd: 0,
    nextModel: "openai/gpt-4.1-nano",
  });
  await llm("CallCompleted", taskId, callId, { ...completedCall("  "), attempt: 2 });
  const retry = await lastCall(taskId, 2);
  assert.equal(retry.payload.kind, "plan");
  assert.equal((await streamOf(taskId)).length, 4);
});

test("each consumed event is handled once: a redelivered message and call end", async () => {
  const messageId = randomUUID();
  const eventId = randomUUID();
  await post({ messageId, intent: "new_task" }, eventId);
  await post({ messageId, intent: "new_task" }, eventId, randomUUID());
  const taskId = await taskOf(messageId);
  const { callId } = await callOf(taskId);
  const endId = randomUUID();
  await llm("CallCompleted", taskId, callId, completedCall(onePlan), endId);
  await until(taskId, "orchestration.PlanProposed");
  await llm("CallCompleted", taskId, callId, completedCall(onePlan), endId, randomUUID());
  await consumed("CONVERSATION");
  await consumed("LLM");
  assert.equal((await manager.streams.info("DLQ")).state.messages, 0);
  assert.equal(
    await stored().countDocuments({
      type: "orchestration.TaskSubmitted",
      "payload.messageId": messageId,
    }),
    1,
  );
  assert.equal(
    (await streamOf(taskId)).filter((event) => event.type === "orchestration.PlanProposed").length,
    1,
  );
});

test("no active orchestrator version fails the task with not_found", async () => {
  agentAnswer = [404, { code: "not_found" }];
  try {
    const messageId = randomUUID();
    await post({ messageId, intent: "new_task" });
    const taskId = await taskOf(messageId);
    const events = await until(taskId, "orchestration.RunFailed");
    assert.deepEqual(
      events.map((event) => event.type),
      ["orchestration.TaskSubmitted", "orchestration.RunFailed"],
    );
    assert.equal(events.at(-1)?.payload.code, "not_found");
  } finally {
    agentAnswer = [200, orchestratorAgent];
  }
});

test("agent-manager with 5xx after the retries fails the task with unavailable and a reply", async () => {
  agentAnswer = [503, { code: "unavailable" }];
  try {
    const messageId = randomUUID();
    await post({ messageId, intent: "new_task" });
    const taskId = await taskOf(messageId);
    const events = await until(taskId, "orchestration.RunFailed");
    assert.equal(events.at(-1)?.payload.code, "unavailable");
    assert.match(String(events.at(-1)?.payload.reason), /Try again in a minute\.$/);
  } finally {
    agentAnswer = [200, orchestratorAgent];
  }
});

test("a planned run with no orchestrator: the timer starts a due retry, the output goes to the dependent, and the run completes with the outputs", async () => {
  const taskId = randomUUID();
  const runId = randomUUID();
  const [first, second] = [randomUUID(), randomUUID()];
  const subtask = (subtaskId: string, title: string, dependsOn: string[]) => ({
    subtaskId,
    title,
    instructions: `Do ${title}.`,
    agentId: writerAgent.agentId,
    dependsOn,
    needsApproval: false,
  });
  const ids = { taskId, runId, subtaskId: first };
  const history: [string, object][] = [
    ["TaskSubmitted", { taskId, text: "Write a report", mode: "auto" }],
    [
      "PlanProposed",
      {
        taskId,
        planVersion: 1,
        subtasks: [subtask(first, "Facts", []), subtask(second, "Text", [first])],
        awaitsApproval: false,
      },
    ],
    ["PlanApproved", { taskId, planVersion: 1 }],
    ["RunStarted", { taskId, runId }],
    ["SubtaskQueued", ids],
    ["SubtaskWaiting", { ...ids, subtaskId: second }],
    [
      "SubtaskFailed",
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
    ["SubtaskRetrying", { ...ids, tryNumber: 2, retryAt: new Date().toISOString() }],
  ];
  const appended = await createEventStore(mongo.db).append({
    streamId: `task-${taskId}`,
    expectedVersion: 0,
    events: history.map(([type, payload]) => ({
      type: `orchestration.${type}`,
      schemaVersion: 1,
      payload,
      correlationId: taskId,
      causationId: taskId,
      actor: { kind: "system", id: "test" },
    })) as never,
  });
  const timerId = appended.events.at(-1)?.eventId ?? "";
  const retried = await until(taskId, "orchestration.SubtaskStarted");
  const startedTry = retried.find((event) => event.type === "orchestration.SubtaskStarted");
  assert.ok(startedTry);
  assert.equal(startedTry.payload.tryNumber, 2);
  assert.equal(startedTry.causationId, timerId);
  await waitFor(
    async () =>
      (await mongo.db.collection("processed").countDocuments({ _id: timerId as never })) === 1,
  );
  const firstCall = String(startedTry.payload.callId);
  await llm("CallCompleted", taskId, firstCall, {
    ...completedCall("The facts."),
    callInputTokens: 12,
    callOutputTokens: 3,
    callCostMicroUsd: 21,
  });
  await waitFor(async () =>
    (await streamOf(taskId)).some(
      (event) =>
        event.type === "orchestration.SubtaskStarted" && event.payload.subtaskId === second,
    ),
  );
  const middle = await streamOf(taskId);
  const secondCall = middle.findLast((event) => event.type === "orchestration.CallRequested");
  assert.ok(secondCall);
  assert.equal(secondCall.payload.maxCostMicroUsd, 50_000 - 21);
  assert.equal(secondCall.payload.kind, "subtask");
  await llm("CallCompleted", taskId, String(secondCall.payload.callId), completedCall("The text."));
  const done = await until(taskId, "orchestration.RunCompleted");
  assert.deepEqual(
    done.slice(history.length).map((event) => event.type.replace("orchestration.", "")),
    [
      "SubtaskStarted",
      "CallRequested",
      "CallSettled",
      "SubtaskCompleted",
      "SubtaskQueued",
      "SubtaskStarted",
      "CallRequested",
      "CallSettled",
      "SubtaskCompleted",
      "RunCompleted",
    ],
  );
  assert.equal(done.at(-1)?.payload.summary, "## Facts\n\nThe facts.\n\n## Text\n\nThe text.");
});
