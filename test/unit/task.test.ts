import assert from "node:assert/strict";
import { test } from "node:test";
import { fakeIds } from "@famashines/ai-systems-kit/testing";
import {
  callDurationMs,
  decide,
  evolve,
  initialState,
  nextCall,
  staleReason,
  TaskExistsError,
  taskStreamId,
  type RunEnv,
  type TaskCommand,
  type TaskEvent,
  type Writer,
} from "../../src/domain/task.ts";

const taskId = "00000000-0000-4000-8000-000000000001";
const runId = "00000000-0000-4000-8000-000000000002";
const callId = "00000000-0000-4000-8000-000000000003";
const conversationId = "00000000-0000-4000-8000-000000000004";
const messageId = "00000000-0000-4000-8000-000000000005";
const agentId = "00000000-0000-4000-8000-00000000a002";
const promptId = "cc74090a-534e-4072-8bfd-e6c5b9f22f41";
const message = { conversationId, messageId, text: "Write a poem" };
const calledAt = "2026-11-01T10:00:00.000Z";
const minutes = (n: number) => new Date(Date.parse(calledAt) + n * 60_000);

const writer: Writer = {
  agent: {
    agentId,
    name: "writer",
    enabled: true,
    version: 2,
    prompt: { promptId, version: 1 },
    models: ["google/gemini-2.5-flash-lite", "openai/gpt-4.1-nano"],
    limits: { maxCalls: 10, maxTokens: 4096, maxCostMicroUsd: 50_000 },
  },
  prompt: {
    promptId,
    name: "writer",
    version: 1,
    text: "You are the writer agent.",
    variables: [],
  },
};

const submitted: TaskEvent = {
  type: "orchestration.TaskSubmitted",
  schemaVersion: 1,
  payload: { taskId, text: "Write a poem", mode: "auto", conversationId, messageId },
};
const started: TaskEvent = {
  type: "orchestration.RunStarted",
  schemaVersion: 1,
  payload: { taskId, runId },
};
const env = (now = minutes(1)): RunEnv => ({
  now,
  newId: fakeIds(7),
  settings: {
    maxParallel: 3,
    maxCallsPerTask: 50,
    retryDelaysMs: [5000, 30000],
    checkpointTimeoutMs: 60_000,
  },
  agents: new Map(),
});

const requested: TaskEvent = {
  type: "orchestration.CallRequested",
  schemaVersion: 1,
  payload: {
    taskId,
    callId,
    agentId,
    agentVersion: 2,
    messages: [
      { role: "system", content: writer.prompt.text },
      { role: "user", content: "Write a poem" },
    ],
    models: writer.agent.models,
    maxTokens: 4096,
    maxDurationMs: callDurationMs,
    maxCostMicroUsd: 50_000,
  },
};
const running = [submitted, started, { ...requested, occurredAt: calledAt }];

const given = (events: (TaskEvent & { occurredAt?: string })[]) => ({
  when:
    (command: TaskCommand, now = minutes(1)) =>
    () =>
      decide(events.reduce(evolve, initialState), command, env(now)),
});

const completed = (text: string): TaskCommand => ({
  type: "CompleteCall",
  call: {
    taskId,
    callId,
    attempt: 1,
    model: "google/gemini-2.5-flash-lite",
    text,
    finishReason: "stop",
    inputTokens: 10,
    outputTokens: 2,
    cachedTokens: 0,
    costMicroUsd: 42,
    latencyMs: 100,
    bodyId: "00000000-0000-4000-8000-000000000006",
  },
});

const failed = (
  errorClass: "budget_refused" | "rate_limit" | "timeout" | "provider" | "invalid",
  extra: { nextModel?: string; callId?: string } = {},
): TaskCommand => ({
  type: "FailCall",
  call: {
    taskId,
    callId: extra.callId ?? callId,
    attempt: 1,
    model: "google/gemini-2.5-flash-lite",
    errorClass,
    reason: "No monthly cap is set",
    costMicroUsd: 0,
    ...(extra.nextModel === undefined ? {} : { nextModel: extra.nextModel }),
  },
});

const refuse = (intent: "steer" | "answer"): TaskCommand => ({
  type: "TakeMessage",
  taskId,
  message,
  intent,
});

const refused = (intent: "steer" | "answer", reason: string): TaskEvent => ({
  type: "orchestration.MessageRefused",
  schemaVersion: 1,
  payload: { taskId, conversationId, messageId, intent, reason },
});

test("given no events, when SubmitTask, then TaskSubmitted", () => {
  const submit: TaskCommand = { type: "SubmitTask", taskId, text: "Write a poem", mode: "auto" };
  assert.deepEqual(given([]).when(submit)(), [
    {
      type: "orchestration.TaskSubmitted",
      schemaVersion: 1,
      payload: { taskId, text: "Write a poem", mode: "auto" },
    },
  ]);
  assert.throws(given([submitted]).when(submit), TaskExistsError);
});

test("nextCall gives the rest of the task allowance after an earlier call", () => {
  const limits = { maxCalls: 3, maxTokens: 4096, maxCostMicroUsd: 50_000 };
  assert.deepEqual(nextCall(limits, { calls: 0, costMicroUsd: 0 }), { maxCostMicroUsd: 50_000 });
  assert.deepEqual(nextCall(limits, { calls: 1, costMicroUsd: 12_345 }), {
    maxCostMicroUsd: 37_655,
  });
  assert.deepEqual(nextCall(limits, { calls: 2, costMicroUsd: 50_000 }), {
    stop: "The task used 50000 of its 50000 micro-USD allowance.",
  });
  assert.deepEqual(nextCall(limits, { calls: 3, costMicroUsd: 10 }), {
    stop: "The task made 3 calls, the limit of the agent.",
  });
});

const runFailed = (code: string, reason: string): TaskEvent =>
  ({
    type: "orchestration.RunFailed",
    schemaVersion: 1,
    payload: { taskId, runId, code, reason },
  }) as TaskEvent;

test("given a running call, when CallCompleted, then RunCompleted with the text and cost", () => {
  assert.deepEqual(given(running).when(completed("A poem."))(), [
    {
      type: "orchestration.RunCompleted",
      schemaVersion: 1,
      payload: { taskId, runId, summary: "A poem.", costMicroUsd: 42 },
    },
  ]);
});

for (const text of ["", " \n\t "]) {
  test(`given a running call, when CallCompleted with ${JSON.stringify(text)}, then RunFailed empty_output`, () => {
    assert.deepEqual(given(running).when(completed(text))(), [
      runFailed("empty_output", "The model gave an empty answer."),
    ]);
  });
}

test("given a running call, when CallFailed budget_refused, then RunBlocked budget_refused", () => {
  assert.deepEqual(given(running).when(failed("budget_refused"))(), [
    {
      type: "orchestration.RunBlocked",
      schemaVersion: 1,
      payload: {
        taskId,
        runId,
        code: "budget_refused",
        reason: "The budget refused the call: No monthly cap is set",
      },
    },
  ]);
});

for (const [errorClass, code] of [
  ["rate_limit", "rate_limited"],
  ["timeout", "unavailable"],
  ["provider", "unavailable"],
  ["invalid", "internal"],
] as const) {
  test(`given a running call, when the last CallFailed is ${errorClass}, then RunFailed ${code}`, () => {
    assert.deepEqual(given(running).when(failed(errorClass))(), [
      runFailed(code, "The model call failed: No monthly cap is set"),
    ]);
  });
}

test("a CallFailed with a next model, or for another call, gives no event", () => {
  assert.deepEqual(
    given(running).when(failed("provider", { nextModel: "openai/gpt-4.1-nano" }))(),
    [],
  );
  assert.deepEqual(
    given(running).when(failed("provider", { callId: "00000000-0000-4000-8000-0000000000ff" }))(),
    [],
  );
});

test("given an ended task, a late call event gives no event", () => {
  const ended = [...running, runFailed("empty_output", "The model gave an empty answer.")];
  assert.deepEqual(given(ended).when(completed("Late."))(), []);
  assert.deepEqual(given([]).when(completed("Unknown."))(), []);
});

test("given a running call, when a steer comes, then MessageRefused", () => {
  assert.deepEqual(given(running).when(refuse("steer"))(), [
    refused(
      "steer",
      "The task is running and cannot take a message now. Wait for the answer, or send the message as a new task.",
    ),
  ]);
});

test("given a call older than 15 minutes, when a steer or an answer comes, then RunFailed and MessageRefused", () => {
  const reason =
    "The answer did not come in 15 minutes, so the task stopped. A later answer is not shown, and its cost is still booked.";
  assert.equal(staleReason, reason);
  const cancel: TaskEvent = {
    type: "orchestration.CallCancelRequested",
    schemaVersion: 1,
    payload: { taskId, callId },
  };
  assert.deepEqual(given(running).when(refuse("steer"), minutes(16))(), [
    runFailed("unavailable", reason),
    cancel,
    refused("steer", "The task has ended. Send the message as a new task."),
  ]);
  assert.deepEqual(given(running).when(refuse("answer"), minutes(16))(), [
    runFailed("unavailable", reason),
    cancel,
    refused("answer", "The task has no open question."),
  ]);
  assert.deepEqual(given(running).when(refuse("steer"), minutes(15))().length, 1);
});

test("given a blocked task, when a steer comes, then MessageRefused with the block reason", () => {
  const blocked = given(running).when(failed("budget_refused"))();
  assert.deepEqual(given([...running, ...blocked]).when(refuse("steer"))(), [
    refused(
      "steer",
      "The task is blocked until the owner acts. Send the message as a new task. The reason: The budget refused the call: No monthly cap is set",
    ),
  ]);
});

test("other refusals: unknown, answer, ended and an API task", () => {
  const reasonOf = (events: TaskEvent[]) =>
    events[0]?.type === "orchestration.MessageRefused" ? events[0].payload.reason : undefined;
  assert.equal(reasonOf(given([]).when(refuse("steer"))()), "The task is not known.");
  assert.equal(reasonOf(given(running).when(refuse("answer"))()), "The task has no open question.");
  const done = given(running).when(completed("A poem."))();
  assert.equal(
    reasonOf(given([...running, ...done]).when(refuse("steer"))()),
    "The task has ended. Send the message as a new task.",
  );
  assert.equal(
    reasonOf(given([submitted]).when(refuse("steer"))()),
    "The task has not started its run yet. Wait for the plan or the questions.",
  );
  const api: TaskEvent = {
    type: "orchestration.TaskSubmitted",
    schemaVersion: 1,
    payload: { taskId, text: "Write a poem", mode: "auto" },
  };
  assert.equal(
    reasonOf(given([api]).when(refuse("steer"))()),
    "The task does not run in the chat. Send the message as a new task.",
  );
  for (const status of [
    { type: "orchestration.ClarificationRequested", payload: { taskId, questions: ["Which?"] } },
    {
      type: "orchestration.PlanProposed",
      payload: { taskId, planVersion: 1, subtasks: [], awaitsApproval: true },
    },
  ]) {
    assert.equal(
      reasonOf(
        given([submitted, { ...status, schemaVersion: 1 } as TaskEvent]).when(refuse("steer"))(),
      ),
      "The task has not started its run yet. Wait for the plan or the questions.",
    );
  }
});

test("a long reason is cut to 1000 characters", () => {
  const command = failed("provider");
  if (command.type === "FailCall") command.call.reason = "x".repeat(1000);
  const [event] = given(running).when(command)();
  assert.equal(event?.type === "orchestration.RunFailed" && event.payload.reason.length, 1000);
});

test("MessageRefused changes no state; evolve refuses an unknown event type", () => {
  const state = running.reduce(evolve, initialState);
  assert.deepEqual(evolve(state, refused("steer", "No.")), state);
  const unknown = { ...submitted, type: "orchestration.Unknown" } as unknown as TaskEvent;
  assert.throws(() => evolve(initialState, unknown), /Unknown event type orchestration.Unknown/);
});

test("the stream id is task-<taskId>", () => {
  assert.equal(taskStreamId(taskId), `task-${taskId}`);
});
