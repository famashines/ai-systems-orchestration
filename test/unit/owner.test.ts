import assert from "node:assert/strict";
import { test } from "node:test";
import type { PlanSubtask } from "@famashines/ai-systems-contracts";
import { fakeIds } from "@famashines/ai-systems-kit/testing";
import {
  decide,
  event,
  fold,
  initialState,
  TaskConflictError,
  type HistoryEvent,
  type TaskCommand,
} from "../../src/domain/task.ts";
import { cell, history, states } from "./matrix.ts";

const taskId = "00000000-0000-4000-8000-000000000101";
const A = "00000000-0000-4000-8000-0000000001a1";
const B = "00000000-0000-4000-8000-0000000001b1";
const otherAgent = "00000000-0000-4000-8000-0000000001c1";
const model = "openai/gpt-4.1-nano";

const step = (subtaskId: string, over: Partial<PlanSubtask> = {}): PlanSubtask => ({
  subtaskId,
  title: subtaskId === A ? "A" : "B",
  instructions: "Do it.",
  agentId: otherAgent,
  dependsOn: [],
  needsApproval: false,
  ...over,
});

const inputs: [string, TaskCommand][] = [
  ["ApprovePlan", { type: "ApprovePlan", planVersion: 1 }],
  ["EditPlan", { type: "EditPlan", planVersion: 1, subtasks: [step(A)] }],
  ["RejectPlan", { type: "RejectPlan", planVersion: 1, reason: "No." }],
  ["ReassignSubtask", { type: "ReassignSubtask", subtaskId: B, agentId: otherAgent }],
  ["SetTaskMode", { type: "SetTaskMode", mode: "approval" }],
];

const conflict = "conflict";
const before = [conflict, conflict, conflict, conflict, "TaskModeChanged"];
const ended = [conflict, conflict, conflict, conflict, conflict];
const plan: Record<string, string[]> = {
  new: ended,
  submitted: before,
  clarifying: before,
  planning: before,
  awaiting_approval: [
    "PlanApproved RunStarted SubtaskQueued SubtaskWaiting SubtaskStarted CallRequested",
    "PlanEdited",
    "PlanRejected",
    conflict,
    "TaskModeChanged",
  ],
  running: [conflict, conflict, conflict, "SubtaskReassigned", "TaskModeChanged"],
  paused: [conflict, conflict, conflict, "SubtaskReassigned", "TaskModeChanged"],
  blocked: [conflict, conflict, conflict, "SubtaskReassigned", "TaskModeChanged"],
  completed: ended,
  failed: ended,
  cancelled: ended,
  rejected: ended,
};
const chat: Record<string, string[]> = {
  ...plan,
  running: before,
  paused: before,
  blocked: before,
};

for (const [kind, expected] of [
  ["plan", plan],
  ["chat", chat],
] as const) {
  for (const status of states) {
    test(`owner plan commands of a ${kind} task in ${status}`, () => {
      const events = history(kind === "plan", status);
      assert.deepEqual(
        inputs.map(([, command]) => cell(events, command)),
        expected[status],
      );
    });
  }
}

const env = {
  now: new Date("2026-11-01T10:00:00.000Z"),
  newId: fakeIds(7),
  settings: { maxParallel: 3, maxCallsPerTask: 50, retryDelaysMs: [5000], checkpointTimeoutMs: 1 },
  agents: new Map(),
};

const waiting = (subtasks: PlanSubtask[], planVersion = 1): HistoryEvent[] => [
  event("orchestration.TaskSubmitted", { taskId, text: "Write.", mode: "approval" }),
  event("orchestration.PlanProposed", { taskId, planVersion, subtasks, awaitsApproval: true }),
];

const conflictOf = (events: HistoryEvent[], command: TaskCommand): string => {
  try {
    decide(fold(initialState, events), command, env);
  } catch (error) {
    if (error instanceof TaskConflictError) return error.message;
    throw error;
  }
  return "no conflict";
};

test("ApprovePlan, EditPlan and RejectPlan of an older plan version are conflicts", () => {
  const edited = [
    ...waiting([step(A)]),
    event("orchestration.PlanEdited", { taskId, planVersion: 2, subtasks: [step(A)] }),
  ];
  for (const command of inputs.slice(0, 3).map(([, input]) => input)) {
    assert.equal(conflictOf(edited, command), "The plan version is 2, not 1. Read the plan again.");
  }
  const approved = decide(fold(initialState, edited), { type: "ApprovePlan", planVersion: 2 }, env);
  assert.deepEqual(approved[0], event("orchestration.PlanApproved", { taskId, planVersion: 2 }));
});

test("ApprovePlan approves the plan and starts the run with a new runId", () => {
  const events = waiting([step(A)]);
  const decided = decide(fold(initialState, events), { type: "ApprovePlan", planVersion: 1 }, env);
  const [, started] = decided;
  assert.equal(started?.type, "orchestration.RunStarted");
  const runId = started.payload.runId;
  assert.match(runId, /^[0-9a-f-]{36}$/);
  assert.deepEqual(
    decided.map((item) => item.type),
    ["orchestration.PlanApproved", "orchestration.RunStarted", "orchestration.SubtaskQueued"],
  );
  const state = fold(fold(initialState, events), decided);
  assert.equal(state.status, "running");
  assert.equal(state.plan?.approved, true);
});

test("EditPlan gives the next plan version, drops the estimates of the owner, and waits for approval", () => {
  const subtasks = [
    step(A, { estimatedCostMicroUsd: 9, estimatedDurationMs: 9, model }),
    step(B, { dependsOn: [A] }),
  ];
  const decided = decide(
    fold(initialState, waiting([step(A)])),
    { type: "EditPlan", planVersion: 1, subtasks },
    env,
  );
  assert.deepEqual(decided, [
    event("orchestration.PlanEdited", {
      taskId,
      planVersion: 2,
      subtasks: [step(A, { model }), step(B, { dependsOn: [A] })],
    }),
  ]);
  assert.equal(subtasks[0]?.estimatedCostMicroUsd, 9);
  const state = fold(fold(initialState, waiting([step(A)])), decided);
  assert.equal(state.status, "awaiting_approval");
  assert.deepEqual(state.plan, {
    planVersion: 2,
    subtasks: decided[0]?.payload.subtasks,
    approved: false,
  });
});

test("RejectPlan ends the task with the reason", () => {
  const decided = decide(
    fold(initialState, waiting([step(A)])),
    { type: "RejectPlan", planVersion: 1, reason: "x".repeat(1200) },
    env,
  );
  assert.deepEqual(decided, [
    event("orchestration.PlanRejected", { taskId, planVersion: 1, reason: "x".repeat(1000) }),
  ]);
  assert.equal(fold(fold(initialState, waiting([step(A)])), decided).status, "rejected");
});

test("ReassignSubtask: only a subtask of the run that does not run and has not ended", () => {
  const running = history(true, "running");
  assert.equal(
    conflictOf(running, { type: "ReassignSubtask", subtaskId: A, agentId: otherAgent }),
    "The subtask is not in the run, or it runs or has ended.",
  );
  assert.equal(
    conflictOf(running, { type: "ReassignSubtask", subtaskId: otherAgent, agentId: otherAgent }),
    "The subtask is not in the run, or it runs or has ended.",
  );
  const state = fold(initialState, running);
  const decided = decide(
    state,
    { type: "ReassignSubtask", subtaskId: B, agentId: otherAgent, model },
    env,
  );
  assert.deepEqual(decided, [
    event("orchestration.SubtaskReassigned", {
      taskId,
      runId: state.runId ?? "",
      subtaskId: B,
      agentId: otherAgent,
      model,
    }),
  ]);
  const after = fold(state, decided).plan?.subtasks.find((item) => item.subtaskId === B);
  assert.equal(after?.agentId, otherAgent);
  assert.equal(after.model, model);
});

test("SetTaskMode changes the mode until the task ends", () => {
  const state = fold(initialState, waiting([step(A)]));
  const decided = decide(state, { type: "SetTaskMode", mode: "auto" }, env);
  assert.deepEqual(decided, [event("orchestration.TaskModeChanged", { taskId, mode: "auto" })]);
  const changed = fold(state, decided);
  assert.equal(changed.mode, "auto");
  assert.equal(changed.status, "awaiting_approval");
});
