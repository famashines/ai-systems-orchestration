import type { PlanSubtask } from "@famashines/ai-systems-contracts";
import { fakeIds } from "@famashines/ai-systems-kit/testing";
import {
  decide,
  event,
  fold,
  initialState,
  schedule,
  TaskConflictError,
  type HistoryEvent,
  type TaskCommand,
  type TaskStatus,
  type Writer,
} from "../../src/domain/task.ts";

const taskId = "00000000-0000-4000-8000-000000000101";
const runId = "00000000-0000-4000-8000-000000000102";
const conversationId = "00000000-0000-4000-8000-000000000103";
const messageId = "00000000-0000-4000-8000-000000000104";
const agentId = "00000000-0000-4000-8000-000000000105";
const callId = "00000000-0000-4000-8000-000000000106";
const unknownId = "00000000-0000-4000-8000-000000000107";
const [A, B] = ["00000000-0000-4000-8000-0000000001a1", "00000000-0000-4000-8000-0000000001b1"];
const ids = { taskId, runId };
const model = "google/gemini-2.5-flash-lite";
const now = new Date("2026-11-01T10:00:00.000Z");

const agent: Writer = {
  agent: {
    agentId,
    name: "writer",
    enabled: true,
    version: 1,
    prompt: { promptId: agentId, version: 1 },
    models: [model],
    limits: { maxCalls: 10, maxTokens: 1000, maxCostMicroUsd: 50_000 },
  },
  prompt: { promptId: agentId, name: "writer", version: 1, text: "You write.", variables: [] },
};

const step = (subtaskId: string, dependsOn: string[]): PlanSubtask => ({
  subtaskId,
  title: subtaskId === A ? "A" : "B",
  instructions: "Do it.",
  agentId,
  dependsOn,
  needsApproval: false,
});

const submitted = event("orchestration.TaskSubmitted", {
  taskId,
  text: "Write.",
  mode: "auto",
  conversationId,
  messageId,
});
const proposed = (awaitsApproval: boolean) =>
  event("orchestration.PlanProposed", {
    taskId,
    planVersion: 1,
    subtasks: [step(A, []), step(B, [A])],
    awaitsApproval,
  });
const call = (kind?: "subtask" | "plan") =>
  event("orchestration.CallRequested", {
    taskId,
    callId,
    agentId,
    agentVersion: 1,
    messages: [{ role: "user", content: "x" }],
    models: [model],
    maxTokens: 1000,
    maxDurationMs: 120_000,
    maxCostMicroUsd: 25_000,
    ...(kind === undefined ? {} : kind === "plan" ? { kind } : { kind, subtaskId: A }),
  });
const usage = { inputTokens: 1, outputTokens: 1, costMicroUsd: 10 };

const planRun: HistoryEvent[] = [
  submitted,
  proposed(false),
  event("orchestration.PlanApproved", { taskId, planVersion: 1 }),
  event("orchestration.RunStarted", ids),
  event("orchestration.SubtaskQueued", { ...ids, subtaskId: A }),
  event("orchestration.SubtaskWaiting", { ...ids, subtaskId: B }),
  event("orchestration.SubtaskStarted", {
    ...ids,
    subtaskId: A,
    tryNumber: 1,
    callId,
    agentId,
    agentVersion: 1,
  }),
  call("subtask"),
];
const chatRun: HistoryEvent[] = [submitted, event("orchestration.RunStarted", ids), call()];
const settle = event("orchestration.CallSettled", {
  taskId,
  callId,
  kind: "subtask",
  subtaskId: A,
  ...usage,
});

export const states: TaskStatus[] = [
  "new",
  "submitted",
  "clarifying",
  "planning",
  "awaiting_approval",
  "running",
  "paused",
  "blocked",
  "completed",
  "failed",
  "cancelled",
  "rejected",
];

const planSettled = event("orchestration.CallSettled", { taskId, callId, kind: "plan", ...usage });
const asked = [
  submitted,
  call("plan"),
  planSettled,
  event("orchestration.ClarificationRequested", { taskId, questions: ["Which?"] }),
];

const before = (plan: boolean): Record<string, HistoryEvent[]> => ({
  new: [],
  submitted: [submitted, call("plan")],
  clarifying: asked,
  planning: plan
    ? [submitted, proposed(false), event("orchestration.PlanApproved", { taskId, planVersion: 1 })]
    : [
        ...asked,
        event("orchestration.ClarificationAnswered", {
          taskId,
          conversationId,
          messageId,
          text: "This.",
        }),
      ],
  awaiting_approval: [submitted, proposed(true)],
  rejected: [
    submitted,
    proposed(true),
    event("orchestration.PlanRejected", { taskId, planVersion: 1, reason: "No." }),
  ],
});

export function history(plan: boolean, state: TaskStatus): HistoryEvent[] {
  const pre = before(plan)[state];
  if (pre !== undefined) return pre;
  const run = plan ? planRun : chatRun;
  switch (state) {
    case "paused":
      return [...run, event("orchestration.RunPaused", ids)];
    case "blocked":
      return plan
        ? [
            ...run,
            settle,
            event("orchestration.SubtaskFailed", {
              ...ids,
              subtaskId: A,
              tryNumber: 1,
              code: "internal",
              reason: "x",
              ...usage,
            }),
            event("orchestration.RunBlocked", { ...ids, code: "subtask_failed", reason: "x" }),
          ]
        : [
            ...run,
            event("orchestration.RunBlocked", { ...ids, code: "budget_refused", reason: "x" }),
          ];
    case "completed":
      return [
        ...run,
        event("orchestration.RunCompleted", { ...ids, summary: "x", costMicroUsd: 0 }),
      ];
    case "failed":
      return [...run, event("orchestration.RunFailed", { ...ids, code: "internal", reason: "x" })];
    case "cancelled":
      return [...run, event("orchestration.RunCancelled", ids)];
    default:
      return run;
  }
}

const message = { conversationId, messageId: unknownId, text: "More." };
export const inputs: [string, TaskCommand][] = [
  ["StartRun", { type: "StartRun", runId }],
  ["PauseRun", { type: "PauseRun" }],
  ["ResumeRun", { type: "ResumeRun" }],
  ["CancelRun", { type: "CancelRun" }],
  ["CancelTask", { type: "CancelTask" }],
  ["RetrySubtask", { type: "RetrySubtask", subtaskId: A }],
  [
    "ResolveCheckpoint",
    { type: "ResolveCheckpoint", checkpointId: unknownId, decision: "approved" },
  ],
  ["CheckpointDue", { type: "CheckpointDue", checkpointId: unknownId }],
  ["RetryDue", { type: "RetryDue", subtaskId: A }],
  ["steer", { type: "TakeMessage", taskId, message, intent: "steer" }],
  ["answer", { type: "TakeMessage", taskId, message, intent: "answer" }],
  [
    "CompleteCall",
    {
      type: "CompleteCall",
      call: {
        taskId,
        callId,
        attempt: 1,
        model,
        text: "Out.",
        finishReason: "stop",
        cachedTokens: 0,
        latencyMs: 1,
        bodyId: unknownId,
        ...usage,
      },
    },
  ],
  [
    "FailCall",
    {
      type: "FailCall",
      call: {
        taskId,
        callId,
        attempt: 1,
        model,
        errorClass: "provider",
        reason: "x",
        costMicroUsd: 2,
      },
    },
  ],
];

export function cell(events: HistoryEvent[], command: TaskCommand): string {
  const env = {
    now,
    newId: fakeIds(9),
    settings: {
      maxParallel: 3,
      maxCallsPerTask: 50,
      retryDelaysMs: [5000],
      checkpointTimeoutMs: 60_000,
      planAgents: ["writer"],
    },
    agents: new Map([[agentId, agent]]),
    named: new Map([["writer", agent]]),
  };
  const state = fold(initialState, events);
  try {
    const decided = decide(state, command, env);
    const all = [...decided, ...schedule(fold(state, decided), env)];
    return all.map((item) => item.type.replace("orchestration.", "")).join(" ") || "-";
  } catch (error) {
    if (error instanceof TaskConflictError) return "conflict";
    throw error;
  }
}
