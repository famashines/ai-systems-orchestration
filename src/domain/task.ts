import type {
  CallCompletedEventPayloadV1,
  CallFailedEventPayloadV1,
  TaskMode,
} from "@famashines/ai-systems-contracts";
import { agentNames, isPlanCall, planCall, settlePlan } from "./plan.ts";
import {
  decideRun,
  event,
  notStarted,
  settleCall,
  type RunCommand,
  type RunEnv,
  type RunSettings,
} from "./run.ts";
import {
  failedCodes,
  fold,
  isRunActive,
  reason,
  runningCalls,
  TaskExistsError,
  type AgentLoad,
  type TaskEvent,
  type TaskState,
} from "./state.ts";

export * from "./state.ts";
export * from "./run.ts";
export * from "./plan.ts";

export const staleAfterMs = 15 * 60_000;
export const staleReason =
  "The answer did not come in 15 minutes, so the task stopped. A later answer is not shown, and its cost is still booked.";

export interface ChatMessage {
  conversationId: string;
  messageId: string;
  text: string;
}

export interface SubmitTask {
  type: "SubmitTask";
  taskId: string;
  text: string;
  mode: TaskMode;
}

export interface StartChatTask {
  type: "StartChatTask";
  taskId: string;
  mode: TaskMode;
  message: ChatMessage;
  orchestrator: AgentLoad;
}

export interface TakeMessage {
  type: "TakeMessage";
  taskId: string;
  message: ChatMessage;
  intent: "steer" | "answer";
}

export interface CompleteCall {
  type: "CompleteCall";
  call: CallCompletedEventPayloadV1;
}

export interface FailCall {
  type: "FailCall";
  call: CallFailedEventPayloadV1;
}

export type TaskCommand =
  SubmitTask | StartChatTask | TakeMessage | CompleteCall | FailCall | RunCommand;

function startChatTask(
  state: TaskState,
  { taskId, mode, message, orchestrator }: StartChatTask,
  env: RunEnv,
): TaskEvent[] {
  const submitted = event("orchestration.TaskSubmitted", {
    taskId,
    text: message.text,
    mode,
    conversationId: message.conversationId,
    messageId: message.messageId,
  });
  return [submitted, ...planCall(fold(state, [submitted]), orchestrator, env)];
}

function refusal(state: TaskState, intent: "steer" | "answer"): string {
  if (state.status === "new") return "The task is not known.";
  if (intent === "answer") return "The task has no open question.";
  if (state.plan !== undefined && isRunActive(state)) {
    return "All subtasks of the run have started, so the message cannot change the run. Send the message as a new task.";
  }
  switch (state.status) {
    case "running":
    case "paused":
      return "The task is running and cannot take a message now. Wait for the answer, or send the message as a new task.";
    case "blocked":
      return `The task is blocked until the owner acts. Send the message as a new task. The reason: ${state.reason ?? "not known"}`;
    case "submitted":
    case "clarifying":
    case "planning":
    case "awaiting_approval":
      return state.conversationId === undefined
        ? "The task does not run in the chat. Send the message as a new task."
        : "The task has not started its run yet. Wait for the plan or the questions.";
    default:
      return "The task has ended. Send the message as a new task.";
  }
}

function takeMessage(state: TaskState, command: TakeMessage, env: RunEnv): TaskEvent[] {
  const { taskId, message, intent } = command;
  if (intent === "answer" && state.status === "clarifying" && state.conversationId !== undefined) {
    const answered = event("orchestration.ClarificationAnswered", { taskId, ...message });
    const load =
      state.orchestratorId === undefined ? undefined : env.agents.get(state.orchestratorId);
    return [answered, ...planCall(fold(state, [answered]), load, env)];
  }
  if (intent === "steer" && state.runId !== undefined && isRunActive(state) && notStarted(state)) {
    return [
      event("orchestration.SteeringMessageAdded", { taskId, ...message, runId: state.runId }),
    ];
  }
  const events: TaskEvent[] = [];
  let current = state;
  if (
    state.plan === undefined &&
    state.status === "running" &&
    state.runId !== undefined &&
    state.calledAt !== undefined &&
    env.now.getTime() - Date.parse(state.calledAt) > staleAfterMs
  ) {
    events.push(
      event("orchestration.RunFailed", {
        taskId,
        runId: state.runId,
        code: "unavailable",
        reason: staleReason,
      }),
      ...runningCalls(state).map((callId) =>
        event("orchestration.CallCancelRequested", { taskId, callId }),
      ),
    );
    current = { ...state, status: "failed" };
  }
  events.push(
    event("orchestration.MessageRefused", {
      taskId,
      conversationId: message.conversationId,
      messageId: message.messageId,
      intent,
      reason: reason(refusal(current, intent)),
    }),
  );
  return events;
}

export const isClosed = (state: TaskState): boolean =>
  ["completed", "failed", "blocked", "cancelled", "rejected"].includes(state.status);

const isCurrent = (state: TaskState, callId: string): state is TaskState & { runId: string } =>
  state.status === "running" && state.callId === callId && state.runId !== undefined;

function completeCall(state: TaskState, { call }: CompleteCall): TaskEvent[] {
  if (!isCurrent(state, call.callId)) return [];
  const { taskId } = call;
  const { runId } = state;
  if (call.text.trim() === "") {
    return [
      event("orchestration.RunFailed", {
        taskId,
        runId,
        code: "empty_output",
        reason: "The model gave an empty answer.",
      }),
    ];
  }
  return [
    event("orchestration.RunCompleted", {
      taskId,
      runId,
      summary: call.text,
      costMicroUsd: call.costMicroUsd,
    }),
  ];
}

function failCall(state: TaskState, { call }: FailCall): TaskEvent[] {
  if (call.nextModel !== undefined || !isCurrent(state, call.callId)) return [];
  const { taskId } = call;
  const { runId } = state;
  if (call.errorClass === "budget_refused") {
    return [
      event("orchestration.RunBlocked", {
        taskId,
        runId,
        code: "budget_refused",
        reason: reason(`The budget refused the call: ${call.reason}`),
      }),
    ];
  }
  return [
    event("orchestration.RunFailed", {
      taskId,
      runId,
      code: failedCodes[call.errorClass],
      reason: reason(`The model call failed: ${call.reason}`),
    }),
  ];
}

export function decide(state: TaskState, command: TaskCommand, env: RunEnv): TaskEvent[] {
  switch (command.type) {
    case "SubmitTask": {
      if (state.status !== "new") throw new TaskExistsError();
      const { taskId, text, mode } = command;
      return [event("orchestration.TaskSubmitted", { taskId, text, mode })];
    }
    case "StartChatTask":
      if (state.status !== "new") throw new TaskExistsError();
      return startChatTask(state, command, env);
    case "TakeMessage":
      return takeMessage(state, command, env);
    case "CompleteCall":
      return settle(state, { completed: command.call }, env) ?? completeCall(state, command);
    case "FailCall":
      return settle(state, { failed: command.call }, env) ?? failCall(state, command);
    default:
      return decideRun(state, command, env);
  }
}

type FinalCall = Parameters<typeof settlePlan>[1];

function settle(state: TaskState, final: FinalCall, env: RunEnv): TaskEvent[] | undefined {
  const { callId } = "completed" in final ? final.completed : final.failed;
  const planning = isPlanCall(state, callId);
  const settled = settleCall(state, final, env);
  if (!planning || settled === undefined || settled.length === 0) return settled;
  return [...settled, ...settlePlan(fold(state, settled), final, env)];
}

export interface Loads {
  agentIds: string[];
  names: string[];
}

export function toLoadBefore(
  state: TaskState,
  command: TaskCommand,
  settings: Pick<RunSettings, "planAgents">,
): Loads {
  const orchestrator = state.orchestratorId === undefined ? [] : [state.orchestratorId];
  switch (command.type) {
    case "TakeMessage":
      return command.intent === "answer" && state.status === "clarifying"
        ? { agentIds: orchestrator, names: [] }
        : { agentIds: [], names: [] };
    case "CompleteCall":
    case "FailCall": {
      if (!isPlanCall(state, command.call.callId)) return { agentIds: [], names: [] };
      const names =
        command.type === "CompleteCall"
          ? agentNames(command.call.text, settings.planAgents ?? [])
          : [];
      return { agentIds: orchestrator, names };
    }
    default:
      return { agentIds: [], names: [] };
  }
}
