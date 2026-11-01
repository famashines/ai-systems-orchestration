import type {
  CallCompletedEventPayloadV1,
  CallFailedEventPayloadV1,
  CallMessage,
  ErrorCode,
  TaskMode,
} from "@famashines/ai-systems-contracts";
import {
  block,
  callDurationMs,
  decideRun,
  event,
  notStarted,
  settleCall,
  type RunCommand,
  type RunEnv,
} from "./run.ts";
import {
  failedCodes,
  isRunActive,
  nextCall,
  reason,
  runningCalls,
  TaskExistsError,
  type LoadFailure,
  type TaskEvent,
  type TaskState,
  type Writer,
} from "./state.ts";

export * from "./state.ts";
export * from "./run.ts";

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
  runId: string;
  callId: string;
  message: ChatMessage;
  writer: Writer | LoadFailure;
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

export const dataBlock = ({ messageId, text }: ChatMessage): string =>
  [
    "Answer the owner's chat message. It is in the data block below.",
    block("owner_message", messageId, text),
  ].join("\n");

function startChatTask(command: StartChatTask): TaskEvent[] {
  const { taskId, runId, callId, message, writer } = command;
  const events: TaskEvent[] = [
    event("orchestration.TaskSubmitted", {
      taskId,
      text: message.text,
      mode: "auto",
      conversationId: message.conversationId,
      messageId: message.messageId,
    }),
    event("orchestration.RunStarted", { taskId, runId }),
  ];
  const failed = (code: ErrorCode, text: string): TaskEvent[] => [
    ...events,
    event("orchestration.RunFailed", { taskId, runId, code, reason: reason(text) }),
  ];
  if (!("agent" in writer)) return failed(writer.code, writer.reason);
  const { agent, prompt } = writer;
  if (!agent.enabled) return failed("unavailable", "The writer agent is off.");
  if (prompt.variables.length > 0) {
    return failed("internal", "The writer prompt has variables. Orchestration cannot fill them.");
  }
  const next = nextCall(agent.limits, { calls: 0, costMicroUsd: 0 });
  if ("stop" in next) {
    return [
      ...events,
      event("orchestration.RunBlocked", {
        taskId,
        runId,
        code: "runaway_stopped",
        reason: reason(next.stop),
      }),
    ];
  }
  const messages: CallMessage[] = [
    { role: "system", content: prompt.text },
    { role: "user", content: dataBlock(message) },
  ];
  return [
    ...events,
    event("orchestration.CallRequested", {
      taskId,
      callId,
      agentId: agent.agentId,
      agentVersion: agent.version,
      messages,
      models: agent.models,
      maxTokens: agent.limits.maxTokens,
      maxDurationMs: callDurationMs,
      maxCostMicroUsd: next.maxCostMicroUsd,
    }),
  ];
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
      return "The task does not run in the chat. Send the message as a new task.";
    case "clarifying":
    case "planning":
    case "awaiting_approval":
      return "The task has no run yet. Send the message as a new task.";
    default:
      return "The task has ended. Send the message as a new task.";
  }
}

function takeMessage(state: TaskState, command: TakeMessage, env: RunEnv): TaskEvent[] {
  const { taskId, message, intent } = command;
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
      return startChatTask(command);
    case "TakeMessage":
      return takeMessage(state, command, env);
    case "CompleteCall":
      return settleCall(state, { completed: command.call }, env) ?? completeCall(state, command);
    case "FailCall":
      return settleCall(state, { failed: command.call }, env) ?? failCall(state, command);
    default:
      return decideRun(state, command, env);
  }
}
