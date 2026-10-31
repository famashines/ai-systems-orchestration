import type {
  ActiveAgentVersionResultV1,
  AgentLimits,
  CallCompletedEventPayloadV1,
  CallFailedEventPayloadV1,
  CallMessage,
  CallRequestedEventPayloadV1,
  ErrorCode,
  LlmErrorClass,
  MessageRefusedEventPayloadV1,
  PromptVersionResultV1,
  RunBlockedEventPayloadV1,
  RunCompletedEventPayloadV1,
  RunFailedEventPayloadV1,
  RunStartedEventPayloadV1,
  TaskMode,
  TaskSubmittedEventPayloadV1,
} from "@famashines/ai-systems-contracts";

export const callDurationMs = 120_000;
export const staleAfterMs = 15 * 60_000;
export const staleReason =
  "The answer did not come in 15 minutes, so the task stopped. A later answer is not shown, and its cost is still booked.";

export interface Writer {
  agent: ActiveAgentVersionResultV1;
  prompt: PromptVersionResultV1;
}

export interface LoadFailure {
  code: ErrorCode;
  reason: string;
}

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

export interface RefuseMessage {
  type: "RefuseMessage";
  taskId: string;
  message: ChatMessage;
  intent: "steer" | "answer";
  now: Date;
}

export interface CompleteCall {
  type: "CompleteCall";
  call: CallCompletedEventPayloadV1;
}

export interface FailCall {
  type: "FailCall";
  call: CallFailedEventPayloadV1;
}

export type TaskCommand = SubmitTask | StartChatTask | RefuseMessage | CompleteCall | FailCall;

type Event<T extends string, P> = { type: T; schemaVersion: 1; payload: P };

export type TaskEvent =
  | Event<"orchestration.TaskSubmitted", TaskSubmittedEventPayloadV1>
  | Event<"orchestration.RunStarted", RunStartedEventPayloadV1>
  | Event<"orchestration.CallRequested", CallRequestedEventPayloadV1>
  | Event<"orchestration.RunCompleted", RunCompletedEventPayloadV1>
  | Event<"orchestration.RunFailed", RunFailedEventPayloadV1>
  | Event<"orchestration.RunBlocked", RunBlockedEventPayloadV1>
  | Event<"orchestration.MessageRefused", MessageRefusedEventPayloadV1>;

export type TaskStatus = "new" | "submitted" | "running" | "blocked" | "completed" | "failed";

export interface TaskState {
  status: TaskStatus;
  runId?: string;
  callId?: string;
  calledAt?: string;
  reason?: string;
}

export class TaskExistsError extends Error {
  constructor() {
    super("The task exists");
    this.name = "TaskExistsError";
  }
}

export const initialState: TaskState = { status: "new" };

export const taskStreamId = (taskId: string): string => `task-${taskId}`;

const reason = (text: string): string => text.slice(0, 1000);

export const dataBlock = ({ messageId, text }: ChatMessage): string =>
  [
    "Answer the owner's chat message. It is in the data block below.",
    `BEGIN DATA owner_message ${messageId}`,
    text,
    `END DATA owner_message ${messageId}`,
  ].join("\n");

export type NextCall = { maxCostMicroUsd: number } | { stop: string };

export function nextCall(
  limits: AgentLimits,
  used: { calls: number; costMicroUsd: number },
): NextCall {
  if (used.calls >= limits.maxCalls) {
    return { stop: `The task made ${String(used.calls)} calls, the limit of the agent.` };
  }
  const rest = limits.maxCostMicroUsd - used.costMicroUsd;
  if (rest <= 0) {
    return {
      stop: `The task used ${String(used.costMicroUsd)} of its ${String(limits.maxCostMicroUsd)} micro-USD allowance.`,
    };
  }
  return { maxCostMicroUsd: rest };
}

const failedCodes: Record<Exclude<LlmErrorClass, "budget_refused">, ErrorCode> = {
  rate_limit: "rate_limited",
  timeout: "unavailable",
  provider: "unavailable",
  invalid: "internal",
};

function startChatTask(command: StartChatTask): TaskEvent[] {
  const { taskId, runId, callId, message, writer } = command;
  const events: TaskEvent[] = [
    {
      type: "orchestration.TaskSubmitted",
      schemaVersion: 1,
      payload: {
        taskId,
        text: message.text,
        mode: "auto",
        conversationId: message.conversationId,
        messageId: message.messageId,
      },
    },
    { type: "orchestration.RunStarted", schemaVersion: 1, payload: { taskId, runId } },
  ];
  const failed = (code: ErrorCode, text: string): TaskEvent[] => [
    ...events,
    {
      type: "orchestration.RunFailed",
      schemaVersion: 1,
      payload: { taskId, runId, code, reason: reason(text) },
    },
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
      {
        type: "orchestration.RunBlocked",
        schemaVersion: 1,
        payload: { taskId, runId, code: "runaway_stopped", reason: reason(next.stop) },
      },
    ];
  }
  const messages: CallMessage[] = [
    { role: "system", content: prompt.text },
    { role: "user", content: dataBlock(message) },
  ];
  return [
    ...events,
    {
      type: "orchestration.CallRequested",
      schemaVersion: 1,
      payload: {
        taskId,
        callId,
        agentId: agent.agentId,
        agentVersion: agent.version,
        messages,
        models: agent.models,
        maxTokens: agent.limits.maxTokens,
        maxDurationMs: callDurationMs,
        maxCostMicroUsd: next.maxCostMicroUsd,
      },
    },
  ];
}

function refusal(state: TaskState, command: RefuseMessage): string {
  if (state.status === "new") return "The task is not known.";
  if (command.intent === "answer") return "The task has no open question.";
  switch (state.status) {
    case "running":
      return "The task is running and cannot take a message now. Wait for the answer, or send the message as a new task.";
    case "blocked":
      return `The task is blocked until the owner acts. Send the message as a new task. The reason: ${state.reason ?? "not known"}`;
    case "submitted":
      return "The task does not run in the chat. Send the message as a new task.";
    default:
      return "The task has ended. Send the message as a new task.";
  }
}

function refuseMessage(state: TaskState, command: RefuseMessage): TaskEvent[] {
  const { taskId, message, intent, now } = command;
  const events: TaskEvent[] = [];
  let current = state;
  if (
    state.status === "running" &&
    state.runId !== undefined &&
    state.calledAt !== undefined &&
    now.getTime() - Date.parse(state.calledAt) > staleAfterMs
  ) {
    events.push({
      type: "orchestration.RunFailed",
      schemaVersion: 1,
      payload: {
        taskId,
        runId: state.runId,
        code: "unavailable",
        reason: staleReason,
      },
    });
    current = { status: "failed" };
  }
  events.push({
    type: "orchestration.MessageRefused",
    schemaVersion: 1,
    payload: {
      taskId,
      conversationId: message.conversationId,
      messageId: message.messageId,
      intent,
      reason: reason(refusal(current, command)),
    },
  });
  return events;
}

export const isClosed = (state: TaskState): boolean =>
  state.status === "completed" || state.status === "failed" || state.status === "blocked";

const isCurrent = (state: TaskState, callId: string): state is TaskState & { runId: string } =>
  state.status === "running" && state.callId === callId && state.runId !== undefined;

function completeCall(state: TaskState, { call }: CompleteCall): TaskEvent[] {
  if (!isCurrent(state, call.callId)) return [];
  const { taskId } = call;
  const { runId } = state;
  if (call.text.trim() === "") {
    return [
      {
        type: "orchestration.RunFailed",
        schemaVersion: 1,
        payload: { taskId, runId, code: "empty_output", reason: "The model gave an empty answer." },
      },
    ];
  }
  return [
    {
      type: "orchestration.RunCompleted",
      schemaVersion: 1,
      payload: { taskId, runId, summary: call.text, costMicroUsd: call.costMicroUsd },
    },
  ];
}

function failCall(state: TaskState, { call }: FailCall): TaskEvent[] {
  if (call.nextModel !== undefined || !isCurrent(state, call.callId)) return [];
  const { taskId } = call;
  const { runId } = state;
  if (call.errorClass === "budget_refused") {
    return [
      {
        type: "orchestration.RunBlocked",
        schemaVersion: 1,
        payload: {
          taskId,
          runId,
          code: "budget_refused",
          reason: reason(`The budget refused the call: ${call.reason}`),
        },
      },
    ];
  }
  return [
    {
      type: "orchestration.RunFailed",
      schemaVersion: 1,
      payload: {
        taskId,
        runId,
        code: failedCodes[call.errorClass],
        reason: reason(`The model call failed: ${call.reason}`),
      },
    },
  ];
}

export function decide(state: TaskState, command: TaskCommand): TaskEvent[] {
  switch (command.type) {
    case "SubmitTask": {
      if (state.status !== "new") throw new TaskExistsError();
      const { taskId, text, mode } = command;
      return [
        { type: "orchestration.TaskSubmitted", schemaVersion: 1, payload: { taskId, text, mode } },
      ];
    }
    case "StartChatTask":
      if (state.status !== "new") throw new TaskExistsError();
      return startChatTask(command);
    case "RefuseMessage":
      return refuseMessage(state, command);
    case "CompleteCall":
      return completeCall(state, command);
    case "FailCall":
      return failCall(state, command);
  }
}

export function evolve(state: TaskState, event: TaskEvent & { occurredAt?: string }): TaskState {
  switch (event.type) {
    case "orchestration.TaskSubmitted":
      return { status: "submitted" };
    case "orchestration.RunStarted":
      return { status: "running", runId: event.payload.runId };
    case "orchestration.CallRequested":
      return {
        ...state,
        callId: event.payload.callId,
        ...(event.occurredAt === undefined ? {} : { calledAt: event.occurredAt }),
      };
    case "orchestration.RunCompleted":
      return { status: "completed" };
    case "orchestration.RunFailed":
      return { status: "failed" };
    case "orchestration.RunBlocked":
      return { status: "blocked", reason: event.payload.reason };
    case "orchestration.MessageRefused":
      return state;
    default:
      throw new Error(`Unknown event type ${(event as { type: string }).type}`);
  }
}
