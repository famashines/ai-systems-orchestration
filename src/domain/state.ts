import type {
  ActiveAgentVersionResultV1,
  AgentLimits,
  CallKind,
  CheckpointOutcome,
  ErrorCode,
  LlmErrorClass,
  PlanSubtask,
  PromptVersionResultV1,
  RegisteredEvent,
  SubtaskStatus,
  TaskMode,
  TaskStatus as ContractTaskStatus,
} from "@famashines/ai-systems-contracts";

type Bare<E> = E extends {
  type: infer T extends `orchestration.${string}`;
  schemaVersion: infer V;
  payload: infer P;
}
  ? { type: T; schemaVersion: V; payload: P }
  : never;

export type TaskEvent = Bare<RegisteredEvent>;
export type HistoryEvent = TaskEvent & { occurredAt?: string };

export type TaskStatus = "new" | ContractTaskStatus;

export interface Writer {
  agent: ActiveAgentVersionResultV1;
  prompt: PromptVersionResultV1;
}

export interface LoadFailure {
  code: ErrorCode;
  reason: string;
}

export type AgentLoad = Writer | LoadFailure;

export interface Checkpoint {
  checkpointId: string;
  status: "open" | CheckpointOutcome;
  deadline?: string;
}

export interface SubtaskState {
  status: SubtaskStatus;
  tries: number;
  nextTry?: number;
  retryAt?: string;
  callId?: string;
  output?: { callId: string; text: string };
  steers?: number;
  failure?: { code: ErrorCode; reason: string };
  checkpoint?: Checkpoint;
}

export interface CallState {
  kind?: CallKind;
  subtaskId?: string;
  agentId: string;
  maxCostMicroUsd?: number;
  settled: boolean;
  cancelRequested: boolean;
}

export interface TaskState {
  taskId: string;
  status: TaskStatus;
  mode?: TaskMode;
  text?: string;
  conversationId?: string;
  messageId?: string;
  plan?: { planVersion: number; subtasks: PlanSubtask[]; approved: boolean };
  runId?: string;
  callId?: string;
  calledAt?: string;
  reason?: string;
  blockCode?: ErrorCode;
  subtasks: Readonly<Record<string, SubtaskState>>;
  calls: Readonly<Record<string, CallState>>;
  costMicroUsd: number;
  steers: readonly { messageId: string; text: string }[];
}

export class TaskExistsError extends Error {
  constructor() {
    super("The task exists");
    this.name = "TaskExistsError";
  }
}

export class TaskConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TaskConflictError";
  }
}

export const initialState: TaskState = {
  taskId: "",
  status: "new",
  subtasks: {},
  calls: {},
  costMicroUsd: 0,
  steers: [],
};

export const taskStreamId = (taskId: string): string => `task-${taskId}`;

export const reason = (text: string): string => text.slice(0, 1000);

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

export const failedCodes: Record<Exclude<LlmErrorClass, "budget_refused">, ErrorCode> = {
  rate_limit: "rate_limited",
  timeout: "unavailable",
  provider: "unavailable",
  invalid: "internal",
  cancelled: "internal",
};

export const isRunActive = (state: TaskState): boolean =>
  state.runId !== undefined &&
  (state.status === "running" || state.status === "paused" || state.status === "blocked");

export const runningCalls = (state: TaskState): string[] =>
  Object.entries(state.calls)
    .filter(([, call]) => !call.settled && !call.cancelRequested)
    .map(([callId]) => callId);

const endedRun: Partial<Record<SubtaskStatus, SubtaskStatus>> = {
  running: "cancelled",
  queued: "cancelled",
  retrying: "cancelled",
  awaiting_approval: "cancelled",
  waiting: "skipped",
};

function endRun(state: TaskState, status: "failed" | "cancelled"): TaskState {
  const subtasks = Object.fromEntries(
    Object.entries(state.subtasks).map(([id, subtask]) => [
      id,
      { ...subtask, status: endedRun[subtask.status] ?? subtask.status },
    ]),
  );
  return { ...endChatCall(state), status, subtasks };
}

function endChatCall(state: TaskState): TaskState {
  const call = state.callId === undefined ? undefined : state.calls[state.callId];
  if (state.callId === undefined || call === undefined) return state;
  return { ...state, calls: { ...state.calls, [state.callId]: { ...call, settled: true } } };
}

function withSubtask(
  state: TaskState,
  subtaskId: string,
  change: (subtask: SubtaskState) => SubtaskState,
): TaskState {
  const subtask = state.subtasks[subtaskId] ?? { status: "waiting", tries: 0 };
  return { ...state, subtasks: { ...state.subtasks, [subtaskId]: change(subtask) } };
}

function withCall(state: TaskState, callId: string, change: Partial<CallState>): TaskState {
  const call = state.calls[callId];
  if (call === undefined) return state;
  return { ...state, calls: { ...state.calls, [callId]: { ...call, ...change } } };
}

export function evolve(state: TaskState, event: HistoryEvent): TaskState {
  switch (event.type) {
    case "orchestration.TaskSubmitted": {
      const { taskId, mode, text, conversationId, messageId } = event.payload;
      return {
        ...initialState,
        taskId,
        status: "submitted",
        mode,
        text,
        ...(conversationId === undefined ? {} : { conversationId }),
        ...(messageId === undefined ? {} : { messageId }),
      };
    }
    case "orchestration.ClarificationRequested":
      return { ...state, status: "clarifying" };
    case "orchestration.ClarificationAnswered":
      return { ...state, status: "planning" };
    case "orchestration.PlanProposed": {
      const { planVersion, subtasks, awaitsApproval } = event.payload;
      return {
        ...state,
        status: awaitsApproval ? "awaiting_approval" : "planning",
        plan: { planVersion, subtasks, approved: false },
      };
    }
    case "orchestration.PlanEdited": {
      const { planVersion, subtasks } = event.payload;
      return {
        ...state,
        status: "awaiting_approval",
        plan: { planVersion, subtasks, approved: false },
      };
    }
    case "orchestration.PlanApproved":
      return state.plan?.planVersion === event.payload.planVersion
        ? { ...state, plan: { ...state.plan, approved: true } }
        : state;
    case "orchestration.PlanRejected":
      return { ...state, status: "rejected" };
    case "orchestration.TaskCancelled":
      return { ...state, status: "cancelled" };
    case "orchestration.TaskModeChanged":
      return { ...state, mode: event.payload.mode };
    case "orchestration.RunStarted":
      return { ...state, status: "running", runId: event.payload.runId };
    case "orchestration.SubtaskWaiting":
      return withSubtask(state, event.payload.subtaskId, (subtask) => ({
        ...subtask,
        status: "waiting",
      }));
    case "orchestration.SubtaskQueued":
      return withSubtask(state, event.payload.subtaskId, ({ checkpoint, steers }) => ({
        status: "queued",
        tries: 0,
        ...(checkpoint === undefined ? {} : { checkpoint }),
        ...(steers === undefined ? {} : { steers }),
      }));
    case "orchestration.CheckpointOpened": {
      const { checkpointId, deadline } = event.payload;
      return withSubtask(state, event.payload.subtaskId, (subtask) => ({
        ...subtask,
        status: "awaiting_approval",
        checkpoint: {
          checkpointId,
          status: "open",
          ...(deadline === undefined ? {} : { deadline }),
        },
      }));
    }
    case "orchestration.CheckpointResolved": {
      const { checkpointId, decision } = event.payload;
      const found = Object.entries(state.subtasks).find(
        ([, subtask]) => subtask.checkpoint?.checkpointId === checkpointId,
      );
      if (found === undefined) return state;
      return withSubtask(state, found[0], (subtask) => ({
        ...subtask,
        ...(decision === "approved" ? { status: "queued" } : {}),
        checkpoint: { ...(subtask.checkpoint ?? { checkpointId }), status: decision },
      }));
    }
    case "orchestration.SubtaskStarted": {
      const { tryNumber, callId } = event.payload;
      return withSubtask(state, event.payload.subtaskId, (subtask) => ({
        ...subtask,
        status: "running",
        tries: tryNumber,
        callId,
        steers: subtask.steers ?? state.steers.length,
      }));
    }
    case "orchestration.SubtaskCompleted": {
      const { callId, text } = event.payload;
      return withSubtask(state, event.payload.subtaskId, (subtask) => ({
        ...subtask,
        status: "done",
        output: { callId, text },
      }));
    }
    case "orchestration.SubtaskFailed": {
      const { tryNumber, code, reason: why } = event.payload;
      return withSubtask(state, event.payload.subtaskId, (subtask) => ({
        ...subtask,
        status: "failed",
        tries: tryNumber,
        failure: { code, reason: why },
      }));
    }
    case "orchestration.SubtaskRetrying": {
      const { tryNumber, retryAt } = event.payload;
      return withSubtask(state, event.payload.subtaskId, (subtask) => ({
        ...subtask,
        status: "retrying",
        nextTry: tryNumber,
        retryAt,
      }));
    }
    case "orchestration.SubtaskBlocked": {
      const { code, reason: why } = event.payload;
      return withSubtask(state, event.payload.subtaskId, (subtask) => ({
        ...subtask,
        status: "blocked",
        failure: { code, reason: why },
      }));
    }
    case "orchestration.SubtaskReassigned": {
      const { subtaskId, agentId, model } = event.payload;
      if (state.plan === undefined) return state;
      const subtasks = state.plan.subtasks.map((subtask) => {
        if (subtask.subtaskId !== subtaskId) return subtask;
        const next: PlanSubtask = { ...subtask, agentId };
        if (model === undefined) delete next.model;
        else next.model = model;
        return next;
      });
      return { ...state, plan: { ...state.plan, subtasks } };
    }
    case "orchestration.RunPaused":
      return { ...state, status: "paused" };
    case "orchestration.RunResumed":
      return { ...state, status: "running" };
    case "orchestration.RunBlocked":
      return {
        ...endChatCall(state),
        status: "blocked",
        reason: event.payload.reason,
        blockCode: event.payload.code,
      };
    case "orchestration.RunCompleted":
      return { ...endChatCall(state), status: "completed" };
    case "orchestration.RunFailed":
      return endRun(state, "failed");
    case "orchestration.RunCancelled":
      return endRun(state, "cancelled");
    case "orchestration.CallRequested": {
      const { callId, kind, subtaskId, agentId, maxCostMicroUsd } = event.payload;
      const calls = {
        ...state.calls,
        [callId]: {
          agentId,
          ...(maxCostMicroUsd === undefined ? {} : { maxCostMicroUsd }),
          settled: false,
          cancelRequested: false,
          ...(kind === undefined ? {} : { kind }),
          ...(subtaskId === undefined ? {} : { subtaskId }),
        },
      };
      if (kind !== undefined) return { ...state, calls };
      return {
        ...state,
        calls,
        callId,
        ...(event.occurredAt === undefined ? {} : { calledAt: event.occurredAt }),
      };
    }
    case "orchestration.CallSettled":
      return {
        ...withCall(state, event.payload.callId, { settled: true }),
        costMicroUsd: state.costMicroUsd + event.payload.costMicroUsd,
      };
    case "orchestration.CallCancelRequested":
      return withCall(state, event.payload.callId, { cancelRequested: true });
    case "orchestration.SteeringMessageAdded": {
      const { messageId, text } = event.payload;
      return { ...state, steers: [...state.steers, { messageId, text }] };
    }
    case "orchestration.MessageRefused":
      return state;
    default:
      throw new Error(`Unknown event type ${(event as { type: string }).type}`);
  }
}

export const fold = (state: TaskState, events: readonly HistoryEvent[]): TaskState =>
  events.reduce(evolve, state);
