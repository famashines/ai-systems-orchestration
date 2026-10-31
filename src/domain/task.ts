import type { TaskMode, TaskSubmittedEventPayloadV1 } from "@famashines/ai-systems-contracts";

export interface SubmitTask {
  type: "SubmitTask";
  taskId: string;
  text: string;
  mode: TaskMode;
}

export type TaskCommand = SubmitTask;

export interface TaskSubmitted {
  type: "orchestration.TaskSubmitted";
  schemaVersion: 1;
  payload: TaskSubmittedEventPayloadV1;
}

export type TaskEvent = TaskSubmitted;

export type TaskState = { status: "new" } | { status: "submitted"; taskId: string; mode: TaskMode };

export class TaskExistsError extends Error {
  constructor() {
    super("The task exists");
    this.name = "TaskExistsError";
  }
}

export const initialState: TaskState = { status: "new" };

export const taskStreamId = (taskId: string): string => `task-${taskId}`;

export function decide(state: TaskState, command: TaskCommand): TaskEvent[] {
  if (state.status !== "new") throw new TaskExistsError();
  const { taskId, text, mode } = command;
  return [
    { type: "orchestration.TaskSubmitted", schemaVersion: 1, payload: { taskId, text, mode } },
  ];
}

type Evolvers = {
  [T in TaskEvent["type"]]: (state: TaskState, event: Extract<TaskEvent, { type: T }>) => TaskState;
};

const evolvers: Evolvers = {
  "orchestration.TaskSubmitted": (_state, event) => ({
    status: "submitted",
    taskId: event.payload.taskId,
    mode: event.payload.mode,
  }),
};

export function evolve(state: TaskState, event: TaskEvent): TaskState {
  if (!Object.hasOwn(evolvers, event.type)) throw new Error(`Unknown event type ${event.type}`);
  return evolvers[event.type](state, event);
}
