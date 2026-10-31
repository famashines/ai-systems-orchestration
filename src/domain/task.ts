import type { TaskMode, TaskSubmittedEventPayloadV1 } from "@famashines/ai-systems-contracts";
import { ConflictError } from "@famashines/ai-systems-kit/http";

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

export const initialState: TaskState = { status: "new" };

export const taskStreamId = (taskId: string): string => `task-${taskId}`;

export function decide(state: TaskState, command: TaskCommand): TaskEvent[] {
  if (state.status !== "new") throw new ConflictError("The task exists");
  const { taskId, text, mode } = command;
  return [
    { type: "orchestration.TaskSubmitted", schemaVersion: 1, payload: { taskId, text, mode } },
  ];
}

export function evolve(_state: TaskState, event: TaskEvent): TaskState {
  return { status: "submitted", taskId: event.payload.taskId, mode: event.payload.mode };
}
