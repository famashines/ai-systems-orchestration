import type { Actor, SubmitTaskCommandPayloadV1 } from "@famashines/ai-systems-contracts";
import type { EventStore } from "@famashines/ai-systems-kit/event-store";
import { decide, initialState, taskStreamId } from "../domain/task.ts";

export interface SubmitTaskInput {
  commandId: string;
  actor: Actor;
  payload: SubmitTaskCommandPayloadV1;
}

export interface SubmitTaskAnswer {
  taskId: string;
  version: number;
}

export interface Deps {
  store: EventStore;
  newId: () => string;
}

export async function submitTask(
  { store, newId }: Deps,
  { commandId, actor, payload }: SubmitTaskInput,
): Promise<{ answer: SubmitTaskAnswer; replayed: boolean }> {
  const taskId = newId();
  const events = decide(
    initialState,
    { type: "SubmitTask", taskId, ...payload },
    {
      now: new Date(),
      newId,
      settings: { maxParallel: 1, maxCallsPerTask: 1, retryDelaysMs: [], checkpointTimeoutMs: 0 },
      agents: new Map(),
    },
  );
  const answer = { taskId, version: events.length };
  const result = await store.append({
    streamId: taskStreamId(taskId),
    expectedVersion: 0,
    events: events.map((event) => ({
      ...event,
      correlationId: taskId,
      causationId: commandId,
      actor,
    })),
    command: { commandId, answer },
  });
  return { answer: result.answer ?? answer, replayed: result.replayed };
}
