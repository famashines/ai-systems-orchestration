import type { Actor } from "@famashines/ai-systems-contracts";
import type { EventStore } from "@famashines/ai-systems-kit/event-store";
import { NotFoundError } from "@famashines/ai-systems-kit/http";
import type { ClientSession } from "mongodb";
import type { WarnLogger } from "../adapters/writer.ts";
import {
  agentsToLoad,
  decide,
  fold,
  initialState,
  schedule,
  taskStreamId,
  type AgentLoad,
  type HistoryEvent,
  type RunEnv,
  type RunSettings,
  type TaskCommand,
  type TaskEvent,
  type TaskState,
} from "../domain/task.ts";

export interface TaskDeps {
  store: EventStore;
  newId: () => string;
  now: () => Date;
  logger: WarnLogger;
  settings: RunSettings;
  loadAgent: (agentId: string) => Promise<AgentLoad>;
}

export const system: Actor = { kind: "system", id: "orchestration" };

export interface Act {
  taskId: string;
  causationId: string;
  command: TaskCommand;
  actorOf?: (event: TaskEvent, decided: boolean) => Actor;
  also?: (session: ClientSession) => Promise<void>;
  idempotent?: boolean;
}

export interface Answer {
  taskId: string;
  version: number;
}

export interface Acted {
  state: TaskState;
  events: TaskEvent[];
  answer: Answer;
  replayed: boolean;
}

export async function act(
  deps: TaskDeps,
  { taskId, causationId, command, actorOf = () => system, also, idempotent = false }: Act,
): Promise<Acted> {
  const streamId = taskStreamId(taskId);
  const history = (await deps.store.readStream(streamId)) as unknown as (HistoryEvent & {
    version: number;
    causationId: string;
  })[];
  const state = fold(initialState, history);
  if (idempotent) {
    if (history.length === 0) throw new NotFoundError("The task is not known");
    const earlier = history.findLast((stored) => stored.causationId === causationId);
    if (earlier !== undefined) {
      return { state, events: [], answer: { taskId, version: earlier.version }, replayed: true };
    }
  }
  const env: RunEnv = {
    now: deps.now(),
    newId: deps.newId,
    settings: deps.settings,
    agents: new Map(),
  };
  const decided = decide(state, command, env);
  const after = fold(state, decided);
  const loads = await Promise.all(
    agentsToLoad(after, env).map(
      async (agentId) => [agentId, await deps.loadAgent(agentId)] as const,
    ),
  );
  const events = [...decided, ...schedule(after, { ...env, agents: new Map(loads) })];
  const answer = { taskId, version: history.length + events.length };
  if (events.length === 0) return { state, events, answer, replayed: false };
  const result = await deps.store.append({
    streamId,
    expectedVersion: history.length,
    events: events.map((event, index) => ({
      ...event,
      correlationId: taskId,
      causationId,
      actor: actorOf(event, index < decided.length),
    })),
    ...(idempotent ? { command: { commandId: causationId, answer } } : {}),
    ...(also === undefined ? {} : { also }),
  });
  return result.replayed
    ? { state, events: [], answer: result.answer ?? answer, replayed: true }
    : { state, events, answer, replayed: false };
}

export interface TimerEvent {
  _id: string;
  type: string;
  payload: { taskId: string; subtaskId: string; checkpointId?: string };
}

export async function onTimer(
  deps: TaskDeps,
  timer: TimerEvent,
  also: (session: ClientSession) => Promise<void>,
): Promise<boolean> {
  const { taskId, subtaskId, checkpointId } = timer.payload;
  const command: TaskCommand =
    checkpointId === undefined
      ? { type: "RetryDue", subtaskId }
      : { type: "CheckpointDue", checkpointId };
  const { events } = await act(deps, { taskId, causationId: timer._id, command, also });
  return events.length > 0;
}
