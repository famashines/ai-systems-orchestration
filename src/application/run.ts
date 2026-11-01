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
  toLoadBefore,
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
  loadAgentByName: (name: string) => Promise<AgentLoad>;
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
  const agents = new Map<string, AgentLoad>();
  const loadAll = (ids: string[], load: (id: string) => Promise<AgentLoad>) =>
    Promise.all(ids.map(async (id) => [id, await load(id)] as const));
  const before = toLoadBefore(state, command, deps.settings);
  const [byId, byName] = await Promise.all([
    loadAll(before.agentIds, deps.loadAgent),
    loadAll(before.names, deps.loadAgentByName),
  ]);
  for (const [id, load] of byId) agents.set(id, load);
  for (const [, load] of byName) if ("agent" in load) agents.set(load.agent.agentId, load);
  const env: RunEnv = {
    now: deps.now(),
    newId: deps.newId,
    settings: deps.settings,
    agents,
    named: new Map(byName),
  };
  const decided = decide(state, command, env);
  const after = fold(state, decided);
  const missing = agentsToLoad(after, env).filter((agentId) => !agents.has(agentId));
  for (const [id, load] of await loadAll(missing, deps.loadAgent)) agents.set(id, load);
  const events = [...decided, ...schedule(after, env)];
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
