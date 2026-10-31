import type { RegisteredEvent } from "@famashines/ai-systems-contracts";
import type { EventStore } from "@famashines/ai-systems-kit/event-store";
import type { HandlerContext } from "@famashines/ai-systems-kit/messaging";
import type { WarnLogger } from "../adapters/writer.ts";
import {
  decide,
  isClosed,
  evolve,
  initialState,
  taskStreamId,
  type LoadFailure,
  type TaskCommand,
  type TaskEvent,
  type TaskState,
  type Writer,
} from "../domain/task.ts";

export interface ChatDeps {
  store: EventStore;
  newId: () => string;
  loadWriter: () => Promise<Writer | LoadFailure>;
  now: () => Date;
  logger: WarnLogger;
}

const system = { kind: "system", id: "orchestration" } as const;

async function decideAndAppend(
  { store }: ChatDeps,
  source: RegisteredEvent,
  taskId: string,
  command: TaskCommand,
  { markProcessed }: HandlerContext,
): Promise<TaskState> {
  const streamId = taskStreamId(taskId);
  const history = (await store.readStream(streamId)) as unknown as (TaskEvent & {
    occurredAt: string;
  })[];
  const state = history.reduce(evolve, initialState);
  const events = decide(state, command);
  if (events.length === 0) return state;
  await store.append({
    streamId,
    expectedVersion: history.length,
    events: events.map((event) => ({
      ...event,
      correlationId: taskId,
      causationId: source.eventId,
      actor: event.type === "orchestration.TaskSubmitted" ? source.actor : system,
    })),
    also: markProcessed,
  });
  return state;
}

export async function onOwnerMessage(
  deps: ChatDeps,
  event: RegisteredEvent,
  context: HandlerContext,
): Promise<void> {
  if (event.type !== "conversation.OwnerMessagePosted") return;
  const { payload } = event;
  const message = {
    conversationId: payload.conversationId,
    messageId: payload.messageId,
    text: payload.text,
  };
  if (payload.intent === "new_task") {
    const writer = await deps.loadWriter();
    const taskId = deps.newId();
    const runId = deps.newId();
    const callId = deps.newId();
    await decideAndAppend(
      deps,
      event,
      taskId,
      { type: "StartChatTask", taskId, runId, callId, message, writer },
      context,
    );
    return;
  }
  const { taskId, intent } = payload;
  await decideAndAppend(
    deps,
    event,
    taskId,
    { type: "RefuseMessage", taskId, message, intent, now: deps.now() },
    context,
  );
}

export async function onLlmEvent(
  deps: ChatDeps,
  event: RegisteredEvent,
  context: HandlerContext,
): Promise<void> {
  if (event.type !== "llm.CallCompleted" && event.type !== "llm.CallFailed") return;
  const { taskId, callId, costMicroUsd } = event.payload;
  const state = await decideAndAppend(
    deps,
    event,
    taskId,
    event.type === "llm.CallCompleted"
      ? { type: "CompleteCall", call: event.payload }
      : { type: "FailCall", call: event.payload },
    context,
  );
  if (isClosed(state)) {
    deps.logger.warn(
      { taskId, callId, costMicroUsd, type: event.type },
      "a late call event for a closed task is dropped",
    );
  }
}
