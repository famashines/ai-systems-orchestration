import type { RegisteredEvent } from "@famashines/ai-systems-contracts";
import type { HandlerContext } from "@famashines/ai-systems-kit/messaging";
import { isClosed, type AgentLoad } from "../domain/task.ts";
import { act, system, type TaskDeps } from "./run.ts";

export interface ChatDeps extends TaskDeps {
  loadWriter: () => Promise<AgentLoad>;
}

export async function onOwnerMessage(
  deps: ChatDeps,
  event: RegisteredEvent,
  { markProcessed }: HandlerContext,
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
    await act(deps, {
      taskId,
      causationId: event.eventId,
      command: { type: "StartChatTask", taskId, runId, callId, message, writer },
      actorOf: (created) => (created.type === "orchestration.TaskSubmitted" ? event.actor : system),
      also: markProcessed,
    });
    return;
  }
  const { taskId, intent } = payload;
  await act(deps, {
    taskId,
    causationId: event.eventId,
    command: { type: "TakeMessage", taskId, message, intent },
    also: markProcessed,
  });
}

export async function onLlmEvent(
  deps: TaskDeps,
  event: RegisteredEvent,
  { markProcessed }: HandlerContext,
): Promise<void> {
  if (event.type !== "llm.CallCompleted" && event.type !== "llm.CallFailed") return;
  const { taskId, callId, costMicroUsd } = event.payload;
  const { state, events } = await act(deps, {
    taskId,
    causationId: event.eventId,
    command:
      event.type === "llm.CallCompleted"
        ? { type: "CompleteCall", call: event.payload }
        : { type: "FailCall", call: event.payload },
    also: markProcessed,
  });
  if (events.length === 0 && isClosed(state)) {
    deps.logger.warn(
      { taskId, callId, costMicroUsd, type: event.type },
      "a late call event for a closed task is dropped",
    );
  }
}
