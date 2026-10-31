import assert from "node:assert/strict";
import { test } from "node:test";
import { ConcurrencyError } from "@famashines/ai-systems-kit/event-store";
import { createMemoryEventStore, fakeIds } from "@famashines/ai-systems-kit/testing";
import { submitTask } from "../../src/application/submit-task.ts";

const taskId = "00000000-0000-4000-8000-0000000000aa";
const actor = { kind: "owner", id: "owner-1" } as const;
const payload = { text: "Write a poem", mode: "auto" } as const;

test("given a TaskSubmitted stream, when SubmitTask with its task id, then ConcurrencyError", async () => {
  const store = createMemoryEventStore({ newId: fakeIds(1) });
  await store.append({
    streamId: `task-${taskId}`,
    events: [
      {
        type: "orchestration.TaskSubmitted",
        schemaVersion: 1,
        correlationId: taskId,
        causationId: taskId,
        actor,
        payload: { taskId, ...payload },
      },
    ],
  });
  const commandId = "00000000-0000-4000-8000-0000000000bb";
  await assert.rejects(
    submitTask({ store, newId: () => taskId }, { commandId, actor, payload }),
    ConcurrencyError,
  );
  assert.equal((await store.readStream(`task-${taskId}`)).length, 1);
});
