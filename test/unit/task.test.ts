import assert from "node:assert/strict";
import { test } from "node:test";
import { ConflictError } from "@famashines/ai-systems-kit/http";
import {
  decide,
  evolve,
  initialState,
  taskStreamId,
  type TaskCommand,
  type TaskEvent,
} from "../../src/domain/task.ts";

const taskId = "00000000-0000-4000-8000-000000000001";

const submit: TaskCommand = { type: "SubmitTask", taskId, text: "Write a poem", mode: "auto" };

const submitted: TaskEvent = {
  type: "orchestration.TaskSubmitted",
  schemaVersion: 1,
  payload: { taskId, text: "Write a poem", mode: "auto" },
};

const given = (events: TaskEvent[]) => ({
  when: (command: TaskCommand) => () => decide(events.reduce(evolve, initialState), command),
});

test("given no events, when SubmitTask, then TaskSubmitted", () => {
  assert.deepEqual(given([]).when(submit)(), [submitted]);
});

test("given TaskSubmitted, when SubmitTask, then a conflict", () => {
  assert.throws(given([submitted]).when(submit), ConflictError);
});

test("TaskSubmitted gives the submitted state", () => {
  assert.deepEqual(evolve(initialState, submitted), { status: "submitted", taskId, mode: "auto" });
});

test("the stream id is task-<taskId>", () => {
  assert.equal(taskStreamId(taskId), `task-${taskId}`);
});
