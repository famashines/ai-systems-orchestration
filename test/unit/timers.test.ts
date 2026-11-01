import assert from "node:assert/strict";
import { test } from "node:test";
import { windowStart } from "../../src/adapters/timers.ts";

test("the timer window keeps its start, and moves back after a backward clock step", () => {
  const since = "2026-11-01T10:00:00.000Z";
  const at = (iso: string) => new Date(iso);
  assert.equal(windowStart(since, at("2026-11-01T10:10:00.000Z")), since);
  assert.equal(windowStart(since, at("2026-11-01T11:00:00.000Z")), since);
  assert.equal(windowStart(since, at("2026-11-01T10:05:00.000Z")), "2026-11-01T09:55:00.000Z");
  assert.equal(windowStart(since, at("2026-11-01T09:00:00.000Z")), "2026-11-01T08:50:00.000Z");
});
