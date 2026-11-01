import { setTimeout as sleep } from "node:timers/promises";
import type { Migration } from "@famashines/ai-systems-kit/mongo";
import { MongoServerError, type ClientSession, type Db } from "mongodb";
import type { TimerEvent } from "../application/run.ts";
import type { WarnLogger } from "./writer.ts";

const retrying = "orchestration.SubtaskRetrying";
const checkpoint = "orchestration.CheckpointOpened";
const lookbackMs = 45 * 86_400_000;
const marginMs = 10 * 60_000;

export const timersMigration: Migration = {
  name: "001-timers",
  up: async (db) => {
    const events = db.collection("events");
    await events.createIndex(
      { "payload.retryAt": 1 },
      { name: "timer_retry_at", partialFilterExpression: { type: retrying } },
      undefined,
    );
    await events.createIndex(
      { "payload.deadline": 1 },
      { name: "timer_deadline", partialFilterExpression: { type: checkpoint } },
      undefined,
    );
  },
};

interface TimerDoc extends TimerEvent {
  payload: TimerEvent["payload"] & { retryAt?: string; deadline?: string };
}

export interface TimersOptions {
  db: Db;
  intervalMs: number;
  logger: WarnLogger;
  handle: (timer: TimerEvent, also: (session: ClientSession) => Promise<void>) => Promise<boolean>;
  now?: () => Date;
}

export function startTimers({
  db,
  intervalMs,
  logger,
  handle,
  now = () => new Date(),
}: TimersOptions) {
  const events = db.collection<TimerDoc>("events");
  const processed = db.collection<{ _id: string; processedAt: Date }>("processed");
  const stop = new AbortController();
  let since = new Date(now().getTime() - lookbackMs).toISOString();

  const mark = (eventId: string, session?: ClientSession) =>
    processed.insertOne({ _id: eventId, processedAt: new Date() }, session ? { session } : {});

  async function poll(): Promise<void> {
    const at = now();
    const until = at.toISOString();
    const due = await events
      .find(
        {
          $or: [
            { type: retrying, "payload.retryAt": { $gt: since, $lte: until } },
            { type: checkpoint, "payload.deadline": { $gt: since, $lte: until } },
          ],
        },
        { projection: { type: 1, payload: 1 } },
      )
      .toArray();
    const done = new Set(
      (await processed.find({ _id: { $in: due.map((timer) => timer._id) } }).toArray()).map(
        (marker) => marker._id,
      ),
    );
    let next = new Date(at.getTime() - marginMs).toISOString();
    for (const timer of due) {
      if (done.has(timer._id) || stop.signal.aborted) continue;
      try {
        if (!(await handle(timer, (session) => mark(timer._id, session).then(() => undefined)))) {
          await mark(timer._id).catch((error: unknown) => {
            if (!(error instanceof MongoServerError && error.code === 11000)) throw error;
          });
        }
      } catch (error) {
        logger.warn({ err: error, eventId: timer._id }, "a timer failed; it runs again");
        const dueAt = timer.payload.retryAt ?? timer.payload.deadline ?? until;
        const before = new Date(Date.parse(dueAt) - 1).toISOString();
        if (before < next) next = before;
      }
    }
    if (next > since) since = next;
  }

  const loop = (async () => {
    while (!stop.signal.aborted) {
      try {
        await poll();
      } catch (error) {
        logger.warn({ err: error }, "the timer poll failed; it runs again");
      }
      await sleep(intervalMs, undefined, { signal: stop.signal }).catch(() => undefined);
    }
  })();

  return {
    close: async () => {
      stop.abort();
      await loop;
    },
  };
}
