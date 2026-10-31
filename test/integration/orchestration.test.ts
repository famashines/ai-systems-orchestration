import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { after, before, test } from "node:test";
import { jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";
import { loadConfig } from "@famashines/ai-systems-kit/config";
import { createEventStore } from "@famashines/ai-systems-kit/event-store";
import { connectNats, type Nats } from "@famashines/ai-systems-kit/messaging";
import { connectMongo, type Mongo } from "@famashines/ai-systems-kit/mongo";
import { createLogger } from "@famashines/ai-systems-kit/observability";
import {
  createStreams,
  startMongo,
  startNats,
  waitFor,
  type Started,
} from "@famashines/ai-systems-kit/testing";
import { envSchema, start } from "../../src/main.ts";

const orchestrationGrants = [
  "orchestration.>",
  "rpc.budget.reserve",
  "rpc.budget.settle",
  "dlq.orchestration.>",
  "$JS.API.INFO",
  "$JS.API.STREAM.INFO.ORCHESTRATION",
  "$JS.API.STREAM.INFO.CONVERSATION",
  "$JS.API.CONSUMER.CREATE.CONVERSATION.orchestration.conversation.OwnerMessagePosted",
  "$JS.API.CONSUMER.INFO.CONVERSATION.orchestration",
  "$JS.API.CONSUMER.MSG.NEXT.CONVERSATION.orchestration",
  "$JS.ACK.CONVERSATION.orchestration.>",
  "$JS.API.STREAM.INFO.LLM",
  "$JS.API.CONSUMER.CREATE.LLM.orchestration.llm.>",
  "$JS.API.CONSUMER.INFO.LLM.orchestration",
  "$JS.API.CONSUMER.MSG.NEXT.LLM.orchestration",
  "$JS.ACK.LLM.orchestration.>",
];

const adminGrants = [
  "$JS.API.>",
  "$JS.ACK.>",
  "orchestration.>",
  "conversation.>",
  "llm.>",
  "budget.>",
  "agents.>",
  "prompts.>",
  "dlq.>",
];

const list = (subjects: string[]): string => subjects.map((s) => `"${s}"`).join(", ");

const natsConfig = `
jetstream { store_dir: /data }
accounts {
  AISYSTEMS {
    jetstream { max_file: 1073741824, max_memory: 0 }
    users = [
      {
        user: ai-systems-admin, password: admin
        permissions {
          publish { allow: [${list(adminGrants)}] }
          subscribe { allow: ["_INBOX_admin.>"] }
        }
      }
      {
        user: ai-systems-orchestration, password: orchestration
        permissions {
          publish { allow: [${list(orchestrationGrants)}] }
          subscribe { allow: ["_INBOX_orchestration.>"] }
        }
      }
    ]
  }
}
`;

const logger = createLogger({ service: "test", release: "test", level: "silent" });
let mongoContainer: Started;
let natsContainer: Started;
let mongo: Mongo;
let admin: Nats;
let manager: JetStreamManager;
let running: Awaited<ReturnType<typeof start>> | undefined;
let base = "";

const startService = async (): Promise<void> => {
  const config = loadConfig(envSchema, {
    MONGODB_URI: mongoContainer.url,
    NATS_SERVERS: natsContainer.url,
    NATS_USER: "ai-systems-orchestration",
    NATS_PASSWORD: "orchestration",
    LOG_LEVEL: "silent",
  });
  running = await start({ ...config, PORT: 0 }, { drainDelayMs: 0 });
  base = `http://127.0.0.1:${String(running.address.port)}`;
  await waitFor(async () => (await fetch(`${base}/ready`)).ok);
};

const stopService = async (): Promise<void> => {
  const stopping = running;
  running = undefined;
  if (stopping) assert.equal(await stopping.app.close(), true);
};

before(async () => {
  [mongoContainer, natsContainer] = await Promise.all([
    startMongo(),
    startNats({ config: natsConfig }),
  ]);
  mongo = await connectMongo({ url: mongoContainer.url, database: "orchestration" });
  admin = await connectNats({
    servers: natsContainer.url,
    user: "ai-systems-admin",
    pass: "admin",
    service: "admin",
    logger,
  });
  manager = await jetstreamManager(admin.connection);
  await createStreams(admin.connection, [{ name: "ORCHESTRATION", subjects: ["orchestration.>"] }]);
  await startService();
});

after(async () => {
  await stopService();
  await admin.close();
  await mongo.close();
  await Promise.all([mongoContainer.stop(), natsContainer.stop()]);
});

const events = () => mongo.db.collection<{ _id: string; publishedAt: Date | null }>("events");

const submit = (commandId: string, text = "Write a poem") =>
  fetch(`${base}/internal/commands/submit-task`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      commandId,
      type: "orchestration.SubmitTask",
      actor: { kind: "owner", id: "owner-1" },
      payload: { text, mode: "auto" },
    }),
  });

const streamMessages = async (eventId: string) => {
  const { state } = await manager.streams.info("ORCHESTRATION");
  const found = [];
  for (let seq = state.first_seq; seq <= state.last_seq && state.messages > 0; seq += 1) {
    const message = await manager.streams.getMessage("ORCHESTRATION", { seq });
    if (message?.header.get("Nats-Msg-Id") === eventId) found.push(message);
  }
  return found;
};

const published = async (eventId: string): Promise<void> => {
  await waitFor(async () => (await events().findOne({ _id: eventId }))?.publishedAt != null);
};

test("the service is ready when MongoDB answers and the migrations are done", async () => {
  const response = await fetch(`${base}/ready`);
  assert.equal(response.status, 200);
  await mongo.db.collection<{ _id: string }>("migrations").deleteOne({ _id: "000-event-store" });
  const missing = await fetch(`${base}/ready`);
  assert.equal(missing.status, 503);
  assert.deepEqual(((await missing.json()) as { failing: string[] }).failing, ["migrations"]);
  await mongo.db
    .collection<{ _id: string; appliedAt: Date }>("migrations")
    .insertOne({ _id: "000-event-store", appliedAt: new Date() });
  assert.equal((await fetch(`${base}/ready`)).status, 200);
});

test("a submitted task is in MongoDB once and in ORCHESTRATION once", async () => {
  const response = await submit(randomUUID());
  assert.equal(response.status, 201);
  const { taskId } = (await response.json()) as { taskId: string };
  const stored = await events()
    .find({ streamId: `task-${taskId}` })
    .toArray();
  assert.equal(stored.length, 1);
  const eventId = stored[0]?._id ?? "";
  await published(eventId);
  const messages = await streamMessages(eventId);
  assert.equal(messages.length, 1);
  const [message] = messages;
  assert.ok(message);
  assert.equal(message.subject, "orchestration.TaskSubmitted");
  const envelope = message.json<{ eventId: string; payload: { taskId: string } }>();
  assert.equal(envelope.eventId, eventId);
  assert.equal(envelope.payload.taskId, taskId);
  assert.equal(message.header.get("correlation-id"), taskId);
});

test("the same commandId twice gives one event and the same answer", async () => {
  const commandId = randomUUID();
  const first = await submit(commandId);
  const second = await submit(commandId, "Another text");
  assert.equal(first.status, 201);
  assert.equal(second.status, 200);
  const answer = (await first.json()) as { taskId: string };
  assert.deepEqual(await second.json(), answer);
  assert.equal(await events().countDocuments({ causationId: commandId }), 1);
  const [stored] = await events().find({ causationId: commandId }).toArray();
  await published(stored?._id ?? "");
  assert.equal((await streamMessages(stored?._id ?? "")).length, 1);
});

test("the replay API gives a page of events in position order", async () => {
  await submit(randomUUID());
  const all = (await (await fetch(`${base}/internal/events?limit=1000`)).json()) as {
    events: { position: number; eventId: string }[];
  };
  assert.ok(all.events.length >= 3);
  assert.deepEqual(
    all.events.map((event) => event.position),
    all.events.map((_, index) => index + 1),
  );
  const page = (await (await fetch(`${base}/internal/events?afterPosition=1&limit=2`)).json()) as {
    events: unknown[];
  };
  assert.deepEqual(page.events, all.events.slice(1, 3));
});

test("without the stream the service stays ready and the event waits in the outbox", async () => {
  await manager.streams.delete("ORCHESTRATION");
  const response = await submit(randomUUID());
  assert.equal(response.status, 201);
  const { taskId } = (await response.json()) as { taskId: string };
  assert.equal((await fetch(`${base}/ready`)).status, 200);
  const [stored] = await events()
    .find({ streamId: `task-${taskId}` })
    .toArray();
  const eventId = stored?._id ?? "";
  assert.equal(stored?.publishedAt, null);
  await createStreams(admin.connection, [{ name: "ORCHESTRATION", subjects: ["orchestration.>"] }]);
  await published(eventId);
  assert.equal((await streamMessages(eventId)).length, 1);
});

test("after a restart the service publishes the pending events", async () => {
  await stopService();
  const store = createEventStore(mongo.db);
  const taskId = randomUUID();
  const { events: appended } = await store.append({
    streamId: `task-${taskId}`,
    expectedVersion: 0,
    events: [
      {
        type: "orchestration.TaskSubmitted",
        schemaVersion: 1,
        correlationId: taskId,
        causationId: randomUUID(),
        actor: { kind: "owner", id: "owner-1" },
        payload: { taskId, text: "Written before the restart", mode: "auto" },
      },
    ],
  });
  const eventId = appended[0]?.eventId ?? "";
  assert.equal((await events().findOne({ _id: eventId }))?.publishedAt, null);
  await startService();
  await published(eventId);
  assert.equal((await streamMessages(eventId)).length, 1);
});
