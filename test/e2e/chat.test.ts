import assert from "node:assert/strict";
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync } from "node:fs";
import { createServer } from "node:net";
import { join } from "node:path";
import { after, before, test } from "node:test";
import { jetstream } from "@nats-io/jetstream";
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
import { natsConfig, password, streams } from "../nats.ts";

const root = process.env.E2E_DIR;

const siblings = [
  { name: "budget", sha: "da183d31292ca99a01b94218bb1259f41a0a78a3" },
  { name: "agent-manager", sha: "5193a1efa0d1b3d6725a2356e623c35441ccab00" },
  { name: "prompt-studio", sha: "bd614ed46d8c10c687a8f361158a4a74e17fe5b0" },
  { name: "ai-proxy", sha: "26d963f900460347ae13a05ec875cef821e4172b" },
] as const;

const seedModels = [
  "google/gemini-2.5-flash-lite",
  "openai/gpt-4.1-nano",
  "mistralai/mistral-small-3.2-24b-instruct",
];

interface MockOpenRouter {
  url: string;
  received: { body: { model: string; messages: unknown } }[];
  models: unknown[];
  reply(model: string, ...replies: unknown[]): void;
  close(): Promise<void>;
}

let streamOf: (model: string, pieces: string[]) => unknown;

const run = (command: string, args: string[], cwd: string): void => {
  execFileSync(command, args, { cwd, stdio: "inherit" });
};

function checkout(dir: string, name: string, sha: string): void {
  if (!existsSync(join(dir, ".git"))) {
    run("git", ["clone", "-q", `https://github.com/famashines/ai-systems-${name}.git`, dir], ".");
  }
  run("git", ["fetch", "-q", "origin", sha], dir);
  run("git", ["-c", "advice.detachedHead=false", "checkout", "-q", sha], dir);
  if (!existsSync(join(dir, "node_modules"))) run("npm", ["ci", "--no-audit", "--no-fund"], dir);
}

const freePort = (): Promise<number> =>
  new Promise((resolve) => {
    const server = createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      server.close(() => {
        resolve(typeof address === "object" && address ? address.port : 0);
      });
    });
  });

const logger = createLogger({ service: "test", release: "test", level: "silent" });
const children: ChildProcess[] = [];
const urls: Record<string, string> = {};
let mongoContainer: Started;
let natsContainer: Started;
let mongo: Mongo;
let admin: Nats;
let mock: MockOpenRouter;
let orchestration: Awaited<ReturnType<typeof start>> | undefined;

async function startSibling(name: string, env: Record<string, string>): Promise<void> {
  const port = await freePort();
  const child = spawn(process.execPath, ["src/main.ts"], {
    cwd: join(root ?? "", name),
    env: {
      ...process.env,
      MONGODB_URI: mongoContainer.url,
      NATS_SERVERS: natsContainer.url,
      NATS_USER: `ai-systems-${name}`,
      NATS_PASSWORD: password(name),
      PORT: String(port),
      LOG_LEVEL: "warn",
      ...env,
    },
    stdio: ["ignore", "inherit", "inherit"],
  });
  children.push(child);
  const url = `http://127.0.0.1:${String(port)}`;
  urls[name] = url;
  await waitFor(
    async () => (await fetch(`${url}/ready`).catch(() => undefined))?.ok === true,
    60_000,
  );
}

before(
  async () => {
    if (root === undefined) return;
    mkdirSync(root, { recursive: true });
    for (const { name, sha } of siblings) checkout(join(root, name), name, sha);
    [mongoContainer, natsContainer] = await Promise.all([
      startMongo(),
      startNats({ config: natsConfig(["admin", "orchestration", ...siblings.map((s) => s.name)]) }),
    ]);
    mongo = await connectMongo({ url: mongoContainer.url, database: "orchestration" });
    admin = await connectNats({
      servers: natsContainer.url,
      user: "ai-systems-admin",
      pass: password("admin"),
      service: "admin",
      logger,
    });
    await createStreams(admin.connection, streams);
    const mockModule = (await import(join(root, "ai-proxy", "test", "mock-openrouter.ts"))) as {
      startMockOpenRouter: (port?: number) => Promise<MockOpenRouter>;
      streamOf: typeof streamOf;
    };
    mock = await mockModule.startMockOpenRouter(0);
    streamOf = mockModule.streamOf;
    mock.models.push(
      ...seedModels.map((id) => ({
        id,
        context_length: 128000,
        pricing: { prompt: "0.0000001", completion: "0.0000004" },
      })),
    );
    await startSibling("budget", {});
    await startSibling("agent-manager", {});
    await startSibling("prompt-studio", {});
    await startSibling("ai-proxy", {
      OPENROUTER_API_KEY: "mock-key",
      OPENROUTER_BASE_URL: mock.url,
    });
    const config = loadConfig(envSchema, {
      MONGODB_URI: mongoContainer.url,
      NATS_SERVERS: natsContainer.url,
      NATS_USER: "ai-systems-orchestration",
      NATS_PASSWORD: password("orchestration"),
      AGENT_MANAGER_URL: urls["agent-manager"] ?? "",
      PROMPT_STUDIO_URL: urls["prompt-studio"] ?? "",
      LOG_LEVEL: "silent",
    });
    orchestration = await start({ ...config, PORT: 0 }, { drainDelayMs: 0 });
  },
  { timeout: 900_000 },
);

after(async () => {
  if (root === undefined) return;
  await orchestration?.app.close();
  for (const child of children) child.kill("SIGTERM");
  await Promise.all(
    children
      .filter((child) => child.exitCode === null)
      .map((child) => new Promise((resolve) => child.once("exit", resolve))),
  );
  await mock.close();
  await admin.close();
  await mongo.close();
  await Promise.all([mongoContainer.stop(), natsContainer.stop()]);
});

type Stored = { type: string; streamId: string; payload: Record<string, unknown> };
const events = () => mongo.db.collection<Stored>("events");
const conversationId = randomUUID();

async function chat(
  text: string,
  mode: "auto" | "approval" = "auto",
): Promise<{ messageId: string; taskId: string }> {
  const messageId = randomUUID();
  const eventId = randomUUID();
  await jetstream(admin.connection).publish(
    "conversation.OwnerMessagePosted",
    JSON.stringify({
      eventId,
      type: "conversation.OwnerMessagePosted",
      schemaVersion: 1,
      streamId: `conversation-${conversationId}`,
      version: 1,
      occurredAt: new Date().toISOString(),
      correlationId: messageId,
      causationId: randomUUID(),
      actor: { kind: "owner", id: "owner-1" },
      payload: { conversationId, messageId, text, intent: "new_task", mode },
    }),
    { msgID: eventId },
  );
  await waitFor(
    async () => (await events().countDocuments({ "payload.messageId": messageId })) > 0,
  );
  const task = await events().findOne({ "payload.messageId": messageId });
  return { messageId, taskId: String(task?.payload.taskId) };
}

const ended = async (taskId: string): Promise<Stored[]> => {
  const final = ["RunCompleted", "RunFailed", "RunBlocked"].map((name) => `orchestration.${name}`);
  await waitFor(
    async () =>
      (await events().countDocuments({ streamId: `task-${taskId}`, type: { $in: final } })) > 0,
    60_000,
  );
  return events()
    .find({ streamId: `task-${taskId}` }, { sort: { version: 1 } })
    .toArray();
};

test(
  "with no monthly cap, the budget refuses the plan call and the task fails",
  { skip: root === undefined },
  async () => {
    const { taskId } = await chat("Write a haiku about the sea.");
    const stream = await ended(taskId);
    assert.deepEqual(
      stream.map((event) => event.type),
      [
        "orchestration.TaskSubmitted",
        "orchestration.CallRequested",
        "orchestration.CallSettled",
        "orchestration.RunFailed",
      ],
    );
    assert.equal(stream[1]?.payload.kind, "plan");
    assert.equal(stream[3]?.payload.code, "budget_refused");
    assert.equal(
      stream[3].payload.reason,
      "The budget refused the plan call: No monthly cap is set",
    );
    assert.equal(mock.received.length, 0);
  },
);

test(
  "a cap that is reached fails the plan call; with room, the orchestrator plans, the writer answers, and the run completes",
  { skip: root === undefined },
  async () => {
    const budget = createEventStore(mongo.db.client.db("budget"));
    const setCap = (hardCapMicroUsd: number, expectedVersion: number) =>
      budget.append({
        streamId: "budget-cap-month",
        expectedVersion,
        events: [
          {
            type: "budget.BudgetSet",
            schemaVersion: 1,
            correlationId: randomUUID(),
            causationId: randomUUID(),
            actor: { kind: "owner", id: "owner-1" },
            payload: { scope: { kind: "month" }, hardCapMicroUsd },
          },
        ],
      });
    await setCap(1, 0);
    const capped = await ended((await chat("Write a haiku about the sea.")).taskId);
    assert.equal(capped.at(-1)?.type, "orchestration.RunFailed");
    assert.equal(capped.at(-1)?.payload.code, "budget_refused");
    assert.equal(
      capped.at(-1)?.payload.reason,
      "The budget refused the plan call: The monthly cap has no room for this call",
    );
    assert.equal(mock.received.length, 0);
    await setCap(1_000_000, 1);
    const [first] = seedModels;
    const plan = JSON.stringify({
      subtasks: [{ title: "Haiku", instructions: "Write a haiku about the sea.", agent: "writer" }],
    });
    mock.reply(
      first ?? "",
      streamOf(first ?? "", [plan]),
      streamOf(first ?? "", ["Waves fold into foam."]),
      streamOf(first ?? "", ["The haiku (step 1): Waves fold into foam."]),
      streamOf(first ?? "", ['{"pass": true}']),
    );
    const { messageId, taskId } = await chat("Write a haiku about the sea.");
    const stream = await ended(taskId);
    assert.equal(stream.at(-1)?.type, "orchestration.RunCompleted");
    assert.equal(stream.at(-1)?.payload.summary, "The haiku (step 1): Waves fold into foam.");
    assert.equal(stream.at(-1)?.payload.costMicroUsd, 42);
    const calls = stream.filter((event) => event.type === "orchestration.CallRequested");
    assert.deepEqual(
      calls.map((event) => event.payload.kind),
      ["plan", "subtask", "aggregate", "check"],
    );
    assert.equal(calls[0]?.payload.maxCostMicroUsd, 50_000);
    assert.equal(stream.filter((event) => event.type === "orchestration.CallSettled").length, 4);
    assert.equal(mock.received.length, 4);
    const [planRequest, subtaskRequest] = mock.received;
    assert.equal(planRequest?.body.model, first);
    const planMessages = planRequest?.body.messages as { role: string; content: string }[];
    assert.match(planMessages[0]?.content ?? "", /^You are the orchestrator/);
    const blockId = /BEGIN DATA owner_message (\S+)\n/.exec(planMessages[1]?.content ?? "")?.[1];
    assert.ok(blockId !== undefined && blockId !== messageId);
    assert.ok(
      planMessages[1]?.content.endsWith(
        `BEGIN DATA owner_message ${blockId}\nWrite a haiku about the sea.\nEND DATA owner_message ${blockId}`,
      ),
    );
    const subtaskMessages = subtaskRequest?.body.messages as { content: string }[];
    assert.match(subtaskMessages[0]?.content ?? "", /^You are the writer agent\./);
    assert.match(subtaskMessages[1]?.content ?? "", /\nThe step: Haiku\n/);
  },
);
