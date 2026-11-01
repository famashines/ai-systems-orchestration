import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { after, before, test } from "node:test";
import { jetstreamManager, type JetStreamManager } from "@nats-io/jetstream";
import type { PlanSubtask } from "@famashines/ai-systems-contracts";
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
import { exportJWK, generateKeyPair, SignJWT, type CryptoKey } from "jose";
import { envSchema, start } from "../../src/main.ts";
import { natsConfig, password, streams } from "../nats.ts";

const issuer = "https://auth.example.test/realms/ai-systems";
const ownerId = "owner-1";
const promptId = "cc74090a-534e-4072-8bfd-e6c5b9f22f41";
const agent = {
  agentId: "00000000-0000-4000-8000-00000000a002",
  name: "writer",
  enabled: true,
  version: 1,
  prompt: { promptId, version: 1 },
  models: ["google/gemini-2.5-flash-lite"],
  limits: { maxCalls: 10, maxTokens: 4096, maxCostMicroUsd: 50_000 },
};
const prompt = { promptId, name: "writer", version: 1, text: "You write.", variables: [] };
const otherAgent = "00000000-0000-4000-8000-00000000a003";

const logger = createLogger({ service: "test", release: "test", level: "silent" });
let mongoContainer: Started;
let natsContainer: Started;
let mongo: Mongo;
let admin: Nats;
let manager: JetStreamManager;
let fakes: Server;
let privateKey: CryptoKey;
let running: Awaited<ReturnType<typeof start>> | undefined;
let base = "";

const listen = async (server: Server): Promise<string> => {
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return `http://127.0.0.1:${String((server.address() as AddressInfo).port)}`;
};

before(async () => {
  const pair = await generateKeyPair("RS256", { extractable: true });
  privateKey = pair.privateKey;
  const jwk = { ...(await exportJWK(pair.publicKey)), kid: "k1", alg: "RS256", use: "sig" };
  fakes = createServer((request, response) => {
    const [status, body] =
      request.url === "/certs"
        ? [200, { keys: [jwk] }]
        : request.url === `/internal/agents/${agent.agentId}/active-version`
          ? [200, agent]
          : request.url === `/internal/prompts/${promptId}/versions/1`
            ? [200, prompt]
            : [404, {}];
    response.writeHead(status, { "content-type": "application/json" });
    response.end(JSON.stringify(body));
  });
  const fakesUrl = await listen(fakes);
  [mongoContainer, natsContainer] = await Promise.all([
    startMongo(),
    startNats({ config: natsConfig(["admin", "orchestration"]) }),
  ]);
  mongo = await connectMongo({ url: mongoContainer.url, database: "orchestration" });
  admin = await connectNats({
    servers: natsContainer.url,
    user: "ai-systems-admin",
    pass: password("admin"),
    service: "admin",
    logger,
  });
  manager = await jetstreamManager(admin.connection);
  await createStreams(admin.connection, streams);
  const config = loadConfig(envSchema, {
    MONGODB_URI: mongoContainer.url,
    NATS_SERVERS: natsContainer.url,
    NATS_USER: "ai-systems-orchestration",
    NATS_PASSWORD: password("orchestration"),
    AGENT_MANAGER_URL: fakesUrl,
    PROMPT_STUDIO_URL: fakesUrl,
    OIDC_ISSUER: issuer,
    OIDC_JWKS_URL: `${fakesUrl}/certs`,
    LOG_LEVEL: "silent",
  });
  running = await start({ ...config, PORT: 0 }, { drainDelayMs: 0 });
  base = `http://127.0.0.1:${String(running.address.port)}`;
  await waitFor(async () => (await fetch(`${base}/ready`)).ok);
});

after(async () => {
  if (running) assert.equal(await running.app.close(), true);
  await admin.close();
  await mongo.close();
  await new Promise((resolve) => fakes.close(resolve));
  await Promise.all([mongoContainer.stop(), natsContainer.stop()]);
});

const token = (roles = ["owner"], sub = ownerId) =>
  new SignJWT({ typ: "Bearer", azp: "ai-systems-app", realm_access: { roles } })
    .setProtectedHeader({ alg: "RS256", kid: "k1" })
    .setIssuer(issuer)
    .setAudience("ai-systems-app")
    .setSubject(sub)
    .setIssuedAt()
    .setExpirationTime("5m")
    .sign(privateKey);

const kebab = (name: string) => name.replace(/(?<!^)[A-Z]/g, "-$&").toLowerCase();

interface Sent {
  status: number;
  body: Record<string, unknown>;
  commandId: string;
}

async function send(
  name: string,
  payload: object,
  options: { commandId?: string; auth?: string | null; actorId?: string } = {},
): Promise<Sent> {
  const commandId = options.commandId ?? randomUUID();
  const auth = options.auth === undefined ? `Bearer ${await token()}` : options.auth;
  const response = await fetch(`${base}/internal/commands/${kebab(name)}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(auth === null ? {} : { authorization: auth }),
    },
    body: JSON.stringify({
      commandId,
      type: `orchestration.${name}`,
      actor: { kind: "owner", id: options.actorId ?? ownerId },
      payload,
    }),
  });
  return {
    status: response.status,
    body: (await response.json()) as Record<string, unknown>,
    commandId,
  };
}

type Stored = {
  _id: string;
  type: string;
  version: number;
  causationId: string;
  actor: { kind: string; id: string };
  payload: Record<string, unknown>;
};
const streamOf = (taskId: string) =>
  mongo.db
    .collection<Stored>("events")
    .find({ streamId: `task-${taskId}` }, { sort: { version: 1 } })
    .toArray();

const byCommand = async (taskId: string, commandId: string) =>
  (await streamOf(taskId))
    .filter((event) => event.causationId === commandId)
    .map((event) => `${event.type.replace("orchestration.", "")}:${event.actor.kind}`);

const step = (subtaskId: string, over: Partial<PlanSubtask> = {}): PlanSubtask => ({
  subtaskId,
  title: `Step ${subtaskId.slice(-2)}`,
  instructions: "Do it.",
  agentId: agent.agentId,
  dependsOn: [],
  needsApproval: false,
  ...over,
});

async function seed(history: [string, object][]): Promise<void> {
  const taskId = (history[0]?.[1] as { taskId: string }).taskId;
  await createEventStore(mongo.db).append({
    streamId: `task-${taskId}`,
    expectedVersion: 0,
    events: history.map(([type, payload]) => ({
      type: `orchestration.${type}`,
      schemaVersion: 1,
      payload,
      correlationId: taskId,
      causationId: randomUUID(),
      actor: { kind: "system", id: "test" },
    })) as never,
  });
}

const awaiting = async (subtasks: PlanSubtask[], mode = "approval"): Promise<string> => {
  const taskId = randomUUID();
  await seed([
    ["TaskSubmitted", { taskId, text: "Write a report", mode }],
    ["PlanProposed", { taskId, planVersion: 1, subtasks, awaitsApproval: true }],
  ]);
  return taskId;
};

const published = async (eventId: string) => {
  await waitFor(
    async () =>
      (
        await mongo.db.collection<Stored & { publishedAt: Date | null }>("events").findOne({
          _id: eventId,
        })
      )?.publishedAt != null,
  );
  const { state } = await manager.streams.info("ORCHESTRATION");
  for (let seq = state.last_seq; seq >= state.first_seq; seq -= 1) {
    const message = await manager.streams.getMessage("ORCHESTRATION", { seq });
    if (message?.header.get("Nats-Msg-Id") === eventId) {
      return message.json<{ actor: { kind: string; id: string } }>();
    }
  }
  return undefined;
};

test("EditPlan, ApprovePlan, PauseRun, ResumeRun, ReassignSubtask and CancelRun with the owner as actor", async () => {
  const [first, second] = [randomUUID(), randomUUID()];
  const taskId = await awaiting([step(first)]);

  const edited = await send("EditPlan", {
    taskId,
    planVersion: 1,
    subtasks: [step(first, { estimatedCostMicroUsd: 7 }), step(second, { dependsOn: [first] })],
  });
  assert.equal(edited.status, 201);
  assert.deepEqual(await byCommand(taskId, edited.commandId), ["PlanEdited:owner"]);
  const plan = (await streamOf(taskId)).at(-1)?.payload as { subtasks: PlanSubtask[] };
  assert.equal(plan.subtasks[0]?.estimatedCostMicroUsd, undefined);

  const stale = await send("ApprovePlan", { taskId, planVersion: 1 });
  assert.equal(stale.status, 409);
  assert.equal(stale.body.code, "conflict");

  const approved = await send("ApprovePlan", { taskId, planVersion: 2 });
  assert.equal(approved.status, 201);
  assert.deepEqual(await byCommand(taskId, approved.commandId), [
    "PlanApproved:owner",
    "RunStarted:owner",
    "SubtaskQueued:owner",
    "SubtaskWaiting:owner",
    "SubtaskStarted:system",
    "CallRequested:system",
  ]);
  const replay = await send("ApprovePlan", { taskId, planVersion: 2 }, approved);
  assert.equal(replay.status, 200);
  assert.deepEqual(replay.body, approved.body);
  assert.deepEqual(approved.body, { taskId, version: (await streamOf(taskId)).length });
  const approval = (await streamOf(taskId)).find(
    (event) => event.type === "orchestration.PlanApproved",
  );
  assert.deepEqual((await published(approval?._id ?? ""))?.actor, { kind: "owner", id: ownerId });

  const paused = await send("PauseRun", { taskId });
  assert.equal(paused.status, 201);
  assert.deepEqual(await byCommand(taskId, paused.commandId), ["RunPaused:owner"]);
  assert.equal((await send("PauseRun", { taskId })).status, 409);

  const resumed = await send("ResumeRun", { taskId });
  assert.equal(resumed.status, 201);
  assert.deepEqual(await byCommand(taskId, resumed.commandId), ["RunResumed:owner"]);

  assert.equal(
    (await send("ReassignSubtask", { taskId, subtaskId: first, agentId: otherAgent })).status,
    409,
  );
  const reassigned = await send("ReassignSubtask", {
    taskId,
    subtaskId: second,
    agentId: otherAgent,
    model: "openai/gpt-4.1-nano",
  });
  assert.equal(reassigned.status, 201);
  assert.deepEqual(await byCommand(taskId, reassigned.commandId), ["SubtaskReassigned:owner"]);

  const cancelled = await send("CancelRun", { taskId, reason: "Not needed." });
  assert.equal(cancelled.status, 201);
  assert.deepEqual(await byCommand(taskId, cancelled.commandId), [
    "RunCancelled:owner",
    "CallCancelRequested:owner",
  ]);
  assert.equal((await send("CancelRun", { taskId })).status, 409);
});

test("RejectPlan ends a task that waits for approval", async () => {
  const taskId = await awaiting([step(randomUUID())]);
  const rejected = await send("RejectPlan", { taskId, planVersion: 1, reason: "Too long." });
  assert.equal(rejected.status, 201);
  assert.deepEqual(await byCommand(taskId, rejected.commandId), ["PlanRejected:owner"]);
  assert.equal((await send("ApprovePlan", { taskId, planVersion: 1 })).status, 409);
});

test("SetTaskMode and CancelTask of a task with no run", async () => {
  const taskId = randomUUID();
  await seed([["TaskSubmitted", { taskId, text: "Write a report", mode: "approval" }]]);
  const mode = await send("SetTaskMode", { taskId, mode: "auto" });
  assert.equal(mode.status, 201);
  assert.deepEqual(await byCommand(taskId, mode.commandId), ["TaskModeChanged:owner"]);
  const cancelled = await send("CancelTask", { taskId });
  assert.equal(cancelled.status, 201);
  assert.deepEqual(await byCommand(taskId, cancelled.commandId), ["TaskCancelled:owner"]);
  assert.equal((await send("SetTaskMode", { taskId, mode: "approval" })).status, 409);
});

test("ResolveCheckpoint starts the subtask; ResumeRun after a failed subtask is a conflict; RetrySubtask queues it", async () => {
  const subtaskId = randomUUID();
  const taskId = await awaiting([step(subtaskId, { needsApproval: true })]);
  const approved = await send("ApprovePlan", { taskId, planVersion: 1 });
  assert.deepEqual(await byCommand(taskId, approved.commandId), [
    "PlanApproved:owner",
    "RunStarted:owner",
    "SubtaskQueued:owner",
    "CheckpointOpened:system",
  ]);
  const opened = (await streamOf(taskId)).at(-1)?.payload as { checkpointId: string };
  const resolved = await send("ResolveCheckpoint", {
    taskId,
    checkpointId: opened.checkpointId,
    decision: "refused",
    comment: "Not now.",
  });
  assert.equal(resolved.status, 201);
  assert.deepEqual(await byCommand(taskId, resolved.commandId), [
    "CheckpointResolved:owner",
    "SubtaskFailed:owner",
    "RunBlocked:system",
  ]);
  const resume = await send("ResumeRun", { taskId });
  assert.equal(resume.status, 409);
  assert.equal(resume.body.detail, "A subtask failed. Retry the subtask.");

  const retried = await send("RetrySubtask", { taskId, subtaskId });
  assert.equal(retried.status, 201);
  assert.deepEqual(await byCommand(taskId, retried.commandId), [
    "SubtaskQueued:owner",
    "RunResumed:owner",
    "CheckpointOpened:system",
  ]);
  const again = (await streamOf(taskId)).at(-1)?.payload as { checkpointId: string };
  const approvedStart = await send("ResolveCheckpoint", {
    taskId,
    checkpointId: again.checkpointId,
    decision: "approved",
  });
  assert.deepEqual(await byCommand(taskId, approvedStart.commandId), [
    "CheckpointResolved:owner",
    "SubtaskStarted:system",
    "CallRequested:system",
  ]);
});

test("the owner token is checked before the body; the actor must be the subject; an unknown task is 404", async () => {
  const taskId = await awaiting([step(randomUUID())]);
  const noToken = await send("ApprovePlan", {}, { auth: null });
  assert.equal(noToken.status, 401);
  const guest = await send("ApprovePlan", {}, { auth: `Bearer ${await token([])}` });
  assert.equal(guest.status, 403);
  const other = await send("ApprovePlan", { taskId, planVersion: 1 }, { actorId: "owner-2" });
  assert.equal(other.status, 403);
  assert.equal((await send("ApprovePlan", { taskId })).status, 400);
  assert.equal((await send("PauseRun", { taskId: randomUUID() })).status, 404);
  assert.equal((await streamOf(taskId)).length, 2);
});
