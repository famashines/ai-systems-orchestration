import { randomUUID } from "node:crypto";
import { AgentName } from "@famashines/ai-systems-contracts";
import { commonEnv, loadConfig } from "@famashines/ai-systems-kit/config";
import { createEventStore, eventStoreMigration } from "@famashines/ai-systems-kit/event-store";
import { createService } from "@famashines/ai-systems-kit/http";
import {
  connectNats,
  processedMigration,
  startConsumer,
  startRelay,
} from "@famashines/ai-systems-kit/messaging";
import { connectMongo, runMigrations } from "@famashines/ai-systems-kit/mongo";
import { createLogger, startTelemetry } from "@famashines/ai-systems-kit/observability";
import { z } from "zod";
import { routes } from "./adapters/http.ts";
import { startTimers, timersMigration } from "./adapters/timers.ts";
import { agentByIdLoader, agentByNameLoader } from "./adapters/writer.ts";
import { onLlmEvent, onOwnerMessage } from "./application/chat.ts";
import { onTimer, type TaskDeps } from "./application/run.ts";

export const service = "orchestration";

const httpUrl = z.url({ protocol: /^https?$/ });
const int = (fallback: string, schema: z.ZodInt) =>
  z.string().regex(/^\d+$/).default(fallback).transform(Number).pipe(schema);

export const envSchema = commonEnv.omit({ SERVICE_NAME: true }).extend({
  MONGODB_URI: z.string().min(1),
  MONGODB_DATABASE: z.string().min(1).default("orchestration"),
  NATS_SERVERS: z
    .string()
    .min(1)
    .transform((value) =>
      value
        .split(",")
        .map((server) => server.trim())
        .filter((server) => server !== ""),
    )
    .pipe(z.array(z.string()).min(1)),
  NATS_USER: z.string().min(1),
  NATS_PASSWORD: z.string().min(1),
  AGENT_MANAGER_URL: httpUrl.default("http://ai-systems-agent-manager:8080"),
  PROMPT_STUDIO_URL: httpUrl.default("http://ai-systems-prompt-studio:8080"),
  MAX_PARALLEL_SUBTASKS: int("3", z.int().min(1).max(20)),
  MAX_CALLS_PER_TASK: int("50", z.int().min(1).max(1000)),
  SUBTASK_RETRY_DELAYS_MS: z
    .string()
    .regex(/^\d+,\d+$/)
    .default("5000,30000")
    .transform((value) => value.split(",").map(Number))
    .pipe(z.array(z.int().min(1).max(3_600_000))),
  CHECKPOINT_TIMEOUT_MS: int(
    "86400000",
    z
      .int()
      .min(60_000)
      .max(30 * 86_400_000),
  ),
  TIMER_INTERVAL_MS: int("1000", z.int().min(100).max(60_000)),
  PLAN_AGENTS: z
    .string()
    .default("writer")
    .transform((value) => value.split(",").map((name) => name.trim()))
    .pipe(z.array(AgentName).min(1).max(50)),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.url().optional(),
  OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: z.url().optional(),
});

export type Config = z.output<typeof envSchema>;

export async function start(config: Readonly<Config>, options: { drainDelayMs?: number } = {}) {
  const release = config.RELEASE;
  const telemetry = startTelemetry({ serviceName: service, release });
  const logger = createLogger({ service, release, level: config.LOG_LEVEL });
  logger.info("connecting to MongoDB");
  const mongo = await connectMongo({
    url: config.MONGODB_URI,
    database: config.MONGODB_DATABASE,
    clientOptions: { serverSelectionTimeoutMS: 5000 },
  });
  const migrations = [eventStoreMigration, processedMigration, timersMigration];
  await runMigrations(mongo.db, migrations);
  const store = createEventStore(mongo.db);
  logger.info("connecting to NATS");
  const nats = await connectNats({
    servers: config.NATS_SERVERS,
    user: config.NATS_USER,
    pass: config.NATS_PASSWORD,
    service,
    logger,
  });
  const relay = startRelay({ store, nats, streams: ["ORCHESTRATION"] });
  const sources = {
    logger,
    agentManagerUrl: config.AGENT_MANAGER_URL,
    promptStudioUrl: config.PROMPT_STUDIO_URL,
  };
  const chat: TaskDeps = {
    store,
    newId: randomUUID,
    now: () => new Date(),
    logger,
    settings: {
      maxParallel: config.MAX_PARALLEL_SUBTASKS,
      maxCallsPerTask: config.MAX_CALLS_PER_TASK,
      retryDelaysMs: config.SUBTASK_RETRY_DELAYS_MS,
      checkpointTimeoutMs: config.CHECKPOINT_TIMEOUT_MS,
      planAgents: config.PLAN_AGENTS,
    },
    loadAgent: agentByIdLoader(sources),
    loadAgentByName: agentByNameLoader(sources),
  };
  const timers = startTimers({
    db: mongo.db,
    intervalMs: config.TIMER_INTERVAL_MS,
    logger,
    handle: (timer, also) => onTimer(chat, timer, also),
  });
  const consumers = [
    startConsumer({
      nats,
      db: mongo.db,
      stream: "CONVERSATION",
      filter: "conversation.OwnerMessagePosted",
      handler: (event, context) => onOwnerMessage(chat, event, context),
      ordered: true,
    }),
    startConsumer({
      nats,
      db: mongo.db,
      stream: "LLM",
      filter: "llm.>",
      handler: (event, context) => onLlmEvent(chat, event, context),
      ordered: true,
    }),
  ];
  const app = createService({
    logger,
    routes: routes({ store, newId: randomUUID }),
    readiness: {
      mongo: mongo.ready,
      migrations: async () =>
        (await mongo.db
          .collection<{ _id: string }>("migrations")
          .countDocuments({ _id: { $in: migrations.map((migration) => migration.name) } })) ===
        migrations.length,
    },
    metrics: telemetry.metricsHandler,
    closeHooks: [
      telemetry.shutdown,
      mongo.close,
      nats.close,
      relay.close,
      timers.close,
      ...consumers.map((consumer) => consumer.close),
    ],
    ...options,
  });
  const address = await app.listen(config.PORT);
  logger.info({ port: address.port }, "orchestration started");
  return { app, address };
}

if (import.meta.main) {
  const exitEarly = (): never => process.exit(143);
  process.once("SIGTERM", exitEarly);
  process.once("SIGINT", exitEarly);
  const { app } = await start(loadConfig(envSchema));
  process.off("SIGTERM", exitEarly);
  process.off("SIGINT", exitEarly);
  app.handleSignals();
}
