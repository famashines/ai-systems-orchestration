import { randomUUID } from "node:crypto";
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
import { writerLoader } from "./adapters/writer.ts";
import { onLlmEvent, onOwnerMessage, type ChatDeps } from "./application/chat.ts";

export const service = "orchestration";

const httpUrl = z.url({ protocol: /^https?$/ });

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
  const migrations = [eventStoreMigration, processedMigration];
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
  const chat: ChatDeps = {
    store,
    newId: randomUUID,
    now: () => new Date(),
    logger,
    loadWriter: writerLoader({
      logger,
      agentManagerUrl: config.AGENT_MANAGER_URL,
      promptStudioUrl: config.PROMPT_STUDIO_URL,
    }),
  };
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
