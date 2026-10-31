import { randomUUID } from "node:crypto";
import { commonEnv, loadConfig } from "@famashines/ai-systems-kit/config";
import { createEventStore, eventStoreMigration } from "@famashines/ai-systems-kit/event-store";
import { createService } from "@famashines/ai-systems-kit/http";
import { connectNats, startRelay } from "@famashines/ai-systems-kit/messaging";
import { connectMongo, runMigrations } from "@famashines/ai-systems-kit/mongo";
import { createLogger, startTelemetry } from "@famashines/ai-systems-kit/observability";
import { z } from "zod";
import { routes } from "./adapters/http.ts";

export const service = "orchestration";

export const envSchema = commonEnv.omit({ SERVICE_NAME: true }).extend({
  MONGODB_URI: z.string().min(1),
  MONGODB_DATABASE: z.string().min(1).default("orchestration"),
  NATS_SERVERS: z
    .string()
    .min(1)
    .transform((value) => value.split(",").map((server) => server.trim())),
  NATS_USER: z.string().min(1),
  NATS_PASSWORD: z.string().min(1),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.url().optional(),
  OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: z.url().optional(),
});

export type Config = z.output<typeof envSchema>;

export async function start(config: Readonly<Config>, options: { drainDelayMs?: number } = {}) {
  const release = config.RELEASE;
  const telemetry = startTelemetry({ serviceName: service, release });
  const logger = createLogger({ service, release, level: config.LOG_LEVEL });
  const mongo = await connectMongo({ url: config.MONGODB_URI, database: config.MONGODB_DATABASE });
  await runMigrations(mongo.db, [eventStoreMigration]);
  const store = createEventStore(mongo.db);
  const nats = await connectNats({
    servers: config.NATS_SERVERS,
    user: config.NATS_USER,
    pass: config.NATS_PASSWORD,
    service,
    logger,
  });
  const relay = startRelay({ store, nats, streams: ["ORCHESTRATION"] });
  const app = createService({
    logger,
    routes: routes({ store, newId: randomUUID }),
    readiness: { mongo: mongo.ready, nats: nats.ready, relay: relay.ready },
    metrics: telemetry.metricsHandler,
    closeHooks: [telemetry.shutdown, mongo.close, nats.close, relay.close],
    ...options,
  });
  const address = await app.listen(config.PORT);
  logger.info({ port: address.port }, "orchestration started");
  return { app, address };
}

if (import.meta.main) {
  const { app } = await start(loadConfig(envSchema));
  app.handleSignals();
}
