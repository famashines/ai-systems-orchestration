import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigError, loadConfig } from "@famashines/ai-systems-kit/config";
import { envSchema } from "../../src/main.ts";

const env = {
  MONGODB_URI: "mongodb://mongo:27017/?replicaSet=rs0",
  NATS_USER: "ai-systems-orchestration",
  NATS_PASSWORD: "secret",
};

test("NATS_SERVERS is trimmed and empty items are dropped", () => {
  const config = loadConfig(envSchema, {
    ...env,
    NATS_SERVERS: " nats://a:4222 ,, nats://b:4222,",
  });
  assert.deepEqual(config.NATS_SERVERS, ["nats://a:4222", "nats://b:4222"]);
  assert.equal(config.MONGODB_DATABASE, "orchestration");
  assert.equal(config.PORT, 8080);
});

for (const value of ["", " , ,", ","]) {
  test(`NATS_SERVERS "${value}" is refused`, () => {
    assert.throws(() => loadConfig(envSchema, { ...env, NATS_SERVERS: value }), ConfigError);
  });
}
