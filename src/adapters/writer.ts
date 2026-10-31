import { setTimeout as sleep } from "node:timers/promises";
import {
  ActiveAgentVersionResultV1,
  PromptVersionResultV1,
} from "@famashines/ai-systems-contracts";
import type { z } from "zod";
import type { LoadFailure, Writer } from "../domain/task.ts";

export interface WarnLogger {
  warn(details: object, message: string): void;
}

export interface WriterSources {
  agentManagerUrl: string;
  promptStudioUrl: string;
  logger: WarnLogger;
  timeoutMs?: number;
  retryDelaysMs?: readonly number[];
}

export class ReadFailedError extends Error {
  constructor(url: string, what: string, options?: { cause: unknown }) {
    super(`The read of ${url} failed: ${what}`, options);
    this.name = "ReadFailedError";
  }
}

async function readOnce<S extends z.ZodType>(
  url: string,
  schema: S,
  timeoutMs: number,
): Promise<z.output<S> | "not_found" | "invalid"> {
  let response: Response;
  try {
    response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
  } catch (error) {
    throw new ReadFailedError(url, (error as Error).name, { cause: error });
  }
  if (response.status === 404) return "not_found";
  if (!response.ok) throw new ReadFailedError(url, `HTTP ${String(response.status)}`);
  const parsed = schema.safeParse(await response.json().catch(() => undefined));
  return parsed.success ? parsed.data : "invalid";
}

const unavailable: LoadFailure = {
  code: "unavailable",
  reason:
    "agent-manager or prompt-studio did not answer, so the writer agent could not start. Try again in a minute.",
};

export function writerLoader({
  agentManagerUrl,
  promptStudioUrl,
  logger,
  timeoutMs = 2000,
  retryDelaysMs = [2000, 4000],
}: WriterSources): () => Promise<Writer | LoadFailure> {
  async function read<S extends z.ZodType>(url: string, schema: S) {
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await readOnce(url, schema, timeoutMs);
      } catch (error) {
        const delayMs = retryDelaysMs[attempt];
        if (delayMs === undefined) {
          logger.warn({ err: error, attempts: attempt + 1 }, "the writer read failed");
          return "unavailable" as const;
        }
        await sleep(delayMs);
      }
    }
  }

  return async () => {
    const agent = await read(
      new URL("/internal/agents/by-name/writer/active-version", agentManagerUrl).href,
      ActiveAgentVersionResultV1,
    );
    if (agent === "unavailable") return unavailable;
    if (agent === "not_found") {
      return { code: "not_found", reason: "The writer agent has no active version." };
    }
    if (agent === "invalid") {
      return { code: "internal", reason: "agent-manager gave a writer agent that is not valid." };
    }
    const { promptId, version } = agent.prompt;
    const prompt = await read(
      new URL(`/internal/prompts/${promptId}/versions/${String(version)}`, promptStudioUrl).href,
      PromptVersionResultV1,
    );
    if (prompt === "unavailable") return unavailable;
    if (prompt === "not_found") {
      return { code: "not_found", reason: "The prompt version of the writer agent is not there." };
    }
    if (prompt === "invalid" || prompt.promptId !== promptId || prompt.version !== version) {
      return { code: "internal", reason: "prompt-studio gave a writer prompt that is not valid." };
    }
    return { agent, prompt };
  };
}
