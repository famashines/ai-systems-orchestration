import { setTimeout as sleep } from "node:timers/promises";
import {
  ActiveAgentVersionResultV1,
  PromptVersionResultV1,
} from "@famashines/ai-systems-contracts";
import type { z } from "zod";
import type { AgentLoad, LoadFailure } from "../domain/task.ts";

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
  readonly status: number | undefined;
  constructor(url: string, what: string, options?: { cause?: unknown; status?: number }) {
    super(`The read of ${url} failed: ${what}`, options);
    this.name = "ReadFailedError";
    this.status = options?.status;
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
  if (!response.ok) {
    throw new ReadFailedError(url, `HTTP ${String(response.status)}`, { status: response.status });
  }
  const parsed = schema.safeParse(await response.json().catch(() => undefined));
  return parsed.success ? parsed.data : "invalid";
}

interface Words {
  agent: string;
  prompt: string;
  log: string;
}

function agentLoader({
  agentManagerUrl,
  promptStudioUrl,
  logger,
  timeoutMs = 2000,
  retryDelaysMs = [2000, 4000],
}: WriterSources): (path: string, words: Words, agentId?: string) => Promise<AgentLoad> {
  async function read<S extends z.ZodType>(url: string, schema: S, log: string) {
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await readOnce(url, schema, timeoutMs);
      } catch (error) {
        const refused =
          error instanceof ReadFailedError && error.status !== undefined && error.status < 500;
        const delayMs = refused ? undefined : retryDelaysMs[attempt];
        if (delayMs === undefined) {
          logger.warn({ err: error, attempts: attempt + 1 }, `the ${log} read failed`);
          return refused ? ("refused" as const) : ("unavailable" as const);
        }
        await sleep(delayMs);
      }
    }
  }

  return async (path, { agent: what, prompt: whatPrompt, log }, agentId) => {
    const unavailable: LoadFailure = {
      code: "unavailable",
      reason: `agent-manager or prompt-studio did not answer, so the ${what} could not start. Try again in a minute.`,
    };
    const agent = await read(new URL(path, agentManagerUrl).href, ActiveAgentVersionResultV1, log);
    if (agent === "unavailable") return unavailable;
    if (agent === "refused") {
      return { code: "internal", reason: `agent-manager refused the read of the ${what}.` };
    }
    if (agent === "not_found") {
      return { code: "not_found", reason: `The ${what} has no active version.` };
    }
    if (agent === "invalid" || (agentId !== undefined && agent.agentId !== agentId)) {
      return { code: "internal", reason: `agent-manager gave a ${what} that is not valid.` };
    }
    const { promptId, version } = agent.prompt;
    const prompt = await read(
      new URL(`/internal/prompts/${promptId}/versions/${String(version)}`, promptStudioUrl).href,
      PromptVersionResultV1,
      log,
    );
    if (prompt === "unavailable") return unavailable;
    if (prompt === "refused") {
      return { code: "internal", reason: `prompt-studio refused the read of the ${whatPrompt}.` };
    }
    if (prompt === "not_found") {
      return { code: "not_found", reason: `The prompt version of the ${what} is not there.` };
    }
    if (prompt === "invalid" || prompt.promptId !== promptId || prompt.version !== version) {
      return { code: "internal", reason: `prompt-studio gave a ${whatPrompt} that is not valid.` };
    }
    return { agent, prompt };
  };
}

export function writerLoader(sources: WriterSources): () => Promise<AgentLoad> {
  const load = agentLoader(sources);
  return () =>
    load("/internal/agents/by-name/writer/active-version", {
      agent: "writer agent",
      prompt: "writer prompt",
      log: "writer",
    });
}

export function agentByIdLoader(sources: WriterSources): (agentId: string) => Promise<AgentLoad> {
  const load = agentLoader(sources);
  return (agentId) =>
    load(
      `/internal/agents/${agentId}/active-version`,
      {
        agent: `subtask agent ${agentId}`,
        prompt: `prompt of the subtask agent ${agentId}`,
        log: "agent",
      },
      agentId,
    );
}
