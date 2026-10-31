import {
  ActiveAgentVersionResultV1,
  PromptVersionResultV1,
} from "@famashines/ai-systems-contracts";
import type { z } from "zod";
import type { LoadFailure, Writer } from "../domain/task.ts";

export interface WriterSources {
  agentManagerUrl: string;
  promptStudioUrl: string;
  timeoutMs?: number;
}

export class ReadFailedError extends Error {
  constructor(url: string, cause: string) {
    super(`The read of ${url} failed: ${cause}`);
    this.name = "ReadFailedError";
  }
}

async function read<S extends z.ZodType>(
  url: string,
  schema: S,
  timeoutMs: number,
): Promise<z.output<S> | "not_found" | "invalid"> {
  let response: Response;
  try {
    response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
  } catch (error) {
    throw new ReadFailedError(url, (error as Error).name);
  }
  if (response.status === 404) return "not_found";
  if (!response.ok) throw new ReadFailedError(url, `HTTP ${String(response.status)}`);
  const parsed = schema.safeParse(await response.json().catch(() => undefined));
  return parsed.success ? parsed.data : "invalid";
}

export function writerLoader({
  agentManagerUrl,
  promptStudioUrl,
  timeoutMs = 5000,
}: WriterSources): () => Promise<Writer | LoadFailure> {
  return async () => {
    const agent = await read(
      new URL("/internal/agents/by-name/writer/active-version", agentManagerUrl).href,
      ActiveAgentVersionResultV1,
      timeoutMs,
    );
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
      timeoutMs,
    );
    if (prompt === "not_found") {
      return { code: "not_found", reason: "The prompt version of the writer agent is not there." };
    }
    if (prompt === "invalid" || prompt.promptId !== promptId || prompt.version !== version) {
      return { code: "internal", reason: "prompt-studio gave a writer prompt that is not valid." };
    }
    return { agent, prompt };
  };
}
