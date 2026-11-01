import {
  ClarificationRequestedEventPayloadV1,
  PlanSubtasks,
  type AgentLimits,
  type CallCompletedEventPayloadV1,
  type CallFailedEventPayloadV1,
  type CallMessage,
  type ErrorCode,
  type PlanSubtask,
} from "@famashines/ai-systems-contracts";
import { z } from "zod";
import {
  authorityLine,
  block,
  callDurationMs,
  decideRun,
  event,
  ownerBlocks,
  prepareCall,
  readJson,
  type RunEnv,
  type RunSettings,
} from "./run.ts";
import {
  AgentUnavailableError,
  estimate,
  failedCodes,
  fold,
  reason,
  type AgentLoad,
  type LoadFailure,
  type TaskEvent,
  type TaskState,
} from "./state.ts";

const maxPlanTries = 2;
export const orchestratorName = "orchestrator";

const isPlanning = (state: TaskState): boolean =>
  (state.status === "submitted" || state.status === "planning") && state.plan === undefined;

const mayAsk = (state: TaskState): boolean =>
  state.conversationId !== undefined && state.answers.length === 0;

export const isPlanCall = (state: TaskState, callId: string): boolean =>
  isPlanning(state) &&
  state.orchestratorCall?.kind === "plan" &&
  state.orchestratorCall.callId === callId &&
  state.calls[callId]?.settled === false;

interface Retry {
  reason: string;
  output?: string;
}

const reworkCalls = 4;

export function maxSubtasks(
  state: TaskState,
  limits: AgentLimits,
  settings: Pick<RunSettings, "maxCallsPerTask">,
): number {
  const byCalls = settings.maxCallsPerTask - Object.keys(state.calls).length - 1 - reworkCalls;
  const byMoney =
    Math.floor((limits.maxCostMicroUsd - state.costMicroUsd) / estimate(limits)) - 1 - reworkCalls;
  return Math.max(1, Math.min(20, byCalls, byMoney));
}

function planMessages(
  state: TaskState,
  prompt: { text: string },
  most: number,
  agents: readonly string[],
  newId: () => string,
  retry?: Retry,
): CallMessage[] {
  const answered = state.answers.length > 0;
  const lines = [
    "Make a plan for the owner's request. The request is in the owner_message data block below.",
    authorityLine,
    "Answer with one JSON object only, with no other text.",
    mayAsk(state)
      ? 'If the request is not clear, ask 1 to 5 short questions: {"questions": ["<question>"]}.'
      : "Do not ask questions.",
    `For a plan: {"subtasks": [{"title": "<short title>", "instructions": "<what to do>", "agent": "<agent name>", "dependsOn": [<numbers of other subtasks>], "needsApproval": false}]}, with 1 to ${String(most)} subtasks. The subtasks have the numbers 1, 2, 3 and so on, in their order. A title is one line. No line of a title or instructions starts with BEGIN DATA or END DATA.`,
    '"needsApproval": true makes the subtask wait for the owner before it starts. "model" is optional: one model of the agent.',
    `The agents that you can use: ${agents.join(", ") || "none"}.`,
  ];
  if (answered) {
    lines.push(
      "Your questions are in the clarifying_questions data block. The answers of the owner are in more owner_message blocks.",
    );
  }
  lines.push(block("owner_message", newId(), state.text ?? ""));
  const messages: CallMessage[] = [
    { role: "system", content: prompt.text },
    { role: "user", content: lines.join("\n") },
  ];
  if (answered) {
    messages.push(
      {
        role: "user",
        content: block("clarifying_questions", newId(), (state.questions ?? []).join("\n")),
      },
      ...ownerBlocks(state.answers, newId),
    );
  }
  if (retry?.output !== undefined && retry.output.trim() !== "") {
    messages.push({ role: "assistant", content: retry.output.slice(0, 100_000) });
  }
  if (retry !== undefined) {
    messages.push({
      role: "user",
      content: `Orchestration refused your last answer: ${retry.reason} Give a new answer as one JSON object only.`,
    });
  }
  return messages;
}

const failed = (state: TaskState, code: ErrorCode, why: string, env: RunEnv): TaskEvent[] => [
  event("orchestration.RunFailed", {
    taskId: state.taskId,
    runId: env.newId(),
    code,
    reason: reason(why),
  }),
];

export function planCall(
  state: TaskState,
  load: AgentLoad | undefined,
  env: RunEnv,
  retry?: Retry,
): TaskEvent[] {
  const prepared = prepareCall(
    state,
    load,
    {
      kind: "plan",
      slots: 1,
      messages: (prompt, agent) =>
        planMessages(
          state,
          prompt,
          maxSubtasks(state, agent.limits, env.settings),
          env.settings.planAgents ?? [],
          env.newId,
          retry,
        ),
    },
    env,
  );
  if ("call" in prepared) return [event("orchestration.CallRequested", prepared.call)];
  if ("fail" in prepared) return failed(state, prepared.fail.code, prepared.fail.reason, env);
  const why = "stop" in prepared ? prepared.stop : "The task has no money left for the plan.";
  return failed(state, "runaway_stopped", why, env);
}

const PlannerSubtask = z.object({
  title: z.string(),
  instructions: z.string(),
  agent: z.string(),
  dependsOn: z.array(z.int()).max(19).default([]),
  needsApproval: z.boolean().default(false),
  model: z.string().exactOptional(),
});
const PlannerSubtasks = z.array(PlannerSubtask).min(1).max(20);
const Questions = ClarificationRequestedEventPayloadV1.shape.questions;

type PlanResult =
  | { questions: string[] }
  | { subtasks: PlanSubtask[] }
  | { refused: string }
  | { failed: LoadFailure };

const issueText = (error: z.ZodError, root: "subtask" | "question"): string => {
  const [issue] = error.issues;
  const [first, ...rest] = issue?.path ?? [];
  const where =
    typeof first === "number"
      ? [`${root} ${String(first + 1)}`, ...rest.map(String)].join(" ")
      : `the ${root}s`;
  return `The value at ${where} is not valid: ${issue?.message ?? "not known"}.`;
};

function namedAgents(text: string): unknown[] {
  const output = readJson(text);
  if (typeof output !== "object" || output === null || !("subtasks" in output)) return [];
  return Array.isArray(output.subtasks) ? (output.subtasks as unknown[]) : [];
}

export function agentNames(text: string, allowed: readonly string[]): string[] {
  const names = namedAgents(text).flatMap((item) =>
    typeof item === "object" && item !== null && "agent" in item && typeof item.agent === "string"
      ? [item.agent]
      : [],
  );
  return [...new Set(names)].filter((name) => allowed.includes(name));
}

function readPlan(state: TaskState, text: string, env: RunEnv): PlanResult {
  const output = readJson(text);
  if (typeof output !== "object" || output === null || Array.isArray(output)) {
    return { refused: "The answer is not one JSON object." };
  }
  const keys = Object.keys(output);
  if (keys.length !== 1 || (keys[0] !== "questions" && keys[0] !== "subtasks")) {
    return { refused: "The JSON object must have one key: questions or subtasks." };
  }
  if ("questions" in output) {
    if (!mayAsk(state)) {
      return { refused: "Questions are not permitted for this task now. Give a plan." };
    }
    const questions = Questions.safeParse(output.questions);
    return questions.success
      ? { questions: questions.data }
      : { refused: issueText(questions.error, "question") };
  }
  const items = PlannerSubtasks.safeParse((output as { subtasks?: unknown }).subtasks);
  if (!items.success) return { refused: issueText(items.error, "subtask") };
  const allowed = env.settings.planAgents ?? [];
  const ids = items.data.map(() => env.newId());
  const subtasks: PlanSubtask[] = [];
  for (const [index, item] of items.data.entries()) {
    const which = `Subtask ${String(index + 1)}`;
    const load = allowed.includes(item.agent) ? env.named?.get(item.agent) : undefined;
    if (load === undefined) return { refused: `${which} names an agent that is not on the list.` };
    if (!("agent" in load)) {
      return load.code === "not_found"
        ? { refused: `${which} names an agent that has no active version.` }
        : { failed: load };
    }
    const dependsOn: string[] = [];
    for (const number of item.dependsOn) {
      const id = ids[number - 1];
      if (id === undefined) {
        return { refused: `${which} depends on a number that is not a subtask of the plan.` };
      }
      dependsOn.push(id);
    }
    subtasks.push({
      subtaskId: ids[index] ?? env.newId(),
      title: item.title,
      instructions: item.instructions,
      agentId: load.agent.agentId,
      ...(item.model === undefined ? {} : { model: item.model }),
      dependsOn,
      needsApproval: item.needsApproval,
    });
  }
  return checkPlan(state, subtasks, env.agents, env.settings);
}

export type PlanCheck = { subtasks: PlanSubtask[] } | { refused: string } | { failed: LoadFailure };

const marker = /^\s*(BEGIN|END) DATA /m;
const lineBreak = /[\n\r\u2028\u2029]/;

export function checkPlan(
  state: TaskState,
  plan: readonly PlanSubtask[],
  agents: ReadonlyMap<string, AgentLoad>,
  settings: Pick<RunSettings, "maxCallsPerTask">,
): PlanCheck {
  const subtasks: PlanSubtask[] = [];
  const counts = new Map<string, number>();
  for (const [index, subtask] of plan.entries()) {
    const which = `Subtask ${String(index + 1)}`;
    if (lineBreak.test(subtask.title))
      return { refused: `${which} has a title with a line break.` };
    if (marker.test(subtask.title) || marker.test(subtask.instructions)) {
      return { refused: `${which} has a line that starts with BEGIN DATA or END DATA.` };
    }
    const load = agents.get(subtask.agentId);
    if (load === undefined) return { refused: `${which} names an agent that is not known.` };
    if (!("agent" in load)) {
      return load.code === "not_found"
        ? { refused: `${which} names an agent that has no active version.` }
        : { failed: load };
    }
    const { agent, prompt } = load;
    if (!agent.enabled) return { refused: `${which} names an agent that is off.` };
    if (subtask.model !== undefined && !agent.models.includes(subtask.model)) {
      return { refused: `${which} names a model that its agent does not use.` };
    }
    if (prompt.variables.length > 0) {
      return { refused: `${which} names an agent whose prompt has variables.` };
    }
    const count = (counts.get(agent.agentId) ?? 0) + 1;
    if (count > agent.limits.maxCalls) {
      return {
        refused: `The plan has more subtasks for the agent ${agent.name} than its limit of ${String(agent.limits.maxCalls)} calls.`,
      };
    }
    counts.set(agent.agentId, count);
    subtasks.push({
      ...subtask,
      estimatedCostMicroUsd: estimate(agent.limits),
      estimatedDurationMs: callDurationMs,
    });
  }
  const parsed = PlanSubtasks.safeParse(subtasks);
  if (!parsed.success) return { refused: issueText(parsed.error, "subtask") };
  return checkBudget(state, parsed.data, agents, settings);
}

function checkBudget(
  state: TaskState,
  subtasks: PlanSubtask[],
  agents: ReadonlyMap<string, AgentLoad>,
  settings: Pick<RunSettings, "maxCallsPerTask">,
): PlanCheck {
  const load = state.orchestratorId === undefined ? undefined : agents.get(state.orchestratorId);
  if (load === undefined || !("agent" in load)) {
    return {
      failed: load ?? { code: "internal", reason: "The orchestrator agent is not known." },
    };
  }
  const { limits } = load.agent;
  const calls = Object.keys(state.calls).length + subtasks.length + reworkCalls;
  if (calls > settings.maxCallsPerTask) {
    return {
      refused: `The plan needs more calls than the limit of a task (${String(settings.maxCallsPerTask)}). Use fewer subtasks.`,
    };
  }
  const estimates = subtasks.map((subtask) => subtask.estimatedCostMicroUsd ?? 0);
  const total =
    estimates.reduce((sum, value) => sum + value, 0) +
    Math.max(...estimates) +
    3 * estimate(limits);
  const rest = limits.maxCostMicroUsd - state.costMicroUsd;
  if (total > rest) {
    return {
      refused: `The plan needs up to ${String(total)} micro-USD with the final answer, its check and one rework, but the task has ${String(rest)} micro-USD left. Use fewer subtasks.`,
    };
  }
  return { subtasks };
}

function throwIfUnavailable(failure: LoadFailure): void {
  if (failure.code === "unavailable") throw new AgentUnavailableError(failure.reason);
}

type FinalCall = { completed: CallCompletedEventPayloadV1 } | { failed: CallFailedEventPayloadV1 };

export function settlePlan(state: TaskState, final: FinalCall, env: RunEnv): TaskEvent[] {
  const load =
    state.orchestratorId === undefined ? undefined : env.agents.get(state.orchestratorId);
  const again = (retry?: Retry): TaskEvent[] | undefined => {
    if (state.planTries >= maxPlanTries) return undefined;
    if (load !== undefined && !("agent" in load)) throwIfUnavailable(load);
    return planCall(state, load, env, retry);
  };
  if ("failed" in final) {
    const { errorClass, reason: why } = final.failed;
    if (errorClass === "budget_refused") {
      return failed(state, "budget_refused", `The budget refused the plan call: ${why}`, env);
    }
    const retryable =
      errorClass === "rate_limit" || errorClass === "timeout" || errorClass === "provider";
    return (
      (retryable ? again() : undefined) ??
      failed(state, failedCodes[errorClass], `The plan call failed: ${why}`, env)
    );
  }
  const { text } = final.completed;
  const result = readPlan(state, text, env);
  if ("failed" in result) {
    throwIfUnavailable(result.failed);
    return failed(state, result.failed.code, result.failed.reason, env);
  }
  if ("refused" in result) {
    return (
      again({ reason: result.refused, output: text }) ??
      failed(
        state,
        "validation_failed",
        `The orchestrator gave no valid plan in ${String(maxPlanTries)} tries. The last reason: ${result.refused}`,
        env,
      )
    );
  }
  const { taskId } = state;
  if ("questions" in result) {
    return [event("orchestration.ClarificationRequested", { taskId, questions: result.questions })];
  }
  const planVersion = (state.plan?.planVersion ?? 0) + 1;
  const proposed = event("orchestration.PlanProposed", {
    taskId,
    planVersion,
    subtasks: result.subtasks,
    awaitsApproval: state.mode !== "auto",
  });
  if (state.mode !== "auto") return [proposed];
  const approved = event("orchestration.PlanApproved", { taskId, planVersion });
  return [
    proposed,
    approved,
    ...decideRun(fold(state, [proposed, approved]), { type: "StartRun", runId: env.newId() }, env),
  ];
}
