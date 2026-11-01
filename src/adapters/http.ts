import {
  Actor,
  ApprovePlanCommandPayloadV1,
  CancelRunCommandPayloadV1,
  CancelTaskCommandPayloadV1,
  CommandEnvelopeV1,
  EditPlanCommandPayloadV1,
  PauseRunCommandPayloadV1,
  ReassignSubtaskCommandPayloadV1,
  RejectPlanCommandPayloadV1,
  ResolveCheckpointCommandPayloadV1,
  ResumeRunCommandPayloadV1,
  RetrySubtaskCommandPayloadV1,
  SetTaskModeCommandPayloadV1,
  SubmitTaskCommandPayloadV1,
} from "@famashines/ai-systems-contracts";
import { bearerHook, subjectOf, type JwtVerifier } from "@famashines/ai-systems-kit/auth";
import { ConcurrencyError } from "@famashines/ai-systems-kit/event-store";
import {
  ConflictError,
  ForbiddenError,
  UnavailableError,
  validate,
  ValidationError,
  type Handler,
  type Request,
  type Route,
} from "@famashines/ai-systems-kit/http";
import { isInfrastructure } from "@famashines/ai-systems-kit/messaging";
import { z } from "zod";
import { act, system, type Act, type TaskDeps } from "../application/run.ts";
import { submitTask } from "../application/submit-task.ts";
import {
  AgentUnavailableError,
  PlanRefusedError,
  TaskConflictError,
  TaskExistsError,
  type TaskCommand,
} from "../domain/task.ts";

const owner = Actor.extend({ kind: z.literal("owner") });

const SubmitTaskBody = CommandEnvelopeV1(SubmitTaskCommandPayloadV1).extend({
  type: z.literal("orchestration.SubmitTask"),
  expectedVersion: z.literal(0).exactOptional(),
  actor: owner,
});

const ownerCommands = {
  ApprovePlan: ApprovePlanCommandPayloadV1,
  EditPlan: EditPlanCommandPayloadV1,
  RejectPlan: RejectPlanCommandPayloadV1,
  PauseRun: PauseRunCommandPayloadV1,
  ResumeRun: ResumeRunCommandPayloadV1,
  CancelRun: CancelRunCommandPayloadV1,
  CancelTask: CancelTaskCommandPayloadV1,
  RetrySubtask: RetrySubtaskCommandPayloadV1,
  ReassignSubtask: ReassignSubtaskCommandPayloadV1,
  SetTaskMode: SetTaskModeCommandPayloadV1,
  ResolveCheckpoint: ResolveCheckpointCommandPayloadV1,
};

export const commandPath = (name: string): string =>
  `/internal/commands/${name.replace(/(?<!^)[A-Z]/g, "-$&").toLowerCase()}`;

const count = (fallback: string, schema: z.ZodInt) =>
  z.string().regex(/^\d+$/).default(fallback).transform(Number).pipe(schema);

const EventsQuery = z.object({
  afterPosition: count("0", z.int().min(0)),
  limit: count("100", z.int().min(1).max(1000)),
});

export function toHttpError(error: unknown): unknown {
  if (error instanceof TaskExistsError || error instanceof TaskConflictError) {
    return new ConflictError(error.message);
  }
  if (error instanceof PlanRefusedError) {
    return new ValidationError(error.message, [
      { path: "body.payload", code: "custom", message: error.message },
    ]);
  }
  if (error instanceof AgentUnavailableError) {
    return new UnavailableError("agent-manager or prompt-studio did not answer");
  }
  if (isInfrastructure(error)) return new UnavailableError("The database is not available");
  return error;
}

const guard =
  (handler: Handler): Handler =>
  async (request) => {
    try {
      return await handler(request);
    } catch (error) {
      throw toHttpError(error);
    }
  };

export interface HttpDeps extends TaskDeps {
  verify: JwtVerifier;
}

const concurrencyTries = 3;

async function actAgain(deps: TaskDeps, input: Act) {
  for (let attempt = 1; ; attempt += 1) {
    try {
      return await act(deps, input);
    } catch (error) {
      if (!(error instanceof ConcurrencyError) || attempt === concurrencyTries) throw error;
    }
  }
}

function checkActor(request: Request, actor: { id: string }): void {
  if (actor.id !== subjectOf(request).sub) {
    throw new ForbiddenError("The actor is not the subject of the token");
  }
}

export function routes(deps: HttpDeps): Route[] {
  const checkToken = bearerHook(deps.verify);
  const commandRoutes = Object.entries(ownerCommands).map(([name, payload]): Route => ({
    method: "POST",
    path: commandPath(name),
    handler: guard(async (request) => {
      await checkToken(request);
      const { body } = await validate(request, {
        body: CommandEnvelopeV1(payload).extend({
          type: z.literal(`orchestration.${name}`),
          expectedVersion: z.never().exactOptional(),
          actor: owner,
        }),
      });
      checkActor(request, body.actor);
      const { taskId, ...input } = body.payload;
      const { answer, replayed } = await actAgain(deps, {
        taskId,
        causationId: body.commandId,
        command: { type: name, ...input } as TaskCommand,
        actorOf: (_event, decided) => (decided ? body.actor : system),
        idempotent: true,
      });
      return { status: replayed ? 200 : 201, body: answer };
    }),
  }));
  return [
    {
      method: "POST",
      path: "/internal/commands/submit-task",
      handler: guard(async (request) => {
        await checkToken(request);
        const { body } = await validate(request, { body: SubmitTaskBody });
        checkActor(request, body.actor);
        const { answer, replayed } = await submitTask(deps, body);
        return { status: replayed ? 200 : 201, body: answer };
      }),
    },
    ...commandRoutes,
    {
      method: "GET",
      path: "/internal/events",
      handler: guard(async (request) => {
        const { query } = await validate(request, { query: EventsQuery });
        return { body: { events: await deps.store.readAll(query) } };
      }),
    },
  ];
}
