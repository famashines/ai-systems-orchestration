import {
  Actor,
  CommandEnvelopeV1,
  SubmitTaskCommandPayloadV1,
} from "@famashines/ai-systems-contracts";
import {
  ConflictError,
  UnavailableError,
  validate,
  type Handler,
  type Route,
} from "@famashines/ai-systems-kit/http";
import { isInfrastructure } from "@famashines/ai-systems-kit/messaging";
import { z } from "zod";
import { submitTask, type Deps } from "../application/submit-task.ts";
import { TaskExistsError } from "../domain/task.ts";

const SubmitTaskBody = CommandEnvelopeV1(SubmitTaskCommandPayloadV1).extend({
  type: z.literal("orchestration.SubmitTask"),
  expectedVersion: z.literal(0).exactOptional(),
  actor: Actor.extend({ kind: z.literal("owner") }),
});

const count = (fallback: string, schema: z.ZodInt) =>
  z.string().regex(/^\d+$/).default(fallback).transform(Number).pipe(schema);

const EventsQuery = z.object({
  afterPosition: count("0", z.int().min(0)),
  limit: count("100", z.int().min(1).max(1000)),
});

export function toHttpError(error: unknown): unknown {
  if (error instanceof TaskExistsError) return new ConflictError(error.message);
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

export function routes(deps: Deps): Route[] {
  return [
    {
      method: "POST",
      path: "/internal/commands/submit-task",
      handler: guard(async (request) => {
        const { body } = await validate(request, { body: SubmitTaskBody });
        const { answer, replayed } = await submitTask(deps, body);
        return { status: replayed ? 200 : 201, body: answer };
      }),
    },
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
