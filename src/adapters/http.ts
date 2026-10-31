import { CommandEnvelopeV1, SubmitTaskCommandPayloadV1 } from "@famashines/ai-systems-contracts";
import { validate, type Route } from "@famashines/ai-systems-kit/http";
import { z } from "zod";
import { submitTask, type Deps } from "../application/submit-task.ts";

const SubmitTaskBody = CommandEnvelopeV1(SubmitTaskCommandPayloadV1).extend({
  type: z.literal("orchestration.SubmitTask"),
  expectedVersion: z.literal(0).exactOptional(),
});

const EventsQuery = z.object({
  afterPosition: z.coerce.number().int().min(0).default(0),
  limit: z.coerce.number().int().min(1).max(1000).default(100),
});

export function routes(deps: Deps): Route[] {
  return [
    {
      method: "POST",
      path: "/internal/commands/submit-task",
      handler: async (request) => {
        const { body } = await validate(request, { body: SubmitTaskBody });
        const { answer, replayed } = await submitTask(deps, body);
        return { status: replayed ? 200 : 201, body: answer };
      },
    },
    {
      method: "GET",
      path: "/internal/events",
      handler: async (request) => {
        const { query } = await validate(request, { query: EventsQuery });
        return { body: { events: await deps.store.readAll(query) } };
      },
    },
  ];
}
