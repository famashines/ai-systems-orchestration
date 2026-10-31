import type { StreamSpec } from "@famashines/ai-systems-kit/testing";

interface User {
  publish: string[];
  subscribe: string[];
  allowResponses?: boolean;
}

const reader = (stream: string, durable: string, filter: string): string[] => [
  `$JS.API.STREAM.INFO.${stream}`,
  `$JS.API.CONSUMER.CREATE.${stream}.${durable}.${filter}`,
  `$JS.API.CONSUMER.INFO.${stream}.${durable}`,
  `$JS.API.CONSUMER.MSG.NEXT.${stream}.${durable}`,
  `$JS.ACK.${stream}.${durable}.>`,
];

export const users: Record<string, User> = {
  admin: {
    publish: [
      "$JS.API.>",
      "$JS.ACK.>",
      "orchestration.>",
      "conversation.>",
      "llm.>",
      "budget.>",
      "agents.>",
      "prompts.>",
      "dlq.>",
    ],
    subscribe: ["_INBOX_admin.>"],
  },
  orchestration: {
    publish: [
      "orchestration.>",
      "dlq.orchestration.>",
      "$JS.API.INFO",
      "$JS.API.STREAM.INFO.ORCHESTRATION",
      ...reader("CONVERSATION", "orchestration", "conversation.OwnerMessagePosted"),
      ...reader("LLM", "orchestration", "llm.>"),
    ],
    subscribe: ["_INBOX_orchestration.>"],
  },
  "ai-proxy": {
    publish: [
      "llm.>",
      "delta.>",
      "rpc.budget.reserve",
      "rpc.budget.settle",
      "dlq.ai-proxy.>",
      "$JS.API.INFO",
      "$JS.API.STREAM.INFO.LLM",
      ...reader("ORCHESTRATION", "ai-proxy", "orchestration.CallRequested"),
    ],
    subscribe: ["_INBOX_ai-proxy.>"],
  },
  budget: {
    publish: ["budget.>", "dlq.budget.>", "$JS.API.INFO", "$JS.API.STREAM.INFO.BUDGET"],
    subscribe: ["_INBOX_budget.>", "rpc.budget.reserve", "rpc.budget.settle"],
    allowResponses: true,
  },
  "agent-manager": {
    publish: ["agents.>", "dlq.agent-manager.>", "$JS.API.INFO", "$JS.API.STREAM.INFO.AGENTS"],
    subscribe: ["_INBOX_agent-manager.>"],
  },
  "prompt-studio": {
    publish: ["prompts.>", "dlq.prompt-studio.>", "$JS.API.INFO", "$JS.API.STREAM.INFO.PROMPTS"],
    subscribe: ["_INBOX_prompt-studio.>"],
  },
};

const list = (subjects: string[]): string => subjects.map((s) => `"${s}"`).join(", ");

export const password = (name: string): string => `${name}-password`;

export const natsConfig = (names: string[]): string => `
jetstream { store_dir: /data }
accounts {
  AISYSTEMS {
    jetstream { max_file: 1073741824, max_memory: 0 }
    users = [
${names
  .map((name) => {
    const user = users[name];
    if (!user) throw new Error(`No grants for ${name}`);
    return `      {
        user: ai-systems-${name}, password: ${password(name)}
        permissions {
          publish { allow: [${list(user.publish)}] }
          subscribe { allow: [${list(user.subscribe)}] }
          ${user.allowResponses === true ? "allow_responses: true" : ""}
        }
      }`;
  })
  .join("\n")}
    ]
  }
}
`;

export const streams: StreamSpec[] = [
  "ORCHESTRATION",
  "CONVERSATION",
  "LLM",
  "BUDGET",
  "AGENTS",
  "PROMPTS",
  "DLQ",
].map((name) => ({ name, subjects: [`${name.toLowerCase()}.>`] }));
