import * as Effect from "effect/Effect";
import * as CheckpointMcpService from "../../CheckpointMcpService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { CheckpointToolkit } from "./tools.ts";

const handlers = {
  t3_checkpoint_restore: McpToolAccess.writesThreads(
    (input) => [input.threadId],
    (input) =>
      Effect.gen(function* () {
        const scope = yield* McpInvocationContext.McpInvocationContext;
        const service = yield* CheckpointMcpService.CheckpointMcpService;
        return yield* service.restore(scope, input);
      }),
  ),
  t3_checkpoint_list: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const service = yield* CheckpointMcpService.CheckpointMcpService;
      return yield* service.list(scope, input);
    }),
  ),
  t3_checkpoint_diff: McpToolAccess.reads((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const service = yield* CheckpointMcpService.CheckpointMcpService;
      return yield* service.diff(scope, input);
    }),
  ),
} satisfies McpToolAccess.Handlers<typeof CheckpointToolkit.tools>;
export const layer = McpToolAccess.toLayer(CheckpointToolkit, handlers);
