import {
  CheckpointMcpRestoreInput,
  CheckpointMcpRestoreResult,
  CheckpointMcpDiffInput,
  CheckpointMcpDiffResult,
  CheckpointMcpFailure,
  CheckpointMcpListInput,
  CheckpointMcpListResult,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import { Tool, Toolkit } from "effect/ai";
import * as Schema from "effect/Schema";
import * as CheckpointMcpService from "../../CheckpointMcpService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ThreadManagement.ThreadManagementService,
  CheckpointMcpService.CheckpointMcpService,
];
const failure = Schema.Union([CheckpointMcpFailure, OrchestratorMcpFailure]);
const list = Tool.make("t3_checkpoint_list", {
  description:
    "List durable checkpoints in a thread, newest first, with exact checkpoint and scope identities, bounded file summaries, ref availability and restore support. Omit threadId for this thread; thread callers can inspect only their project. Continue with nextCursor.",
  parameters: CheckpointMcpListInput,
  success: CheckpointMcpListResult,
  failure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);
const diff = Tool.make("t3_checkpoint_diff", {
  description:
    "Read a checkpoint patch using exact identities from t3_checkpoint_list. Baseline must belong to the same scope. Continue with nextCursor until null to recover the complete diff. Cursors count UTF-16 code units and never split surrogate pairs; oversized patches fail instead of silently truncating.",
  parameters: CheckpointMcpDiffInput,
  success: CheckpointMcpDiffResult,
  failure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true);
const restore = Tool.make("t3_checkpoint_restore", {
  description:
    "Restore an idle thread to an exact checkpoint from t3_checkpoint_list, discarding workspace changes. Thread callers must pass another idle threadId because their own thread is running. Requires discardChanges:true and a stable clientRequestId. REQUESTED is acceptance, not completion: repeat the same request to read APPLIED, FAILED or PARTIAL. A partial restore is never replayed. Only isolated worktrees and providers supporting conversation rollback and snapshots are supported.",
  parameters: CheckpointMcpRestoreInput,
  success: CheckpointMcpRestoreResult,
  failure,
  failureMode: "return",
  dependencies,
})
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true);
export const CheckpointToolkit = Toolkit.make(list, diff, restore);
