import {
  CommandId,
  CheckpointMcpFailure,
  type CheckpointMcpRestoreInput,
  type CheckpointMcpRestoreResult,
  checkpointRollbackAppRunOrdinal,
  latestProviderTurnForAttempt,
  type CheckpointMcpDiffInput,
  type CheckpointMcpDiffResult,
  type CheckpointMcpListInput,
  type CheckpointMcpListResult,
  type CheckpointMcpRestoreBlocker,
  type CheckpointMcpSummary,
  type OrchestrationV2Checkpoint,
  type OrchestrationV2CheckpointScope,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";

import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import {
  checkpointRestoreThreadState,
  CheckpointRestoreRejectedRequest,
  isCheckpointRestoreIsolated,
} from "../orchestration-v2/CheckpointRestoreSafety.ts";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import type { McpInvocationScope } from "./McpInvocationContext.ts";

const decodeRejectedRestore = Schema.decodeUnknownOption(CheckpointRestoreRejectedRequest);
const encodeRejectedRestore = Schema.encodeSync(CheckpointRestoreRejectedRequest);

const DEFAULT_LIST_LIMIT = 25;
const DEFAULT_FILE_LIMIT = 20;
const DEFAULT_DIFF_LIMIT = 20_000;

export class CheckpointMcpService extends Context.Service<
  CheckpointMcpService,
  {
    readonly restore: (
      scope: McpInvocationScope,
      input: CheckpointMcpRestoreInput,
    ) => Effect.Effect<CheckpointMcpRestoreResult, CheckpointMcpFailure>;
    readonly list: (
      scope: McpInvocationScope,
      input: CheckpointMcpListInput,
    ) => Effect.Effect<CheckpointMcpListResult, CheckpointMcpFailure>;
    readonly diff: (
      scope: McpInvocationScope,
      input: CheckpointMcpDiffInput,
    ) => Effect.Effect<CheckpointMcpDiffResult, CheckpointMcpFailure>;
  }
>()("t3/mcp/CheckpointMcpService") {}

function failure(code: CheckpointMcpFailure["code"], message: string): CheckpointMcpFailure {
  return new CheckpointMcpFailure({ code, message });
}

type CheckpointProjection = Pick<
  OrchestrationV2ThreadProjection,
  | "thread"
  | "runs"
  | "attempts"
  | "providerTurns"
  | "providerThreads"
  | "providerSessions"
  | "checkpoints"
  | "checkpointScopes"
>;

function providerTurnForRun(projection: CheckpointProjection, runOrdinal: number) {
  const run = projection.runs.find((candidate) => candidate.ordinal === runOrdinal);
  if (run?.activeAttemptId === null || run === undefined) return undefined;
  return (
    latestProviderTurnForAttempt(projection.providerTurns, run.activeAttemptId) ??
    projection.providerTurns.find((turn) => {
      const attempt = projection.attempts.find((candidate) => candidate.id === run.activeAttemptId);
      return attempt?.providerTurnId === turn.id;
    })
  );
}

function restoreBlockers(input: {
  readonly projection: CheckpointProjection;
  readonly checkpoint: OrchestrationV2Checkpoint;
  readonly scope: OrchestrationV2CheckpointScope | undefined;
  readonly refAvailable: boolean;
}): Array<CheckpointMcpRestoreBlocker> {
  const { projection, checkpoint, scope } = input;
  const blockers = new Set<CheckpointMcpRestoreBlocker>();
  if (projection.thread.archivedAt !== null) blockers.add("thread_archived");
  if (
    projection.runs.some((run) =>
      ["preparing", "queued", "starting", "running", "waiting"].includes(run.status),
    )
  ) {
    blockers.add("thread_active");
  }
  if (checkpoint.status !== "ready") blockers.add("checkpoint_not_ready");
  if (scope === undefined) blockers.add("scope_missing");
  if (!input.refAvailable) blockers.add("ref_unavailable");

  const providerThread = projection.providerThreads.find(
    (candidate) => candidate.id === projection.thread.activeProviderThreadId,
  );
  if (providerThread === undefined) {
    blockers.add("active_provider_thread_missing");
    return [...blockers];
  }
  if (scope?.providerThreadId != null && scope.providerThreadId !== providerThread.id)
    blockers.add("provider_thread_mismatch");
  if (providerThread.providerSessionId === null) {
    blockers.add("provider_session_missing");
    return [...blockers];
  }
  if (providerThread.providerInstanceId !== projection.thread.modelSelection.instanceId) {
    blockers.add("provider_thread_mismatch");
  }
  const providerSession = projection.providerSessions.find(
    (candidate) => candidate.id === providerThread.providerSessionId,
  );
  if (providerSession === undefined) {
    blockers.add("provider_session_missing");
    return [...blockers];
  }
  if (
    !providerSession.capabilities.threads.canRollbackThread ||
    !providerSession.capabilities.checkpointing.providerCanRollbackConversation
  ) {
    blockers.add("provider_rollback_unsupported");
  }
  if (!providerSession.capabilities.checkpointing.providerRollbackReturnsSnapshot) {
    blockers.add("provider_snapshot_unsupported");
  }

  const runOrdinal =
    scope === undefined ? null : checkpointRollbackAppRunOrdinal(checkpoint, scope);
  if (runOrdinal === null) {
    blockers.add("rollback_target_ambiguous");
    return [...blockers];
  }
  if (runOrdinal > 0) {
    const providerTurn = providerTurnForRun(projection, runOrdinal);
    if (providerTurn === undefined) {
      blockers.add("provider_turn_missing");
    } else if (providerTurn.providerThreadId !== providerThread.id) {
      blockers.add("provider_thread_mismatch");
    }
  }
  return [...blockers];
}

function isSurrogateBoundary(value: string, cursor: number): boolean {
  if (cursor <= 0 || cursor >= value.length) return false;
  const before = value.charCodeAt(cursor - 1);
  const after = value.charCodeAt(cursor);
  return before >= 0xd800 && before <= 0xdbff && after >= 0xdc00 && after <= 0xdfff;
}

const make = Effect.gen(function* () {
  const threadManagement = yield* ThreadManagement.ThreadManagementService;
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const projects = yield* ProjectStore.ProjectStoreV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const checkpointStore = yield* CheckpointStore.CheckpointStore;
  const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;

  const requireCapability = (scope: McpInvocationScope) =>
    scope.capabilities.has("orchestration")
      ? Effect.void
      : Effect.fail(
          failure(
            "capability_denied",
            "This MCP credential does not grant orchestration capabilities.",
          ),
        );

  const loadThread = Effect.fn("CheckpointMcpService.loadThread")(function* (
    scope: McpInvocationScope,
    requestedThreadId: CheckpointMcpListInput["threadId"],
  ) {
    const threadId = requestedThreadId ?? scope.thread?.threadId;
    if (threadId === undefined)
      return yield* failure("invalid_request", "Pass threadId when calling outside a T3 thread.");
    const target = yield* threadManagement
      .getThreadShell(threadId)
      .pipe(Effect.mapError(() => failure("operation_failed", "Unable to load thread.")));
    if (target === null || target.deletedAt !== null)
      return yield* failure("thread_not_found", "Thread was not found.");
    if (scope.thread !== undefined) {
      const caller = yield* threadManagement
        .getThreadShell(scope.thread.threadId)
        .pipe(Effect.mapError(() => failure("operation_failed", "Unable to load calling thread.")));
      if (caller === null || caller.deletedAt !== null || caller.projectId !== target.projectId) {
        return yield* failure(
          "thread_not_found",
          "Thread was not found in the calling thread's project.",
        );
      }
    }
    return yield* threadManagement
      .getProjectThreadRecords(
        { projectId: target.projectId, threadId },
        [
          "providerThreads",
          "providerSessions",
          "checkpoints",
          "checkpointScopes",
          "runs",
          "attempts",
          "providerTurns",
        ],
        { turnItemTypes: [] },
      )
      .pipe(
        Effect.mapError(() => failure("operation_failed", "Unable to load checkpoint records.")),
      );
  });

  const readAvailability = Effect.fn("CheckpointMcpService.readAvailability")(function* (
    checkpoint: OrchestrationV2Checkpoint,
    scope: OrchestrationV2CheckpointScope | undefined,
  ) {
    if (scope === undefined) {
      return {
        availability: "metadata_incomplete" as const,
        detail: `Checkpoint scope '${checkpoint.scopeId}' is missing.`,
        refAvailable: false,
      };
    }
    if (checkpoint.status === "missing") {
      return {
        availability: "ref_missing" as const,
        detail: "The durable checkpoint metadata records a missing filesystem ref.",
        refAvailable: false,
      };
    }
    if (checkpoint.status !== "ready") {
      return {
        availability: "metadata_incomplete" as const,
        detail: `Checkpoint status is '${checkpoint.status}'.`,
        refAvailable: false,
      };
    }
    return yield* checkpointStore
      .hasCheckpointRef({ cwd: scope.cwd, checkpointRef: checkpoint.ref })
      .pipe(
        Effect.match({
          onFailure: () => ({
            availability: "unreadable" as const,
            detail: "Unable to read checkpoint ref.",
            refAvailable: false,
          }),
          onSuccess: (exists) => ({
            availability: exists ? ("available" as const) : ("ref_missing" as const),
            detail: exists ? null : "The checkpoint filesystem ref does not exist.",
            refAvailable: exists,
          }),
        }),
      );
  });

  const list: CheckpointMcpService["Service"]["list"] = Effect.fn("CheckpointMcpService.list")(
    function* (scope, input) {
      yield* requireCapability(scope);
      const projection = yield* loadThread(scope, input.threadId);
      const cursor = input.cursor ?? 0;
      const limit = input.limit ?? DEFAULT_LIST_LIMIT;
      const fileLimit = input.fileLimit ?? DEFAULT_FILE_LIMIT;
      const ordered = [...projection.checkpoints].sort(
        (left, right) =>
          DateTime.toEpochMillis(right.capturedAt) - DateTime.toEpochMillis(left.capturedAt) ||
          right.ordinalWithinScope - left.ordinalWithinScope ||
          left.id.localeCompare(right.id),
      );
      if (cursor > ordered.length) {
        return yield* failure(
          "invalid_request",
          `cursor ${cursor} exceeds the checkpoint count ${ordered.length}.`,
        );
      }
      const selected = ordered.slice(cursor, cursor + limit);
      const checkpoints = yield* Effect.forEach(
        selected,
        (checkpoint) =>
          Effect.gen(function* () {
            const scopeSummary = projection.checkpointScopes.find(
              (candidate) => candidate.id === checkpoint.scopeId,
            );
            const availability = yield* readAvailability(checkpoint, scopeSummary);
            {
              const blockers = restoreBlockers({
                projection,
                checkpoint,
                scope: scopeSummary,
                refAvailable: availability.refAvailable,
              });
              if (
                scopeSummary !== undefined &&
                !(yield* isCheckpointRestoreIsolated(projection.thread, scopeSummary, {
                  fileSystem,
                  path,
                  projects,
                  projections,
                }).pipe(Effect.orElseSucceed(() => false)))
              )
                blockers.push("shared_workspace");
              return {
                checkpointId: checkpoint.id,
                scopeId: checkpoint.scopeId,
                threadId: checkpoint.threadId,
                runId: checkpoint.runId,
                nodeId: checkpoint.nodeId,
                parentCheckpointId: checkpoint.parentCheckpointId,
                ordinalWithinScope: checkpoint.ordinalWithinScope,
                appRunOrdinal: checkpoint.appRunOrdinal,
                status: checkpoint.status,
                capturedAt: DateTime.formatIso(checkpoint.capturedAt),
                scope:
                  scopeSummary === undefined
                    ? null
                    : {
                        scopeId: scopeSummary.id,
                        runId: scopeSummary.runId,
                        nodeId: scopeSummary.nodeId,
                        parentScopeId: scopeSummary.parentScopeId,
                        providerThreadId: scopeSummary.providerThreadId,
                        kind: scopeSummary.kind,
                        ordinalWithinParent: scopeSummary.ordinalWithinParent,
                        advancesAppRunCount: scopeSummary.advancesAppRunCount,
                        workspacePath: scopeSummary.cwd,
                        createdAt: DateTime.formatIso(scopeSummary.createdAt),
                      },
                files: checkpoint.files.slice(0, fileLimit),
                fileCount: checkpoint.files.length,
                filesTruncated: checkpoint.files.length > fileLimit,
                availability: availability.availability,
                availabilityDetail: availability.detail,
                restoreSupport: { supported: blockers.length === 0, blockers },
              } satisfies CheckpointMcpSummary;
            }
          }),
        { concurrency: 4 },
      );
      const next = cursor + selected.length;
      return {
        currentThreadId: scope.thread?.threadId ?? null,
        threadId: projection.thread.id,
        checkpoints,
        nextCursor: next < ordered.length ? next : null,
        total: ordered.length,
      };
    },
  );

  const diff: CheckpointMcpService["Service"]["diff"] = Effect.fn("CheckpointMcpService.diff")(
    function* (scope, input) {
      yield* requireCapability(scope);
      const projection = yield* loadThread(scope, input.threadId);
      const checkpointScope = projection.checkpointScopes.find(
        (candidate) => candidate.id === input.scopeId,
      );
      if (checkpointScope === undefined || checkpointScope.threadId !== projection.thread.id) {
        return yield* failure(
          "scope_mismatch",
          `Checkpoint scope '${input.scopeId}' does not belong to thread '${projection.thread.id}'.`,
        );
      }
      const target = projection.checkpoints.find(
        (candidate) => candidate.id === input.checkpointId,
      );
      if (target === undefined || target.threadId !== projection.thread.id) {
        return yield* failure(
          "checkpoint_not_found",
          `Checkpoint '${input.checkpointId}' was not found on thread '${projection.thread.id}'.`,
        );
      }
      if (target.scopeId !== checkpointScope.id) {
        return yield* failure(
          "scope_mismatch",
          `Checkpoint '${target.id}' belongs to scope '${target.scopeId}', not '${checkpointScope.id}'.`,
        );
      }
      const fromCheckpointId = input.fromCheckpointId ?? target.parentCheckpointId ?? target.id;
      const from = projection.checkpoints.find((candidate) => candidate.id === fromCheckpointId);
      if (from === undefined || from.threadId !== projection.thread.id) {
        return yield* failure(
          "checkpoint_not_found",
          `Baseline checkpoint '${fromCheckpointId}' was not found on thread '${projection.thread.id}'.`,
        );
      }
      if (from.scopeId !== checkpointScope.id) {
        return yield* failure(
          "scope_mismatch",
          `Baseline checkpoint '${from.id}' belongs to scope '${from.scopeId}', not '${checkpointScope.id}'.`,
        );
      }
      if (target.status !== "ready" || from.status !== "ready") {
        return yield* failure(
          "checkpoint_unavailable",
          "Both checkpoints must have ready durable metadata before their diff can be read.",
        );
      }
      const targetAvailable = yield* checkpointStore
        .hasCheckpointRef({ cwd: checkpointScope.cwd, checkpointRef: target.ref })
        .pipe(
          Effect.mapError(() =>
            failure("operation_failed", `Unable to read target checkpoint ref: operation failed`),
          ),
        );
      const fromAvailable =
        from.id === target.id
          ? targetAvailable
          : yield* checkpointStore
              .hasCheckpointRef({ cwd: checkpointScope.cwd, checkpointRef: from.ref })
              .pipe(
                Effect.mapError(() =>
                  failure(
                    "operation_failed",
                    `Unable to read baseline checkpoint ref: operation failed`,
                  ),
                ),
              );
      if (!targetAvailable || !fromAvailable) {
        return yield* failure(
          "checkpoint_unavailable",
          "One or both checkpoint filesystem refs are missing.",
        );
      }

      const completeDiff =
        from.id === target.id
          ? ""
          : yield* checkpointStore
              .diffCheckpoints({
                cwd: checkpointScope.cwd,
                fromCheckpointRef: from.ref,
                toCheckpointRef: target.ref,
                fallbackFromToHead: false,
                ignoreWhitespace: input.ignoreWhitespace ?? true,
                requireCompleteOutput: true,
              })
              .pipe(
                Effect.mapError(() =>
                  failure("operation_failed", "Unable to compute checkpoint diff."),
                ),
              );
      const cursor = input.cursor ?? 0;
      if (cursor > completeDiff.length || isSurrogateBoundary(completeDiff, cursor)) {
        return yield* failure(
          "invalid_request",
          `cursor ${cursor} is not a valid UTF-16 boundary for this diff.`,
        );
      }
      const limit = input.limit ?? DEFAULT_DIFF_LIMIT;
      let end = Math.min(completeDiff.length, cursor + limit);
      if (isSurrogateBoundary(completeDiff, end)) end += 1;
      return {
        threadId: projection.thread.id,
        scopeId: checkpointScope.id,
        fromCheckpointId: from.id,
        checkpointId: target.id,
        diff: completeDiff.slice(cursor, end),
        cursor,
        nextCursor: end < completeDiff.length ? end : null,
        totalLength: completeDiff.length,
        truncated: cursor > 0 || end < completeDiff.length,
      };
    },
  );

  const restore: CheckpointMcpService["Service"]["restore"] = Effect.fn(
    "CheckpointMcpService.restore",
  )(function* (scope, input) {
    yield* requireCapability(scope);
    const projection = yield* loadThread(scope, input.threadId);
    const threadId = projection.thread.id;
    // The target is deliberately absent: a key cannot authorize a different restore on retry.
    const commandId = CommandId.make(
      [
        "command",
        "mcp",
        encodeURIComponent(scope.requestNamespace),
        "checkpoint-restore",
        encodeURIComponent(input.clientRequestId),
      ].join(":"),
    );
    const command = {
      type: "checkpoint.rollback" as const,
      commandId,
      threadId,
      scopeId: input.scopeId,
      checkpointId: input.checkpointId,
      restoreFiles: true,
      expectedIdle: true as const,
    };
    const conflict = () =>
      failure("idempotency_conflict", "This clientRequestId belongs to another restore target.");
    const uncertain = () =>
      failure(
        "operation_failed",
        "The restore outcome could not be confirmed. Retry with the same clientRequestId.",
      );
    const rejected = () =>
      failure(
        "restore_rejected",
        "This restore request was rejected. Inspect the target and use a new clientRequestId for a new attempt.",
      );
    const matches = (target: {
      readonly threadId: typeof threadId;
      readonly scopeId: typeof input.scopeId;
      readonly checkpointId: typeof input.checkpointId;
    }) =>
      target.threadId === threadId &&
      target.scopeId === input.scopeId &&
      target.checkpointId === input.checkpointId;
    const readReceipt = receipts.getByCommandId(commandId).pipe(Effect.mapError(uncertain));
    const observe = Effect.fn("CheckpointMcpService.observeRestore")(function* (
      receipt: CommandReceiptStore.CommandReceiptV2,
    ) {
      if (receipt.threadId !== threadId || receipt.commandType !== "checkpoint.rollback")
        return yield* conflict();
      if (receipt.status === "rejected") {
        const target = decodeRejectedRestore(receipt.error);
        if (Option.isNone(target) || !matches(target.value)) return yield* conflict();
        return yield* rejected();
      }
      const effects = yield* outbox.listByCommandId(commandId).pipe(Effect.option);
      const effect = Option.isSome(effects)
        ? effects.value.find((item) => item.request.type === "provider-thread.rollback")
        : undefined;
      if (effect?.request.type === "provider-thread.rollback") {
        if (!matches({ threadId: effect.threadId, ...effect.request })) return yield* conflict();
      } else {
        // Effects can be pruned. The immutable event still binds an accepted key to its target.
        const original = yield* threadManagement.dispatch(command).pipe(Effect.mapError(uncertain));
        const event = original.storedEvents
          .map((item) => item.event)
          .find((item) => item.type === "checkpoint.rollback-requested");
        if (event?.type !== "checkpoint.rollback-requested") return yield* uncertain();
        if (!matches({ threadId: event.threadId, ...event.payload })) return yield* conflict();
      }
      const status =
        effect?.status === "succeeded"
          ? "APPLIED"
          : effect?.failureCode === "checkpoint_restore_partial" ||
              (effect?.status === "cancelled" && effect.attemptCount > 0)
            ? "PARTIAL"
            : effect?.status === "failed" || effect?.status === "cancelled"
              ? "FAILED"
              : "REQUESTED";
      return {
        commandId,
        threadId,
        scopeId: input.scopeId,
        checkpointId: input.checkpointId,
        status,
        receipt: {
          status: "accepted",
          acceptedAt: DateTime.formatIso(receipt.acceptedAt),
          sequence: receipt.resultSequence,
        },
        effectStatus: effect?.status ?? "unavailable",
        detail:
          status === "PARTIAL"
            ? "Restore may have changed the provider or workspace. Inspect both before taking further action; this request will not be replayed."
            : status === "FAILED"
              ? "Restore did not complete. Inspect the target before making a new request."
              : effect === undefined
                ? "The request was accepted, but its execution outcome is unavailable."
                : null,
      } satisfies CheckpointMcpRestoreResult;
    });
    const existing = yield* readReceipt;
    if (Option.isSome(existing)) return yield* observe(existing.value);

    const target = projection.checkpoints.find(
      (item) =>
        item.id === input.checkpointId &&
        item.threadId === threadId &&
        item.scopeId === input.scopeId,
    );
    const targetScope = projection.checkpointScopes.find(
      (item) => item.id === input.scopeId && item.threadId === threadId,
    );
    const availability =
      target === undefined ? undefined : yield* readAvailability(target, targetScope);
    const supported =
      target !== undefined &&
      targetScope !== undefined &&
      availability !== undefined &&
      restoreBlockers({
        projection,
        checkpoint: target,
        scope: targetScope,
        refAvailable: availability.refAvailable,
      }).length === 0 &&
      (yield* isCheckpointRestoreIsolated(projection.thread, targetScope, {
        fileSystem,
        path,
        projects,
        projections,
      }).pipe(
        Effect.mapError(() =>
          failure("operation_failed", "Unable to validate checkpoint workspace isolation."),
        ),
      ));
    let accepted: CommandReceiptStore.CommandReceiptV2 | undefined;
    if (!supported || targetScope === undefined) {
      // Persist business rejection too: fixing the target does not silently reuse an old key.
      yield* receipts
        .insertIfAbsent({
          commandId,
          threadId,
          commandType: command.type,
          status: "rejected",
          acceptedAt: yield* DateTime.now,
          resultSequence: 0,
          error: encodeRejectedRestore({
            type: "checkpoint_restore_rejected",
            threadId,
            scopeId: input.scopeId,
            checkpointId: input.checkpointId,
          }),
        })
        .pipe(Effect.mapError(uncertain));
    } else {
      const expectedThreadState = checkpointRestoreThreadState(projection);
      const expectedWorkspaceFingerprint = yield* checkpointStore
        .readWorkspaceFingerprint(targetScope.cwd)
        .pipe(
          Effect.mapError(() =>
            failure(
              "operation_failed",
              "Unable to read workspace state. Retry with the same clientRequestId.",
            ),
          ),
        );
      // Admission and execution both compare this acceptance-time state under the thread lock.
      // Dispatch can commit before its response fails; the durable receipt decides the outcome.
      const dispatched = yield* threadManagement
        .dispatch({ ...command, expectedThreadState, expectedWorkspaceFingerprint })
        .pipe(Effect.option);
      if (Option.isSome(dispatched)) {
        const event = dispatched.value.storedEvents
          .map((item) => item.event)
          .find((item) => item.type === "checkpoint.rollback-requested");
        if (event?.type === "checkpoint.rollback-requested") {
          if (!matches({ threadId: event.threadId, ...event.payload })) return yield* conflict();
          accepted = {
            commandId,
            threadId,
            commandType: command.type,
            status: "accepted",
            acceptedAt: event.occurredAt,
            resultSequence: dispatched.value.sequence,
            error: null,
          };
        }
      }
    }
    const receipt = yield* readReceipt.pipe(Effect.orElseSucceed(() => Option.none()));
    const resolved = Option.getOrUndefined(receipt) ?? accepted;
    if (resolved === undefined) return yield* uncertain();
    return yield* observe(resolved);
  });

  return CheckpointMcpService.of({ list, diff, restore });
});

export const layer = Layer.effect(CheckpointMcpService, make);
