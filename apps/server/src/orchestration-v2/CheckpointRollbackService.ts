import {
  CheckpointId,
  checkpointRollbackAppRunOrdinal,
  CheckpointScopeId,
  latestProviderTurnForAttempt,
  type OrchestrationV2DomainEvent,
  ProviderThreadId,
  ThreadId,
} from "@t3tools/contracts";
import * as CheckpointStore from "../checkpointing/CheckpointStore.ts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as GitWorkflowService from "../git/GitWorkflowService.ts";
import type * as ServerConfig from "../config.ts";
import type * as ServerSettings from "../serverSettings.ts";
import { makeIsManagedWorktreePath } from "../worktreesDirectory.ts";
import { withWorkspaceLease } from "../workspace/workspaceLease.ts";
import {
  isCheckpointRestoreIsolated,
  checkpointRestoreThreadState,
  checkpointRestoreThreadIdle,
  SHARED_WORKSPACE_RESTORE_MESSAGE,
} from "./CheckpointRestoreSafety.ts";
import { CheckpointServiceV2 } from "./CheckpointService.ts";
import { EventSinkV2 } from "./EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import { ProjectionStoreV2 } from "./ProjectionStore.ts";
import * as ProjectStore from "./ProjectStore.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { ProviderSessionManagerV2 } from "./ProviderSessionManager.ts";
import { RuntimePolicyV2 } from "./RuntimePolicy.ts";
import * as ThreadCommandExecutor from "./ThreadCommandExecutor.ts";

export const ROLLBACK_FAILED_MESSAGE =
  "The provider could not roll back this conversation. Try again; if it keeps failing, check the provider and server logs.";

export class CheckpointRollbackExecutionError extends Schema.TaggedError<CheckpointRollbackExecutionError>()(
  "CheckpointRollbackExecutionError",
  {
    reason: Schema.Literals([
      "rollback-target-invalid",
      "active-provider-changed",
      "provider-turn-unavailable",
      "unexpected-failure",
      "shared-workspace",
      "thread-not-idle",
      "provider-rollback-unsupported",
      "restore-precondition-changed",
    ]),
    threadId: ThreadId,
    providerThreadId: ProviderThreadId,
    checkpointId: CheckpointId,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    switch (this.reason) {
      case "rollback-target-invalid":
        return `Rollback target ${this.checkpointId} for provider thread ${this.providerThreadId} on thread ${this.threadId} is incomplete or invalid.`;
      case "active-provider-changed":
        return `Active provider changed before rollback target ${this.checkpointId} could execute on thread ${this.threadId}.`;
      case "provider-turn-unavailable":
        return `Provider turn for rollback target ${this.checkpointId} is unavailable on provider thread ${this.providerThreadId}.`;
      case "provider-rollback-unsupported":
        return "The current provider does not support restoring this conversation.";
      case "thread-not-idle":
        return "Checkpoint restore requires an idle, unarchived thread with no queued work.";
      case "restore-precondition-changed":
        return "Checkpoint restore was rejected because the workspace or thread changed.";
      case "shared-workspace":
        return SHARED_WORKSPACE_RESTORE_MESSAGE;
      case "unexpected-failure":
        return ROLLBACK_FAILED_MESSAGE;
    }
  }
}

export const ROLLBACK_PARTIAL_MESSAGE =
  "Checkpoint restore has an uncertain or partial outcome. Inspect the files and conversation before taking further action.";

export class CheckpointRollbackPartialError extends Schema.TaggedError<CheckpointRollbackPartialError>()(
  "CheckpointRollbackPartialError",
  {
    threadId: ThreadId,
    checkpointId: CheckpointId,
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return ROLLBACK_PARTIAL_MESSAGE;
  }
}
const isCheckpointRollbackError = Schema.is(
  Schema.Union([CheckpointRollbackExecutionError, CheckpointRollbackPartialError]),
);

export interface CheckpointRollbackServiceV2Shape {
  readonly execute: (input: {
    readonly threadId: ThreadId;
    readonly providerThreadId: ProviderThreadId;
    readonly checkpointId: CheckpointId;
    readonly scopeId: CheckpointScopeId;
    readonly restoreFiles?: boolean;
    readonly expectedIdle?: true;
    readonly expectedWorkspaceFingerprint?: string;
    readonly expectedThreadState?: string;
  }) => Effect.Effect<void, CheckpointRollbackExecutionError | CheckpointRollbackPartialError>;
}

export class CheckpointRollbackServiceV2 extends Context.Service<
  CheckpointRollbackServiceV2,
  CheckpointRollbackServiceV2Shape
>()("t3/orchestration-v2/CheckpointRollbackService/CheckpointRollbackServiceV2") {}

export const layer: Layer.Layer<
  CheckpointRollbackServiceV2,
  never,
  | CheckpointServiceV2
  | CheckpointStore.CheckpointStore
  | GitWorkflowService.GitWorkflowService
  | EventSinkV2
  | IdAllocator.IdAllocatorV2
  | ProjectionStoreV2
  | ProviderSessionManagerV2
  | RuntimePolicyV2
  | FileSystem.FileSystem
  | Path.Path
  | ProjectStore.ProjectStoreV2
  | ServerConfig.ServerConfig
  | ServerSettings.ServerSettingsService
  | ThreadCommandExecutor.ThreadCommandExecutor
> = Layer.effect(
  CheckpointRollbackServiceV2,
  Effect.gen(function* () {
    const checkpoints = yield* CheckpointServiceV2;
    const checkpointStore = yield* CheckpointStore.CheckpointStore;
    const gitWorkflow = yield* GitWorkflowService.GitWorkflowService;
    const eventSink = yield* EventSinkV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    const projections = yield* ProjectionStoreV2;
    const sessions = yield* ProviderSessionManagerV2;
    const runtimePolicy = yield* RuntimePolicyV2;
    const fileSystem = yield* FileSystem.FileSystem;
    const projects = yield* ProjectStore.ProjectStoreV2;
    const path = yield* Path.Path;
    const threadCommands = yield* ThreadCommandExecutor.ThreadCommandExecutor;
    const isManagedWorktreePath = yield* makeIsManagedWorktreePath;

    const execute = Effect.fn("orchestrationV2.checkpointRollback.execute")(function* (input: {
      readonly threadId: ThreadId;
      readonly providerThreadId: ProviderThreadId;
      readonly checkpointId: CheckpointId;
      readonly scopeId: CheckpointScopeId;
      readonly restoreFiles?: boolean;
      readonly expectedIdle?: true;
      readonly expectedWorkspaceFingerprint?: string;
      readonly expectedThreadState?: string;
    }) {
      const guarded =
        input.expectedIdle === true ||
        input.expectedWorkspaceFingerprint !== undefined ||
        input.expectedThreadState !== undefined;
      const reject = (reason: CheckpointRollbackExecutionError["reason"]) =>
        new CheckpointRollbackExecutionError({
          reason,
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      const projection = yield* projections.getThreadRecords(input.threadId, [
        "providerThreads",
        "providerSessions",
        "checkpoints",
        "checkpointScopes",
        "runs",
        "attempts",
        "nodes",
        "providerTurns",
      ]);
      const providerThread = projection.providerThreads.find(
        (candidate) => candidate.id === input.providerThreadId,
      );
      const checkpoint = projection.checkpoints.find(
        (candidate) => candidate.id === input.checkpointId,
      );
      const scope = projection.checkpointScopes.find((candidate) => candidate.id === input.scopeId);
      if (
        providerThread === undefined ||
        providerThread.providerSessionId === null ||
        checkpoint === undefined ||
        scope === undefined ||
        checkpoint.scopeId !== scope.id ||
        checkpoint.status !== "ready"
      ) {
        return yield* new CheckpointRollbackExecutionError({
          reason: "rollback-target-invalid",
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      }
      if (
        providerThread.id !== projection.thread.activeProviderThreadId ||
        providerThread.providerInstanceId !== projection.thread.modelSelection.instanceId
      ) {
        return yield* new CheckpointRollbackExecutionError({
          reason: "active-provider-changed",
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      }

      if (guarded && !checkpointRestoreThreadIdle(projection))
        return yield* reject("thread-not-idle");
      if (
        input.expectedThreadState !== undefined &&
        input.expectedThreadState !== checkpointRestoreThreadState(projection)
      )
        return yield* reject("restore-precondition-changed");
      if (
        guarded &&
        scope.providerThreadId !== null &&
        scope.providerThreadId !== undefined &&
        scope.providerThreadId !== providerThread.id
      )
        return yield* reject("active-provider-changed");
      const targetOrdinal = guarded
        ? checkpointRollbackAppRunOrdinal(checkpoint, scope)
        : (checkpoint.appRunOrdinal ?? 0);
      if (targetOrdinal === null) return yield* reject("rollback-target-invalid");
      const rollbackTarget: ProviderAdapter.ProviderAdapterV2RollbackTarget =
        targetOrdinal === 0
          ? {
              type: "thread_start",
              checkpointId: checkpoint.id,
              appRunOrdinal: 0,
            }
          : yield* Effect.gen(function* () {
              const targetRun = projection.runs.find((run) => run.ordinal === targetOrdinal);
              const targetAttempt = projection.attempts.find(
                (attempt) => attempt.id === targetRun?.activeAttemptId,
              );
              const targetTurn =
                latestProviderTurnForAttempt(projection.providerTurns, targetAttempt?.id) ??
                projection.providerTurns.find((turn) => turn.id === targetAttempt?.providerTurnId);
              if (targetTurn === undefined || targetTurn.providerThreadId !== providerThread.id) {
                return yield* new CheckpointRollbackExecutionError({
                  reason: "provider-turn-unavailable",
                  threadId: input.threadId,
                  providerThreadId: input.providerThreadId,
                  checkpointId: input.checkpointId,
                });
              }
              return {
                type: "provider_turn" as const,
                checkpointId: checkpoint.id,
                appRunOrdinal: targetOrdinal,
                providerTurn: targetTurn,
              };
            });

      const { worktreePath, branch } = projection.thread;
      // Recreated only where the server would have made it, as turn start does.
      if (
        worktreePath != null &&
        branch != null &&
        !(yield* fileSystem.exists(worktreePath)) &&
        (yield* isManagedWorktreePath(worktreePath))
      ) {
        const project = yield* projects.get(projection.thread.projectId);
        if (Option.isSome(project)) {
          yield* gitWorkflow.pruneWorktrees({ cwd: project.value.workspaceRoot });
          yield* gitWorkflow.createWorktree({
            cwd: project.value.workspaceRoot,
            refName: branch,
            path: worktreePath,
          });
        }
      }

      if (
        input.restoreFiles !== false &&
        !(yield* isCheckpointRestoreIsolated(projection.thread, scope, {
          fileSystem,
          projections,
          projects,
          path,
        }))
      ) {
        return yield* new CheckpointRollbackExecutionError({
          reason: "shared-workspace",
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
          checkpointId: input.checkpointId,
        });
      }

      const providerSessionId = providerThread.providerSessionId;
      let mutationStarted = false;
      const run = (
        operations: Pick<CheckpointServiceV2["Service"], "restore" | "deleteStaleRefs">,
      ) =>
        Effect.gen(function* () {
          if (guarded) {
            if (
              !(yield* checkpointStore.hasCheckpointRef({
                cwd: scope.cwd,
                checkpointRef: checkpoint.ref,
              }))
            )
              return yield* reject("rollback-target-invalid");
            if (
              input.expectedWorkspaceFingerprint !== undefined &&
              (yield* checkpointStore.readWorkspaceFingerprint(scope.cwd)) !==
                input.expectedWorkspaceFingerprint
            )
              return yield* reject("restore-precondition-changed");
          }
          const modelSelection = projection.thread.modelSelection;
          const resolvedRuntimePolicy = yield* runtimePolicy.resolve({
            thread: projection.thread,
            modelSelection,
          });
          const existingSession = projection.providerSessions.find(
            (candidate) => candidate.id === providerSessionId,
          );
          const session = yield* sessions.open({
            threadId: input.threadId,
            providerSessionId,
            modelSelection,
            runtimePolicy: resolvedRuntimePolicy,
            ...(existingSession === undefined ? {} : { resumeFromSession: existingSession }),
            ...(providerThread.nativeThreadRef?.nativeId == null
              ? {}
              : { initialNativeThreadId: providerThread.nativeThreadRef.nativeId }),
            ...(providerThread.nativeMetadata?.itemIdentityVersion === undefined
              ? {}
              : {
                  initialProviderItemIdentityVersion:
                    providerThread.nativeMetadata.itemIdentityVersion,
                }),
          });

          if (
            guarded &&
            (!session.providerSession.capabilities.threads.canRollbackThread ||
              !session.providerSession.capabilities.checkpointing.providerCanRollbackConversation ||
              !session.providerSession.capabilities.checkpointing.providerRollbackReturnsSnapshot)
          )
            return yield* reject("provider-rollback-unsupported");

          const runsToRollback = projection.runs.filter(
            (run) =>
              run.ordinal > targetOrdinal &&
              (run.status === "completed" ||
                run.status === "interrupted" ||
                run.status === "failed" ||
                run.status === "cancelled"),
          );
          // Rolled-back turns stay in the audit history, but no longer exist in
          // the provider conversation and must not be counted by a later rewind.
          const rolledBackRunIds = new Set(
            projection.runs.filter((run) => run.status === "rolled_back").map((run) => run.id),
          );
          const rolledBackAttemptIds = new Set(
            projection.attempts
              .filter((attempt) => rolledBackRunIds.has(attempt.runId))
              .map((attempt) => attempt.id),
          );
          const providerThreadTurns = projection.providerTurns.filter(
            (turn) =>
              turn.providerThreadId === providerThread.id &&
              (turn.runAttemptId === null || !rolledBackAttemptIds.has(turn.runAttemptId)),
          );

          mutationStarted = runsToRollback.length > 0 || input.restoreFiles !== false;
          const snapshot =
            runsToRollback.length === 0
              ? { providerThread }
              : yield* session.rollbackThread({
                  providerThread,
                  target: rollbackTarget,
                  providerThreadTurns,
                });
          if (input.restoreFiles !== false) yield* operations.restore({ scope, checkpoint });
          const staleCheckpoints = projection.checkpoints.filter(
            (candidate) =>
              candidate.scopeId === scope.id &&
              candidate.appRunOrdinal !== null &&
              candidate.appRunOrdinal > targetOrdinal &&
              candidate.status === "ready",
          );
          if (staleCheckpoints.length > 0) {
            yield* operations.deleteStaleRefs({ scope, checkpoints: staleCheckpoints });
          }

          // Thread commands write full provider thread rows under this lock. Without
          // it, a message sent during the rollback can plan against the old native
          // session and commit after this, undoing the provider's fork.
          yield* (
            guarded
              ? (effect: Effect.Effect<void, unknown>) => effect
              : (effect: Effect.Effect<void, unknown>) =>
                  threadCommands.withLock(input.threadId, effect)
          )(
            Effect.gen(function* () {
              const now = yield* DateTime.now;
              const makeEvent = <Event extends OrchestrationV2DomainEvent>(
                event: Omit<Event, "id">,
              ) =>
                Effect.map(
                  ids.allocate.event({ threadId: event.threadId }),
                  (id) =>
                    ({
                      ...event,
                      id,
                    }) as Event,
                );
              const events: Array<OrchestrationV2DomainEvent> = [];
              events.push(
                yield* makeEvent({
                  type: "provider-thread.updated",
                  threadId: input.threadId,
                  driver: providerThread.driver,
                  providerInstanceId: providerThread.providerInstanceId,
                  occurredAt: now,
                  payload: {
                    ...snapshot.providerThread,
                    lastRunOrdinal: targetOrdinal === 0 ? null : targetOrdinal,
                    updatedAt: now,
                  },
                }),
              );
              for (const staleCheckpoint of staleCheckpoints) {
                events.push(
                  yield* makeEvent({
                    type: "checkpoint.captured",
                    threadId: input.threadId,
                    ...(staleCheckpoint.runId === null ? {} : { runId: staleCheckpoint.runId }),
                    nodeId: staleCheckpoint.nodeId,
                    providerInstanceId: providerThread.providerInstanceId,
                    occurredAt: now,
                    payload: { ...staleCheckpoint, status: "stale" },
                  }),
                );
              }
              for (const run of runsToRollback) {
                const rootNode = projection.nodes.find(
                  (candidate) => candidate.id === run.rootNodeId,
                );
                events.push(
                  yield* makeEvent({
                    type: "run.updated",
                    threadId: input.threadId,
                    runId: run.id,
                    ...(rootNode === undefined ? {} : { nodeId: rootNode.id }),
                    providerInstanceId: run.providerInstanceId,
                    occurredAt: now,
                    payload: { ...run, status: "rolled_back", completedAt: now },
                  }),
                );
                if (rootNode !== undefined) {
                  events.push(
                    yield* makeEvent({
                      type: "node.updated",
                      threadId: input.threadId,
                      runId: run.id,
                      nodeId: rootNode.id,
                      providerInstanceId: run.providerInstanceId,
                      occurredAt: now,
                      payload: { ...rootNode, status: "rolled_back", completedAt: now },
                    }),
                  );
                }
              }
              yield* eventSink.write({ events });
            }),
          );
        }).pipe(
          Effect.catchCause((cause) => {
            if (mutationStarted)
              return Effect.fail(
                new CheckpointRollbackPartialError({
                  threadId: input.threadId,
                  checkpointId: input.checkpointId,
                  cause: cause.reasons.find((reason) => reason._tag === "Fail")?.error ?? cause,
                }),
              );
            return Effect.failCause(cause);
          }),
        );
      yield* guarded ? checkpoints.withRestoreLock(scope.cwd, run) : run(checkpoints);
    });

    const guardedExecute = (
      input: Parameters<CheckpointRollbackServiceV2["Service"]["execute"]>[0],
    ) =>
      input.expectedIdle === true ||
      input.expectedWorkspaceFingerprint !== undefined ||
      input.expectedThreadState !== undefined
        ? threadCommands.withLock(input.threadId, execute(input))
        : execute(input);

    return CheckpointRollbackServiceV2.of({
      execute: (input) =>
        Effect.gen(function* () {
          // Storage cleanup removes worktrees under the same lease.
          const { thread } = yield* projections.getThreadRecords(input.threadId, []);
          yield* thread.worktreePath === null
            ? guardedExecute(input)
            : withWorkspaceLease(path.resolve(thread.worktreePath), guardedExecute(input));
        }).pipe(
          Effect.mapError((cause) =>
            isCheckpointRollbackError(cause)
              ? cause
              : new CheckpointRollbackExecutionError({
                  reason: "unexpected-failure",
                  threadId: input.threadId,
                  providerThreadId: input.providerThreadId,
                  checkpointId: input.checkpointId,
                  cause,
                }),
          ),
        ),
    });
  }),
);
