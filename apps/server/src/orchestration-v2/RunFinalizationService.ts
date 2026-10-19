import { isTemporaryWorktreeBranch } from "@t3tools/shared/git";
import {
  CheckpointScopeId,
  CommandId,
  type OrchestrationV2ThreadShell,
  ProjectId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";

import * as ProjectService from "../project/ProjectService.ts";
import * as PullRequestService from "../pullRequest/PullRequestService.ts";
import * as VcsDriverRegistry from "../vcs/VcsDriverRegistry.ts";
import * as VcsStatusBroadcaster from "../vcs/VcsStatusBroadcaster.ts";
import * as WorkspaceEntries from "../workspace/WorkspaceEntries.ts";
import * as CheckpointCapture from "./CheckpointCaptureService.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ThreadManagementService from "./ThreadManagementService.ts";

function failureCategory(error: unknown): string {
  if (
    typeof error === "object" &&
    error !== null &&
    "_tag" in error &&
    typeof error._tag === "string"
  ) {
    return error._tag;
  }
  if (error instanceof Error) return error.name;
  return typeof error;
}

function causeCategory(cause: Cause.Cause<unknown>): string {
  for (const reason of cause.reasons) {
    if (Cause.isFailReason(reason)) return failureCategory(reason.error);
    if (Cause.isDieReason(reason)) return failureCategory(reason.defect);
  }
  return "Interrupted";
}

export class RunFinalizationError extends Schema.TaggedError<RunFinalizationError>()(
  "RunFinalizationError",
  {
    threadId: ThreadId,
    runId: RunId,
    scopeId: CheckpointScopeId,
    operation: Schema.Literals(["capture-checkpoint", "refresh-workspace"]),
    cause: Schema.Defect(),
  },
) {}

export class RunFinalizationRefreshError extends Schema.TaggedError<RunFinalizationRefreshError>()(
  "RunFinalizationRefreshError",
  { cwd: Schema.String, cause: Schema.Defect() },
) {}

// Run execution is built before the finalization observer, which depends on the orchestrator.
export class RunTurnObserver extends Context.Reference<{
  readonly refreshAfterTurn: (projectId: ProjectId) => Effect.Effect<void>;
}>("t3/orchestration-v2/RunTurnObserver", {
  defaultValue: () => ({ refreshAfterTurn: () => Effect.void }),
}) {}

export const layerTurnObserver = Layer.effect(
  RunTurnObserver,
  Effect.gen(function* () {
    const pullRequests = yield* PullRequestService.PullRequestService;
    return { refreshAfterTurn: pullRequests.refreshAfterTurn };
  }),
);

export class RunFinalizationFollowError extends Schema.TaggedError<RunFinalizationFollowError>()(
  "RunFinalizationFollowError",
  {
    threadId: ThreadId,
    projectId: Schema.optional(ProjectId),
    operation: Schema.Literals([
      "read-thread",
      "read-run",
      "read-provider-context",
      "read-provider-session",
      "read-session-sharing",
      "read-project",
      "detect-repository",
      "resolve-repository-metadata",
    ]),
    cause: Schema.Defect(),
  },
) {}

export class RunFinalizationObserver extends Context.Reference<{
  readonly refresh: (input: {
    readonly cwd: string;
    readonly threadId: ThreadId;
    readonly runId: RunId;
  }) => Effect.Effect<void, RunFinalizationRefreshError>;
  readonly followWorktreeMove: (input: {
    readonly threadId: ThreadId;
    readonly runId: RunId;
  }) => Effect.Effect<void, RunFinalizationFollowError>;
}>("t3/orchestration-v2/RunFinalizationObserver", {
  defaultValue: () => ({
    refresh: () => Effect.void,
    followWorktreeMove: () => Effect.void,
  }),
}) {}

export class RunFinalizationService extends Context.Service<
  RunFinalizationService,
  {
    readonly finalize: (input: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
      readonly scopeId: CheckpointScopeId;
    }) => Effect.Effect<void, RunFinalizationError>;
  }
>()("t3/orchestration-v2/RunFinalizationService") {}

const make = Effect.gen(function* () {
  const checkpointCapture = yield* CheckpointCapture.CheckpointCaptureServiceV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const observer = yield* RunFinalizationObserver;

  const finalize: RunFinalizationService["Service"]["finalize"] = Effect.fn(
    "RunFinalizationService.finalize",
  )(function* (input) {
    yield* checkpointCapture
      .execute(input)
      .pipe(
        Effect.mapError(
          (cause) => new RunFinalizationError({ ...input, operation: "capture-checkpoint", cause }),
        ),
      );
    yield* observer.followWorktreeMove({ threadId: input.threadId, runId: input.runId }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("worktree-location follow failed", {
          threadId: input.threadId,
          runId: input.runId,
          category: causeCategory(cause),
        }),
      ),
    );
    const projection = yield* projections
      .getCheckpointContext(input.threadId)
      .pipe(
        Effect.mapError(
          (cause) => new RunFinalizationError({ ...input, operation: "refresh-workspace", cause }),
        ),
      );
    const cwd = projection.checkpointScopes.find((scope) => scope.id === input.scopeId)?.cwd;
    if (cwd !== undefined) {
      // Retrying a display refresh after the checkpoint is durable blocks the next turn.
      yield* observer.refresh({ cwd, threadId: input.threadId, runId: input.runId }).pipe(
        Effect.catchTags({
          RunFinalizationRefreshError: (cause) =>
            Effect.logWarning("Failed to refresh workspace after checkpoint capture", {
              ...input,
              cause,
            }),
        }),
      );
    }
  });
  return RunFinalizationService.of({ finalize });
});

export const layer = Layer.effect(RunFinalizationService, make);

export const layerObserver = Layer.effect(
  RunFinalizationObserver,
  Effect.gen(function* () {
    const workspaceEntries = yield* WorkspaceEntries.WorkspaceEntries;
    const vcsStatus = yield* VcsStatusBroadcaster.VcsStatusBroadcaster;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const projects = yield* ProjectService.ProjectService;
    const vcsDrivers = yield* VcsDriverRegistry.VcsDriverRegistry;
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;

    const followBranchDrift = Effect.fn("RunFinalizationService.followBranchDrift")(function* (
      thread: OrchestrationV2ThreadShell,
      cwd: string,
      refName: string,
      runId: RunId,
    ) {
      // Ignore the old checkpoint checkout after a location move.
      if (
        thread.worktreePath === null ||
        thread.worktreePath !== cwd ||
        isTemporaryWorktreeBranch(refName)
      ) {
        return false;
      }
      return yield* threads
        .dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`command:effect:worktree-branch-drift:${runId}`),
          finalizedRunId: runId,
          threadId: thread.id,
          branch: refName,
          expectedActiveRunId: thread.activeRunId,
          expectedBranch: thread.branch,
          expectedWorktreePath: thread.worktreePath,
          requireExclusiveWorktree: true,
        })
        .pipe(
          Effect.as(true),
          Effect.catch((error) =>
            Effect.logWarning("failed to follow worktree branch drift after run completion", {
              threadId: thread.id,
              previousBranch: thread.branch,
              branch: refName,
              category: failureCategory(error),
            }).pipe(Effect.as(false)),
          ),
        );
    });

    const followWorktreeMove = Effect.fn("RunFinalizationService.followWorktreeMove")(function* ({
      threadId,
      runId,
    }: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
    }) {
      const thread = yield* projections
        .getThreadShell(threadId)
        .pipe(
          Effect.mapError(
            (cause) =>
              new RunFinalizationFollowError({ threadId, operation: "read-thread", cause }),
          ),
        );
      if (!thread) return;
      const { runs } = yield* projections
        .getThreadRecords(threadId, ["runs"], {
          runIds:
            thread.activeRunId === null || thread.activeRunId === runId
              ? [runId]
              : [runId, thread.activeRunId],
        })
        .pipe(
          Effect.mapError(
            (cause) => new RunFinalizationFollowError({ threadId, operation: "read-run", cause }),
          ),
        );
      const run = runs.find((candidate) => candidate.id === runId);
      if (run?.startedAt == null) return;
      if (thread.activeRunId !== null && thread.activeRunId !== runId) {
        const active = runs.find((candidate) => candidate.id === thread.activeRunId);
        if (active?.status !== "starting" || active.startedAt !== null) return;
      }
      const { providerThreadId, providerInstanceId } = run;
      if (providerThreadId === null) return;
      const providerContext = yield* projections
        .getThreadProviderContext(threadId, providerInstanceId)
        .pipe(
          Effect.mapError(
            (cause) =>
              new RunFinalizationFollowError({
                threadId,
                operation: "read-provider-context",
                cause,
              }),
          ),
        );
      // A queued successor may already have selected a different provider thread.
      const providerSessionId = providerContext.providerThreads.find(
        (providerThread) => providerThread.id === providerThreadId,
      )?.providerSessionId;
      if (providerSessionId == null) return;
      // Queue promotion can detach the binding before this finishing run follows its session.
      const session = yield* projections.getProviderSession(threadId, providerSessionId).pipe(
        Effect.mapError(
          (cause) =>
            new RunFinalizationFollowError({
              threadId,
              operation: "read-provider-session",
              cause,
            }),
        ),
      );
      // An instance-wide session's cwd is where it opened, not where this thread is.
      if (
        session === undefined ||
        session.capabilities.sessions.supportsMultipleProviderThreadsPerSession
      ) {
        return;
      }
      const liveCwd = session.cwd;
      if (liveCwd === undefined) return;
      // A shared session’s cwd does not identify which thread moved.
      const shared = yield* projections.isProviderSessionShared(threadId, providerSessionId).pipe(
        Effect.mapError(
          (cause) =>
            new RunFinalizationFollowError({
              threadId,
              operation: "read-session-sharing",
              cause,
            }),
        ),
      );
      if (shared) return;
      const project = yield* projects.getById(thread.projectId).pipe(
        Effect.mapError(
          (cause) =>
            new RunFinalizationFollowError({
              threadId,
              projectId: thread.projectId,
              operation: "read-project",
              cause,
            }),
        ),
      );
      if (Option.isNone(project)) return;
      const recordedCwd = thread.worktreePath ?? project.value.workspaceRoot;
      // A failed turn can retain a native move. Only follow while its requested
      // source is still recorded; a project repoint must invalidate that observation.
      if (session.cwdChangeSource !== recordedCwd || liveCwd === recordedCwd) return;

      const [observedRepository, projectRepository] = yield* Effect.all([
        vcsDrivers.detect({ cwd: liveCwd }),
        vcsDrivers.detect({ cwd: project.value.workspaceRoot }),
      ]).pipe(
        Effect.mapError(
          (cause) =>
            new RunFinalizationFollowError({ threadId, operation: "detect-repository", cause }),
        ),
      );
      if (
        observedRepository?.kind !== "git" ||
        projectRepository?.kind !== "git" ||
        !observedRepository.repository.metadataPath ||
        !projectRepository.repository.metadataPath
      ) {
        return;
      }
      // Git common dirs identify a repository across worktrees and are relative to detection cwd.
      const [observedCommonDir, projectCommonDir] = yield* Effect.all([
        fileSystem.realPath(path.resolve(liveCwd, observedRepository.repository.metadataPath)),
        fileSystem.realPath(
          path.resolve(project.value.workspaceRoot, projectRepository.repository.metadataPath),
        ),
      ]).pipe(
        Effect.mapError(
          (cause) =>
            new RunFinalizationFollowError({
              threadId,
              operation: "resolve-repository-metadata",
              cause,
            }),
        ),
      );
      if (observedCommonDir !== projectCommonDir) return;
      // Projects may select a repository subdirectory; sessions keep their factual cwd.
      const location =
        observedRepository.repository.rootPath === projectRepository.repository.rootPath
          ? project.value.workspaceRoot
          : observedRepository.repository.rootPath;
      if (location === recordedCwd) return;
      const newWorktreePath = location === project.value.workspaceRoot ? null : location;
      const local = yield* vcsStatus.refreshLocalStatus(location).pipe(
        Effect.catch((error) =>
          Effect.logWarning("failed to read git status after a worktree move", {
            threadId,
            cwd: location,
            category: failureCategory(error),
          }).pipe(Effect.as(null)),
        ),
      );
      const checkedOutBranch =
        newWorktreePath !== null &&
        local !== null &&
        local.refName !== null &&
        !isTemporaryWorktreeBranch(local.refName)
          ? local.refName
          : null;

      yield* threads
        .dispatch({
          type: "thread.metadata.update",
          commandId: CommandId.make(`command:effect:worktree-location-follow:${runId}`),
          finalizedRunId: runId,
          threadId,
          worktreePath: newWorktreePath,
          expectedWorktreePath: thread.worktreePath,
          expectedBranch: thread.branch,
          expectedActiveRunId: thread.activeRunId,
          branch: checkedOutBranch,
          // The provider already moved; an explicit client handoff still detaches.
          preserveProviderSession: true,
          ...(checkedOutBranch !== null ? { requireExclusiveWorktree: true } : {}),
        })
        .pipe(
          Effect.catch((error) =>
            Effect.logWarning("failed to follow the provider session into its new worktree", {
              threadId,
              worktreePath: newWorktreePath,
              category: failureCategory(error),
            }),
          ),
        );
    });

    return {
      followWorktreeMove,
      refresh: ({ cwd, threadId, runId }) =>
        Effect.gen(function* () {
          const [, local] = yield* Effect.all(
            [workspaceEntries.refresh(cwd), vcsStatus.refreshLocalStatus(cwd)],
            { concurrency: "unbounded" },
          );
          if (local.refName === null) return;
          const thread = yield* projections.getThreadShell(threadId);
          if (!thread) return;
          if (thread.branch !== local.refName) {
            if (!(yield* followBranchDrift(thread, cwd, local.refName, runId))) return;
          } else if (
            local.isDefaultRef ||
            (thread.activeRunId !== null && thread.activeRunId !== runId)
          ) {
            return;
          }
          yield* vcsStatus.refreshPullRequestStatus(cwd).pipe(
            Effect.catch((error) =>
              Effect.logWarning("failed to refresh pull request status after run completion", {
                threadId,
                cwd,
                category: failureCategory(error),
              }),
            ),
          );
        }).pipe(Effect.mapError((cause) => new RunFinalizationRefreshError({ cwd, cause }))),
    };
  }),
);
