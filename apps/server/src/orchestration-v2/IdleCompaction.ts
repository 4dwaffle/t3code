import type { OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import { COMPACTION_SUGGESTION_MIN_TOKENS } from "@t3tools/shared/claudeCompaction";
import { derivePendingBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as DateTime from "effect/DateTime";

const MINUTE_MS = 60 * 1_000;

// Drivers that can compact while idle, and the cache TTL to assume when a turn
// does not report one. Claude reports its real TTL on each turn's token usage
// (`promptCacheTtlMs`: 1h on subscriptions, 5m on API keys), so it has no
// guess: a Claude session that never reported a TTL is skipped. Modern OpenAI
// models keep about 30m; providers without a documented TTL use the same 30m.
// Only drivers whose adapters expose compactThread are listed. ACP Registry
// flavors do not advertise supportsCompaction and must not receive /compact.
// Cursor is left out: its SDK passes /compress to the model as plain text, so
// the turn summarizes in chat and the next turn resends the full context.
export const UNREPORTED_PROMPT_CACHE_TTL_MS: Readonly<Record<string, number | null>> = {
  claudeAgent: null,
  codex: 30 * MINUTE_MS,
  opencode: 30 * MINUTE_MS,
  muse: 30 * MINUTE_MS,
  pi: 30 * MINUTE_MS,
  grok: 30 * MINUTE_MS,
  antigravity: 30 * MINUTE_MS,
};
// Claude's 1h cache is the longest TTL any provider reports or is assumed to keep.
export const MAX_PROMPT_CACHE_TTL_MS = 60 * MINUTE_MS;
// The sweep ticks every minute, so a shorter window (10% of a 5m cache) could fall between ticks.
const MIN_IDLE_COMPACTION_WINDOW_MS = 2 * MINUTE_MS;

// Claude Code's idleCompaction defaults to 200K and won't go below 100K. Below
// that a cold re-read is cheap, so compacting mostly loses context for little
// saving. Shared with the stale-thread Compact chip so both agree.
export const MIN_IDLE_COMPACTION_CONTEXT_TOKENS = COMPACTION_SUGGESTION_MIN_TOKENS;

/** The provider-reported cache TTL, else the driver's assumed one. Null means unknown: skip. */
export function resolvePromptCacheTtlMs(
  driver: string,
  reportedTtlMs: number | null | undefined,
): number | null {
  if (!Object.hasOwn(UNREPORTED_PROMPT_CACHE_TTL_MS, driver)) return null;
  return reportedTtlMs ?? UNREPORTED_PROMPT_CACHE_TTL_MS[driver] ?? null;
}

/**
 * The window, in epoch ms, when an idle thread should compact: the end of its
 * cache TTL, at least the last 10%. Past `endMs` the cache is cold, so
 * compacting would pay a full uncached read for a thread that may never be
 * reopened.
 */
export function idleCompactionWindow(
  completedAt: DateTime.Utc,
  promptCacheTtlMs: number,
  delayOverrideMs?: number,
): { readonly startMs: number; readonly endMs: number } {
  const completedMs = DateTime.toEpochMillis(completedAt);
  const margin = Math.max(Math.round(promptCacheTtlMs / 10), MIN_IDLE_COMPACTION_WINDOW_MS);
  const delay = delayOverrideMs ?? promptCacheTtlMs - margin;
  return { startMs: completedMs + delay, endMs: completedMs + delay + margin };
}

/** Rechecked inside serialized message dispatch, so a stale sweep cannot wake work. */
export function idleCompactionCandidate(
  projection: OrchestrationV2ThreadProjection,
  enabled: boolean,
  nowMs: number,
) {
  const { thread, runs, messages, providerThreads, providerTurns, runtimeRequests, turnItems } =
    projection;
  if (!enabled || thread.deletedAt !== null || thread.archivedAt !== null) return null;
  if (thread.lineage.relationshipToParent === "subagent") return null;
  // A pending merge-back belongs to the user's next message; compaction would
  // consume it and bind the merged context to the current provider thread.
  if (
    projection.contextTransfers.some(
      (transfer) =>
        transfer.type === "merge_back" &&
        transfer.targetThreadId === thread.id &&
        transfer.status === "pending",
    )
  )
    return null;
  if (thread.settledOverride === "settled" || thread.settledAt !== null) return null;
  if (runtimeRequests.some((request) => request.status === "pending")) return null;
  if (
    runs.some((run) =>
      ["preparing", "queued", "starting", "running", "waiting"].includes(run.status),
    )
  ) {
    return null;
  }
  const latestRun = runs.reduce<(typeof runs)[number] | null>(
    (latest, run) => (latest === null || run.ordinal > latest.ordinal ? run : latest),
    null,
  );
  if (latestRun?.status !== "completed" || latestRun.completedAt === null) return null;
  if (latestRun.idleCompactionOfRunId !== undefined) return null;
  const message = messages.find((candidate) => candidate.id === latestRun.userMessageId);
  if (message === undefined || message.text.trim().toLowerCase() === "/compact") return null;
  // A snooze that already woke on normal completion may compact, but a parked
  // snooze must retain its wake time and never be resumed by maintenance.
  if (
    thread.snoozedUntil != null &&
    DateTime.toEpochMillis(thread.snoozedUntil) > nowMs &&
    (thread.snoozedAt == null || DateTime.Order(latestRun.completedAt, thread.snoozedAt) <= 0)
  )
    return null;
  // Any pending background work (even a dev server that does not hold
  // completion) can report into the next turn; a hidden compaction would
  // swallow that report, notably when Claude resumes a released process.
  if (
    derivePendingBackgroundWork({
      latestRun,
      runs,
      providerThreads,
      turnItems,
      activeProviderThreadId: thread.activeProviderThreadId,
      pullRequests: thread.pullRequests,
    }).length > 0 ||
    // Persistent monitors are left out of pending background work but still report.
    turnItems.some(
      (item) =>
        (item.type === "command_execution" ||
          item.type === "dynamic_tool" ||
          item.type === "subagent") &&
        (item.status === "pending" || item.status === "running" || item.status === "waiting"),
    )
  )
    return null;
  // Include unadopted messages, even if a queued run has not been persisted yet.
  if (
    messages.some(
      (candidate) =>
        candidate.role === "user" &&
        DateTime.Order(candidate.updatedAt, latestRun.completedAt!) > 0,
    )
  )
    return null;
  const providerThread = providerThreads.find(
    (candidate) => candidate.id === thread.activeProviderThreadId,
  );
  if (
    providerThread === undefined ||
    providerThread.id !== latestRun.providerThreadId ||
    providerThread.providerInstanceId !== thread.providerInstanceId ||
    latestRun.providerInstanceId !== thread.providerInstanceId
  )
    return null;
  const usages = providerTurns
    .filter((turn) => turn.providerThreadId === providerThread.id && turn.tokenUsage !== undefined)
    .map((turn) => turn.tokenUsage!)
    .toSorted((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt));
  // A turn served fully from cache creates no cache entry and reports no TTL;
  // the last one reported still holds for this provider thread.
  const promptCacheTtlMs = resolvePromptCacheTtlMs(
    providerThread.driver,
    usages.find((usage) => usage.promptCacheTtlMs !== undefined)?.promptCacheTtlMs,
  );
  if (promptCacheTtlMs === null) return null;
  const usedTokens = usages[0]?.usedTokens ?? providerThread.contextUsage?.usedTokens;
  if (
    usedTokens === undefined ||
    !Number.isFinite(usedTokens) ||
    usedTokens < MIN_IDLE_COMPACTION_CONTEXT_TOKENS
  )
    return null;
  return {
    threadId: thread.id,
    runId: latestRun.id,
    completedAt: latestRun.completedAt,
    driver: providerThread.driver,
    promptCacheTtlMs,
  };
}
