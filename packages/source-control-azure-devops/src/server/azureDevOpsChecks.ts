import type { PullRequestCheck, PullRequestCheckStatus } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { decodeJsonResult } from "@t3tools/shared/schemaJson";

const optionalString = Schema.optional(Schema.NullOr(Schema.String));
const optionalBoolean = Schema.optional(Schema.NullOr(Schema.Boolean));
const PolicySchema = Schema.Struct({
  status: Schema.String,
  configuration: Schema.Struct({
    id: Schema.Int,
    isEnabled: Schema.Boolean,
    isBlocking: Schema.Boolean,
    type: Schema.Struct({ id: Schema.String }),
    settings: Schema.Struct({
      displayName: optionalString,
      statusName: optionalString,
      statusGenre: optionalString,
    }),
  }),
  context: Schema.optional(
    Schema.NullOr(
      Schema.Struct({
        buildId: Schema.optional(Schema.NullOr(Schema.Int)),
        buildDefinitionName: optionalString,
        lastMergeSourceCommitId: optionalString,
        buildIsNotCurrent: optionalBoolean,
        isExpired: optionalBoolean,
      }),
    ),
  ),
});
const StatusSchema = Schema.Struct({
  id: Schema.Int,
  iterationId: Schema.optional(Schema.NullOr(Schema.Int)),
  context: Schema.Struct({ name: Schema.String, genre: optionalString }),
  state: optionalString,
  description: optionalString,
  targetUrl: optionalString,
});

export const decodePolicyEvaluationsJson = decodeJsonResult(
  Schema.Struct({ value: Schema.Array(PolicySchema) }),
);
export const decodeStatusesJson = decodeJsonResult(
  Schema.Struct({
    value: Schema.Array(StatusSchema),
    continuation_token: optionalString,
  }),
);

export type AzureDevOpsPolicyEvaluation = typeof PolicySchema.Type;
export type AzureDevOpsStatus = typeof StatusSchema.Type;

function policyStatus(status: string): PullRequestCheckStatus {
  switch (status.toLowerCase()) {
    case "approved":
      return "success";
    case "rejected":
      return "failure";
    case "broken":
      return "action-required";
    case "notapplicable":
      return "skipped";
    default:
      return "pending";
  }
}

function statusState(status: string | null | undefined): PullRequestCheckStatus {
  switch (status?.toLowerCase()) {
    case "succeeded":
      return "success";
    case "failed":
    case "error":
      return "failure";
    case "notapplicable":
      return "skipped";
    default:
      return "pending";
  }
}

const statusKey = (context: {
  readonly name: string;
  readonly genre?: string | null | undefined;
}) => JSON.stringify([context.genre ?? "", context.name]);

export function azureDevOpsChecks(input: {
  readonly policies: ReadonlyArray<AzureDevOpsPolicyEvaluation>;
  readonly statuses: ReadonlyArray<AzureDevOpsStatus>;
  readonly headSha: string;
  readonly iterationId: number;
  readonly url: string;
}): ReadonlyArray<PullRequestCheck> {
  const checks: PullRequestCheck[] = [];
  const covered = new Set<string>();
  const latest = new Map<string, AzureDevOpsStatus>();
  for (const status of input.statuses) {
    if (status.iterationId != null && status.iterationId !== input.iterationId) continue;
    const key = statusKey(status.context);
    const previous = latest.get(key);
    if (previous?.iterationId != null && status.iterationId == null) continue;
    if (
      previous === undefined ||
      (previous.iterationId == null && status.iterationId != null) ||
      previous.id < status.id
    )
      latest.set(key, status);
  }
  for (const policy of input.policies) {
    const { configuration, context } = policy;
    if (!configuration.isEnabled) continue;
    const build = configuration.type.id.toLowerCase() === "0609b952-1397-4640-95ec-e00a01b2c241";
    const statusName = configuration.settings.statusName;
    if (!build && !statusName) continue;
    const currentStatus = statusName
      ? latest.get(statusKey({ name: statusName, genre: configuration.settings.statusGenre }))
      : undefined;
    if (statusName)
      covered.add(statusKey({ name: statusName, genre: configuration.settings.statusGenre }));
    const stale =
      build &&
      policy.status.toLowerCase() !== "notapplicable" &&
      (context?.lastMergeSourceCommitId !== input.headSha || context?.buildIsNotCurrent === true);
    const expired =
      build &&
      context?.isExpired === true &&
      !["queued", "running", "notapplicable"].includes(policy.status.toLowerCase());
    const label =
      configuration.settings.displayName || context?.buildDefinitionName || statusName || "Build";
    checks.push({
      name: `${label} (policy ${configuration.id})`,
      status: stale ? "pending" : expired ? "failure" : policyStatus(policy.status),
      required: configuration.isBlocking,
      description: stale
        ? "Waiting for a check of the current PR revision."
        : expired
          ? "Build validation expired."
          : (currentStatus?.description ?? null),
      url:
        build && (context?.buildId ?? 0) > 0
          ? `${input.url.split("/_git/")[0]}/_build/results?buildId=${context?.buildId}`
          : (currentStatus?.targetUrl ?? input.url),
    });
  }
  for (const [key, status] of [...latest].sort(([left], [right]) => left.localeCompare(right))) {
    if (covered.has(key)) continue;
    checks.push({
      name: `Status ${statusKey(status.context)}`,
      status: statusState(status.state),
      required: false,
      description: status.description ?? null,
      url: status.targetUrl ?? null,
    });
  }
  return checks;
}
