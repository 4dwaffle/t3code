import { expect, it } from "@effect/vitest";
import { azureDevOpsChecks, type AzureDevOpsPolicyEvaluation } from "./azureDevOpsChecks.ts";

const policy: AzureDevOpsPolicyEvaluation = {
  status: "approved",
  configuration: {
    id: 14,
    isEnabled: true,
    isBlocking: true,
    type: { id: "0609b952-1397-4640-95ec-e00a01b2c241" },
    settings: {},
  },
  context: { buildDefinitionName: "CI", buildId: 123, lastMergeSourceCommitId: "head" },
};

it.each([
  { label: "old head", context: { ...policy.context, lastMergeSourceCommitId: "old" } },
  { label: "not current", context: { ...policy.context, buildIsNotCurrent: true } },
  { label: "no revision proof", context: null },
])("keeps $label builds pending", ({ context }) => {
  const checks = azureDevOpsChecks({
    policies: [{ ...policy, context }],
    statuses: [],
    headSha: "head",
    iterationId: 2,
    url: "https://dev.azure.com/org/project/_git/repo/pullrequest/42",
  });
  expect(checks[0]?.status).toBe("pending");
});

it("keeps optional CI separate from non-CI policies and accepts repositories without checks", () => {
  const input = {
    statuses: [],
    headSha: "head",
    iterationId: 2,
    url: "https://dev.azure.com/org/project/_git/repo/pullrequest/42",
  };
  expect(azureDevOpsChecks({ ...input, policies: [] })).toEqual([]);
  const optional = { ...policy, configuration: { ...policy.configuration, isBlocking: false } };
  const checks = azureDevOpsChecks({
    ...input,
    policies: [
      optional,
      { ...policy, configuration: { ...policy.configuration, type: { id: "reviewer" } } },
    ],
  });
  expect(checks).toHaveLength(1);
  expect(checks[0]).toMatchObject({ required: false, status: "success" });
});

it("uses the latest status of the current iteration ahead of old or unscoped successes", () => {
  const context = { genre: "lint", name: "check" };
  const checks = azureDevOpsChecks({
    policies: [],
    headSha: "head",
    iterationId: 2,
    url: "https://example.test/pr/42",
    statuses: [
      { id: 9, context, state: "succeeded", iterationId: 1 },
      { id: 10, context, state: "succeeded" },
      { id: 3, context, state: "failed", iterationId: 2 },
      { id: 2, context, state: "pending", iterationId: 2 },
    ],
  });
  expect(checks).toHaveLength(1);
  expect(checks[0]?.status).toBe("failure");
});

it.each(["approved", "rejected", "broken", "queued", "notApplicable"])(
  "uses the %s status-policy evaluation even when the status says success",
  (evaluation) => {
    const checks = azureDevOpsChecks({
      policies: [
        {
          ...policy,
          status: evaluation,
          context: null,
          configuration: {
            ...policy.configuration,
            type: { id: "status-policy" },
            settings: { statusName: "CI", statusGenre: "external" },
          },
        },
      ],
      statuses: [
        {
          id: 1,
          context: { name: "CI", genre: "external" },
          state: "succeeded",
          description: "CI result",
          targetUrl: "https://ci.example.test/build/1",
        },
      ],
      headSha: "head",
      iterationId: 2,
      url: "https://example.test/pr/42",
    });
    expect(checks).toHaveLength(1);
    expect(checks[0]).toMatchObject({
      status:
        evaluation === "approved"
          ? "success"
          : evaluation === "rejected"
            ? "failure"
            : evaluation === "broken"
              ? "action-required"
              : evaluation === "notApplicable"
                ? "skipped"
                : "pending",
      description: "CI result",
      url: "https://ci.example.test/build/1",
      required: true,
    });
  },
);

it("includes statuses applying to the whole PR when no policy covers them", () => {
  const checks = azureDevOpsChecks({
    policies: [],
    statuses: [{ id: 1, context: { name: "CI" }, state: "succeeded" }],
    headSha: "head",
    iterationId: 2,
    url: "https://example.test/pr/42",
  });
  expect(checks).toHaveLength(1);
  expect(checks[0]).toMatchObject({ status: "success", required: false });
});

it.each(["queued", "running", "approved", "rejected"])(
  "reports expiration for a current %s build",
  (status) => {
    const checks = azureDevOpsChecks({
      policies: [{ ...policy, status, context: { ...policy.context, isExpired: true } }],
      statuses: [],
      headSha: "head",
      iterationId: 2,
      url: "https://example.test/pr/42",
    });
    expect(checks[0]?.status).toBe(
      status === "queued" || status === "running" ? "pending" : "failure",
    );
  },
);
