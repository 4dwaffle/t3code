import { assert, describe, expect, it, vi } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ChildProcessSpawner } from "effect/process";

import { parseChangeRequestUrl } from "@t3tools/shared/changeRequestUrl";
import * as GitLabCli from "./GitLabCli.ts";

import * as GitLabPullRequestCli from "./GitLabPullRequestCli.ts";
import { gitLabViewerPermissions, make } from "./GitLabPullRequestProvider.ts";

describe("gitLabViewerPermissions", () => {
  it("offers everything to a viewer GitLab says can merge", () => {
    expect(gitLabViewerPermissions({ viewerCanMerge: true })).toEqual({
      actions: [
        "merge",
        "ready",
        "draft",
        "close",
        "reopen",
        "update-branch",
        "enable-auto-merge",
        "disable-auto-merge",
      ],
      comment: true,
      resolve: true,
      verdicts: ["comment", "approve"],
      requestReviewers: true,
      updateMethods: ["rebase"],
    });
  });

  it("keeps merge, now and later, from a viewer GitLab says cannot", () => {
    expect(gitLabViewerPermissions({ viewerCanMerge: false })).toEqual({
      actions: ["ready", "draft", "close", "reopen"],
      comment: true,
      resolve: true,
      verdicts: ["comment", "approve"],
      requestReviewers: true,
    });
  });

  it("names no way of updating a branch it will not let this viewer update", () => {
    expect(gitLabViewerPermissions({ viewerCanMerge: false }).updateMethods).toBeUndefined();
  });

  it("treats an author with read access as any other reader, which is all GitLab says", () => {
    expect(gitLabViewerPermissions({ viewerCanMerge: false }).actions).toEqual([
      "ready",
      "draft",
      "close",
      "reopen",
    ]);
  });
});

describe("getChangeRequest base freshness", () => {
  const detail = {
    number: 7,
    title: "Merge request 7",
    url: "https://gitlab.com/acme/web/-/merge_requests/7",
    author: null,
    headBranch: "feat/page",
    baseBranch: "main",
    state: "open" as const,
    isDraft: false,
    mergeability: "mergeable" as const,
    additions: 0,
    deletions: 0,
    createdAt: "2026-07-01T00:00:00Z",
    updatedAt: "2026-07-02T00:00:00Z",
    reviewRequestLogins: [],
    labels: [],
    body: "",
    changedFiles: 1,
    mergedAt: null,
    closedAt: null,
    reviewers: [],
    checks: [],
    viewerCanMerge: true,
    reviewerIds: [],
  };

  it.effect("reads checks without fetching project settings", () =>
    Effect.gen(function* () {
      const provider = yield* make.pipe(
        Effect.provide(
          Layer.mock(GitLabPullRequestCli.GitLabPullRequestCli)({
            getMergeRequestDetail: () => Effect.succeed(detail),
          }),
        ),
      );
      const read = provider.getChangeRequestChecks;
      if (read === undefined) return yield* Effect.die("checks read missing");
      expect(
        yield* read({ cwd: "/w", repository: "group/subgroup/web", host: "gitlab.com", number: 7 }),
      ).toEqual({ state: "open", checks: [] });
    }),
  );

  const readWith = (divergence: { readonly divergedCommits?: number }) =>
    Effect.gen(function* () {
      const provider = yield* make;
      return yield* provider.getChangeRequest({
        cwd: "/w",
        repository: "acme/web",
        host: "gitlab.com",
        number: 7,
      });
    }).pipe(
      Effect.provide(
        Layer.mock(GitLabPullRequestCli.GitLabPullRequestCli)({
          getMergeRequestDetail: () => Effect.succeed({ ...detail, ...divergence }),
          getProjectMergeCapabilities: () =>
            Effect.succeed({ merge: true, squash: true, rebase: true }),
        }),
      ),
    );

  it.effect("reads a counted divergence as a branch that has fallen behind", () =>
    Effect.gen(function* () {
      const changeRequest = yield* readWith({ divergedCommits: 3 });

      expect(changeRequest.baseComparison).toBe("behind");
      expect(changeRequest.behindBy).toBe(3);
    }),
  );

  it.effect("reads a divergence of none as a branch that is current", () =>
    Effect.gen(function* () {
      const changeRequest = yield* readWith({ divergedCommits: 0 });

      expect(changeRequest.baseComparison).toBe("up-to-date");
      expect(changeRequest.behindBy).toBe(0);
    }),
  );

  it.effect("says nothing at all where GitLab counted nothing", () =>
    Effect.gen(function* () {
      const changeRequest = yield* readWith({});

      expect(changeRequest.baseComparison).toBe("unknown");
      expect(changeRequest.behindBy).toBeUndefined();
    }),
  );
});

describe("rewriting what has already been said", () => {
  const updateMergeRequest = vi.fn(() => Effect.void);
  const updateNote = vi.fn(() => Effect.void);

  const providerWith = make.pipe(
    Effect.provide(
      Layer.mock(GitLabPullRequestCli.GitLabPullRequestCli)({
        updateMergeRequest,
        updateNote,
      }),
    ),
  );

  it.effect("sends only the half of the merge request the reader rewrote", () =>
    Effect.gen(function* () {
      const provider = yield* providerWith;
      assert.isDefined(provider.updateChangeRequest);

      yield* provider.updateChangeRequest({
        cwd: "/w",
        repository: "acme/web",
        host: "gitlab.com",
        number: 7,
        body: "What this changes.",
      });

      expect(updateMergeRequest).toHaveBeenCalledWith({
        cwd: "/w",
        repository: "acme/web",
        number: 7,
        description: "What this changes.",
      });
    }),
  );

  it.effect("rewrites a positioned comment through the same note as any other", () =>
    Effect.gen(function* () {
      const provider = yield* providerWith;
      assert.isDefined(provider.updateComment);

      yield* provider.updateComment({
        cwd: "/w",
        repository: "acme/web",
        host: "gitlab.com",
        number: 7,
        commentId: "42",
        kind: "review-comment",
        body: "Reworded.",
      });

      expect(updateNote).toHaveBeenCalledWith({
        cwd: "/w",
        repository: "acme/web",
        number: 7,
        noteId: "42",
        body: "Reworded.",
      });
    }),
  );
});

describe("expanding unchanged lines", () => {
  const change = {
    cwd: "/w",
    repository: "acme/web",
    host: "gitlab.com",
    number: 7,
    changeType: "change" as const,
    oldPath: "src/page.ts",
    newPath: "src/page.ts",
  };

  it.effect("reads both sides of a file through the merge request or one of its commits", () =>
    Effect.gen(function* () {
      const getMergeRequestDiffFileContents = vi.fn(() =>
        Effect.succeed({ oldContents: "before\n", newContents: "after\n" }),
      );
      const provider = yield* make.pipe(
        Effect.provide(
          Layer.mock(GitLabPullRequestCli.GitLabPullRequestCli)({
            getMergeRequestDiffFileContents,
          }),
        ),
      );
      assert.isDefined(provider.getDiffFileContents);
      const oneCommit = { ...change, commit: "a1b2c3d" };

      expect(yield* provider.getDiffFileContents(change)).toEqual({
        oldContents: "before\n",
        newContents: "after\n",
      });
      yield* provider.getDiffFileContents(oneCommit);
      expect(getMergeRequestDiffFileContents).toHaveBeenNthCalledWith(1, change);
      expect(getMergeRequestDiffFileContents).toHaveBeenNthCalledWith(2, oneCommit);
    }),
  );

  it.effect("reports a file GitLab cannot expand as a failed read", () =>
    Effect.gen(function* () {
      const provider = yield* make.pipe(
        Effect.provide(
          Layer.mock(GitLabPullRequestCli.GitLabPullRequestCli)({
            getMergeRequestDiffFileContents: () =>
              Effect.fail(
                new GitLabPullRequestCli.GitLabDiffFileContentsUnavailableError({
                  command: "glab",
                  cwd: "/w",
                  path: "assets/logo.png",
                  reason: "binary",
                }),
              ),
          }),
        ),
      );
      assert.isDefined(provider.getDiffFileContents);

      expect(yield* Effect.flip(provider.getDiffFileContents(change))).toMatchObject({
        _tag: "PullRequestProviderError",
        provider: "gitlab",
        operation: "getDiffFileContents",
        reason: "failed",
        detail: "The diff file 'assets/logo.png' is binary.",
      });
    }),
  );
});

describe("GitLab project identity under /gitlab", () => {
  const host = "code.example.com";
  const origin = `http://${host}:8924`;

  const fixture = () => {
    const projects = [
      { id: 1, path_with_namespace: "team/subgroup/project", notes: [] as string[] },
      { id: 2, path_with_namespace: "gitlab/team/subgroup/project", notes: [] as string[] },
    ];
    const resolvedUrls: string[] = [];
    const apiRequests: string[] = [];
    const execute: GitLabCli.GitLabCli["Service"]["execute"] = (input) =>
      Effect.gen(function* () {
        const output = (value: unknown) => ({
          exitCode: ChildProcessSpawner.ExitCode(0),
          stdout: JSON.stringify(value),
          stderr: "",
          stdoutTruncated: false,
          stderrTruncated: false,
          stdoutInvalidUtf8: false,
        });
        const [command, argument, selector] = input.args;
        let path: string;
        if (command === "repo" && argument === "view") {
          assert.isDefined(selector);
          const url = URL.parse(selector);
          // glab removes the configured subfolder once, only from URL selectors.
          path = url
            ? url.pathname
                .replace(/^\/gitlab\//u, "")
                .split("/-/")[0]!
                .replace(/\.git$/u, "")
            : selector;
          resolvedUrls.push(selector);
        } else {
          path = decodeURIComponent(argument?.split("?")[0]?.split("/")[1] ?? "");
          apiRequests.push(argument ?? "");
        }
        const project = projects.find(
          (entry) => entry.path_with_namespace === path || String(entry.id) === path,
        );
        if (project === undefined) {
          return yield* new GitLabCli.GitLabMergeRequestNotFoundError({
            operation: "execute",
            command: "glab",
            cwd: input.cwd,
            reference: path,
            cause: new Error("404 Project Not Found"),
          });
        }
        const webUrl = `${origin}/gitlab/${project.path_with_namespace}`;
        if (
          command === "repo" ||
          argument?.split("?")[0] === `projects/${encodeURIComponent(path)}`
        ) {
          return output({ ...project, web_url: webUrl });
        }
        if (argument?.split("?")[0]?.endsWith("/merge_requests/9/notes")) {
          expect(input.args).toContain("POST");
          project.notes.push(JSON.parse(input.stdin!).body);
          return output({ id: project.notes.length });
        }
        if (/\/merge_requests(?:\/9)?$/u.test(argument?.split("?")[0] ?? "")) {
          const mr = {
            iid: 9,
            project_id: project.id,
            title: `MR in ${project.path_with_namespace}`,
            web_url: `${webUrl}/-/merge_requests/9`,
            source_branch: "feature",
            target_branch: "main",
            created_at: "2026-07-01T00:00:00Z",
            updated_at: "2026-07-02T00:00:00Z",
          };
          return output(argument?.split("?")[0]?.endsWith("/9") ? mr : [mr]);
        }
        return yield* Effect.die(`Unexpected glab command: ${input.args.join(" ")}`);
      });
    return {
      projects,
      resolvedUrls,
      apiRequests,
      provider: make.pipe(
        Effect.provide(
          GitLabPullRequestCli.layer.pipe(
            Layer.provide(Layer.mock(GitLabCli.GitLabCli)({ execute })),
          ),
        ),
      ),
    };
  };

  it.effect("uses actual HTTP clone and browser URLs to read and comment on both projects", () =>
    Effect.gen(function* () {
      const { projects, resolvedUrls, apiRequests, provider: createProvider } = fixture();
      const provider = yield* createProvider;
      for (const project of projects) {
        const url = `${origin}/gitlab/${project.path_with_namespace}/-/merge_requests/9`;
        const linked = parseChangeRequestUrl(url);
        assert.isNotNull(linked);
        for (const webUrl of [`${origin}/gitlab/${project.path_with_namespace}.git`, url]) {
          const reference = {
            cwd: "/w",
            host: linked.host,
            repository: linked.repository,
            number: linked.number,
            webUrl,
          };
          const detail = yield* provider.getChangeRequest(reference);
          expect(detail.title).toBe(`MR in ${project.path_with_namespace}`);
          expect(detail.url).toBe(url);
          yield* provider.comment({ ...reference, body: `Reviewed ${project.id}` });
        }
      }
      expect(projects.map(({ id, notes }) => ({ id, notes }))).toEqual([
        { id: 1, notes: ["Reviewed 1", "Reviewed 1"] },
        { id: 2, notes: ["Reviewed 2", "Reviewed 2"] },
      ]);
      expect(new Set(resolvedUrls.map((url) => new URL(url).origin))).toEqual(new Set([origin]));

      const unavailable = {
        cwd: "/w",
        host,
        repository: "gitlab/team/subgroup/project",
        number: 9,
        webUrl: `${origin}/gitlab/unavailable/project`,
      };
      const requestsBeforeFailure = apiRequests.length;
      expect(yield* Effect.flip(provider.getChangeRequest(unavailable))).toMatchObject({
        _tag: "PullRequestProviderError",
      });
      expect(
        yield* Effect.flip(provider.comment({ ...unavailable, body: "Must not be written" })),
      ).toMatchObject({ _tag: "PullRequestProviderError" });
      expect(apiRequests).toHaveLength(requestsBeforeFailure);
    }),
  );

  it.effect("lists and comments on the raw API project when no URL provenance is supplied", () =>
    Effect.gen(function* () {
      const { projects, provider: createProvider } = fixture();
      const provider = yield* createProvider;
      const reference = {
        cwd: "/w",
        host,
        repository: "gitlab/team/subgroup/project",
        number: 9,
      };
      const page = yield* provider.listChangeRequests({
        ...reference,
        state: "open",
        involvement: "all",
        viewer: "reviewer",
        limit: 10,
      });
      expect(page.items.map(({ title, url }) => ({ title, url }))).toEqual([
        {
          title: "MR in gitlab/team/subgroup/project",
          url: `${origin}/gitlab/gitlab/team/subgroup/project/-/merge_requests/9`,
        },
      ]);
      yield* provider.comment({ ...reference, body: "Reviewed API project 2" });
      expect(projects.map(({ id, notes }) => ({ id, notes }))).toEqual([
        { id: 1, notes: [] },
        { id: 2, notes: ["Reviewed API project 2"] },
      ]);
    }),
  );
});
