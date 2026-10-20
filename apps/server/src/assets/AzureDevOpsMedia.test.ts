import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeHttpPlatform from "@effect/platform-node/NodeHttpPlatform";
import { expect, it } from "@effect/vitest";
import * as AzureDevOpsCli from "@t3tools/source-control-azure-devops/server/AzureDevOpsCli";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import { HttpServerResponse } from "effect/http";
import * as AzureDevOpsMedia from "./AzureDevOpsMedia.ts";
import { assetFileResponse } from "../http.ts";

const base =
  "https://dev.azure.com/org/project/_apis/git/repositories/repo/pullRequests/42/attachments/demo";

it.effect(
  "downloads with Azure CLI auth, reuses bytes for video ranges, and cleans up files on shutdown",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      let downloaded = "";
      const calls: ReadonlyArray<string>[] = [];
      const cli = Layer.mock(AzureDevOpsCli.AzureDevOpsCli)({
        execute: ({ cwd, args }) =>
          Effect.gen(function* () {
            expect(cwd).toBe("/repo");
            calls.push(args);
            yield* fs
              .writeFileString(args[args.indexOf("--out-file") + 1]!, "0123456789")
              .pipe(Effect.orDie);
            return {
              exitCode: ChildProcessSpawner.ExitCode(0),
              stdout: "",
              stderr: "",
              stdoutTruncated: false,
              stderrTruncated: false,
            };
          }),
      });
      yield* Effect.gen(function* () {
        const media = yield* AzureDevOpsMedia.AzureDevOpsMedia;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const file = yield* media.get({ cwd: "/repo", url: `${base}.mp4` });
            downloaded = file.path;
            expect(calls[0]).toEqual([
              "devops",
              "invoke",
              "--organization",
              "https://dev.azure.com/org",
              "--area",
              "git",
              "--resource",
              "pullRequestAttachments",
              "--api-version",
              "7.1",
              "--route-parameters",
              "project=project",
              "repositoryId=repo",
              "pullRequestId=42",
              "fileName=demo.mp4",
              "--accept-media-type",
              "application/octet-stream",
              "--out-file",
              file.path,
              "--only-show-errors",
              "--output",
              "none",
            ]);
            const full = HttpServerResponse.toWeb(yield* assetFileResponse(file));
            expect(full.headers.get("content-type")).toBe("video/mp4");
            expect(yield* Effect.promise(() => full.text())).toBe("0123456789");
            const again = yield* media.get({ cwd: "/repo", url: `${base}.mp4` });
            const ranged = HttpServerResponse.toWeb(yield* assetFileResponse(again, "bytes=2-5"));
            expect(ranged.status).toBe(206);
            expect(ranged.headers.get("content-range")).toBe("bytes 2-5/10");
            expect(yield* Effect.promise(() => ranged.text())).toBe("2345");
            expect(calls).toHaveLength(1);
            const image = HttpServerResponse.toWeb(
              yield* assetFileResponse(yield* media.get({ cwd: "/repo", url: `${base}.gif` })),
            );
            expect(image.headers.get("content-type")).toBe("image/gif");
            expect(yield* Effect.promise(() => image.text())).toBe("0123456789");
          }),
        );
        expect(yield* fs.exists(downloaded)).toBe(true);
      }).pipe(Effect.provide(AzureDevOpsMedia.layer.pipe(Layer.provide(cli))));
      expect(yield* fs.exists(downloaded)).toBe(false);
    }).pipe(Effect.provide(Layer.merge(NodeServices.layer, NodeHttpPlatform.layer))),
);

it.effect("rejects unsupported endpoints before invoking Azure CLI", () => {
  let calls = 0;
  return Effect.gen(function* () {
    const media = yield* AzureDevOpsMedia.AzureDevOpsMedia;
    for (const url of ["https://example.test/a.gif", `${base}.html`]) {
      const error = yield* media.get({ cwd: "/repo", url }).pipe(Effect.flip);
      expect(error.message).toBe("Failed to load Azure DevOps attachment.");
      expect(error.cause).toBeUndefined();
    }
    expect(calls).toBe(0);
  }).pipe(
    Effect.provide(
      AzureDevOpsMedia.layer
        .pipe(
          Layer.provide(
            Layer.mock(AzureDevOpsCli.AzureDevOpsCli)({
              execute: () => {
                calls++;
                return Effect.die("Unexpected download");
              },
            }),
          ),
        )
        .pipe(Layer.provideMerge(NodeServices.layer)),
    ),
  );
});

it.effect("shares renewed capabilities and serves overflow without retaining transient files", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    let calls = 0;
    const paths: string[] = [];
    const cli = Layer.mock(AzureDevOpsCli.AzureDevOpsCli)({
      execute: ({ args }) =>
        Effect.gen(function* () {
          calls++;
          const destination = args[args.indexOf("--out-file") + 1]!;
          paths.push(destination);
          yield* fs.writeFileString(destination, "GIF89a").pipe(Effect.orDie);
          return {
            exitCode: ChildProcessSpawner.ExitCode(0),
            stdout: "",
            stderr: "",
            stdoutTruncated: false,
            stderrTruncated: false,
          };
        }),
    });
    yield* Effect.gen(function* () {
      const media = yield* AzureDevOpsMedia.AzureDevOpsMedia;
      const original = {
        kind: "azure-devops-media",
        cwd: "/repo",
        url: `${base}.gif`,
        expiresAt: 1000,
      };
      const first = yield* Effect.scoped(media.get(original));
      const renewedAsset = { ...original, expiresAt: 2000 };
      const renewed = yield* Effect.scoped(media.get(renewedAsset));
      expect(renewed.path).toBe(first.path);
      expect(calls).toBe(1);
      for (let index = 1; index < 20; index++) {
        const file = yield* Effect.scoped(
          Effect.gen(function* () {
            const file = yield* media.get({ cwd: "/repo", url: `${base}-${index}.gif` });
            expect(yield* fs.readFileString(file.path)).toBe("GIF89a");
            return file;
          }),
        );
        expect(yield* fs.exists(file.path)).toBe(index < 16);
        expect(yield* fs.readFileString(first.path)).toBe("GIF89a");
      }
      expect(calls).toBe(20);
    }).pipe(Effect.provide(AzureDevOpsMedia.layer.pipe(Layer.provide(cli))));
    for (const path of paths) expect(yield* fs.exists(path)).toBe(false);
  }).pipe(Effect.provide(NodeServices.layer)),
);

it.effect("retries after a failed authenticated download", () => {
  let calls = 0;
  return Effect.gen(function* () {
    const media = yield* AzureDevOpsMedia.AzureDevOpsMedia;
    const fs = yield* FileSystem.FileSystem;
    const input = { cwd: "/repo", url: `${base}.gif` };
    expect((yield* media.get(input).pipe(Effect.flip)).message).toBe(
      "Failed to load Azure DevOps attachment.",
    );
    expect(yield* fs.readFileString((yield* media.get(input)).path)).toBe("GIF89a");
    expect(calls).toBe(2);
  }).pipe(
    Effect.provide(
      AzureDevOpsMedia.layer
        .pipe(
          Layer.provide(
            Layer.mock(AzureDevOpsCli.AzureDevOpsCli)({
              execute: ({ args }) =>
                Effect.gen(function* () {
                  if (++calls === 1)
                    return yield* new AzureDevOpsCli.AzureDevOpsCliAuthenticationError({
                      operation: "execute",
                      command: "az",
                      cwd: "/repo",
                      argumentCount: args.length,
                      cause: "signed out",
                    });
                  const fs = yield* FileSystem.FileSystem;
                  yield* fs
                    .writeFileString(args[args.indexOf("--out-file") + 1]!, "GIF89a")
                    .pipe(Effect.orDie);
                  return {
                    exitCode: ChildProcessSpawner.ExitCode(0),
                    stdout: "",
                    stderr: "",
                    stdoutTruncated: false,
                    stderrTruncated: false,
                  };
                }).pipe(Effect.provide(NodeServices.layer)),
            }),
          ),
        )
        .pipe(Layer.provideMerge(NodeServices.layer)),
    ),
  );
});
