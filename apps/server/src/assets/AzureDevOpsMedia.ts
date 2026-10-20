import { azureDevOpsMediaLocation } from "@t3tools/shared/azureDevOpsMedia";
import * as AzureDevOpsCli from "@t3tools/source-control-azure-devops/server/AzureDevOpsCli";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as RcMap from "effect/RcMap";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import * as Mime from "effect/http/Mime";

class AzureDevOpsMediaFetchError extends Schema.TaggedError<AzureDevOpsMediaFetchError>()(
  "AzureDevOpsMediaFetchError",
  { cause: Schema.optional(Schema.Defect()) },
) {
  override get message(): string {
    return "Failed to load Azure DevOps attachment.";
  }
}

const isAzureDevOpsMediaFetchError = Schema.is(AzureDevOpsMediaFetchError);

class MediaKey extends Data.Class<{ readonly cwd: string; readonly url: string }> {}

export class AzureDevOpsMedia extends Context.Service<
  AzureDevOpsMedia,
  {
    readonly get: (input: {
      readonly cwd: string;
      readonly url: string;
    }) => Effect.Effect<
      { readonly path: string; readonly mimeType: string },
      AzureDevOpsMediaFetchError,
      Scope.Scope
    >;
  }
>()("t3/assets/AzureDevOpsMedia") {}

const make = Effect.gen(function* () {
  const azure = yield* AzureDevOpsCli.AzureDevOpsCli;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  // Range requests share a download. Each response holds a reference until its stream closes;
  // idle downloads and the service's shutdown both remove their private temporary directories.
  const download = Effect.fn("AzureDevOpsMedia.download")(function* (key: MediaKey) {
    const location = azureDevOpsMediaLocation(key.url);
    const mimeType = location ? Option.getOrNull(Mime.getType(location.fileName)) : null;
    if (!location || !mimeType || !/^(?:image|video|audio)\//iu.test(mimeType)) {
      return yield* new AzureDevOpsMediaFetchError({});
    }
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-ado-media-" });
    const destination = path.join(directory, `attachment${path.extname(location.fileName)}`);
    // `devops invoke` uses the same PAT/tenant resolution as PR reads. `az rest` does not.
    yield* azure.execute({
      cwd: key.cwd,
      args: [
        "devops",
        "invoke",
        "--organization",
        location.organizationUrl,
        "--area",
        "git",
        "--resource",
        "pullRequestAttachments",
        "--api-version",
        "7.1",
        "--route-parameters",
        `project=${location.project}`,
        `repositoryId=${location.repositoryId}`,
        `pullRequestId=${location.pullRequestId}`,
        `fileName=${location.fileName}`,
        "--accept-media-type",
        "application/octet-stream",
        "--out-file",
        destination,
        "--only-show-errors",
        "--output",
        "none",
      ],
      timeoutMs: 120_000,
    });
    return { path: destination, mimeType };
  });
  const downloads = yield* RcMap.make({
    capacity: 16,
    idleTimeToLive: "1 minute",
    lookup: download,
  });
  return AzureDevOpsMedia.of({
    get: Effect.fn("AzureDevOpsMedia.get")(function* (input) {
      const key = new MediaKey({ cwd: input.cwd, url: input.url });
      return yield* RcMap.get(downloads, key).pipe(
        // Overflow belongs to the request scope instead of displacing an active reader.
        Effect.catchTags({ ExceededCapacityError: () => download(key) }),
        Effect.tapError(() => RcMap.invalidate(downloads, key)),
        Effect.mapError((cause) =>
          isAzureDevOpsMediaFetchError(cause) ? cause : new AzureDevOpsMediaFetchError({ cause }),
        ),
      );
    }),
  });
});

export const layer = Layer.effect(AzureDevOpsMedia, make);
