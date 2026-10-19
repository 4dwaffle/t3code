// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect's public File API omits exact large inodes and native descriptors.
import * as NodeFSP from "node:fs/promises";

import type { AgentSessionImportSource } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";

export type TranscriptIdentity = Pick<
  AgentSessionImportSource,
  "filePath" | "size" | "mtimeMs" | "device" | "inode" | "birthtimeMs"
>;

class TranscriptFileError extends Schema.TaggedError<TranscriptFileError>()("TranscriptFileError", {
  filePath: Schema.String,
  operation: Schema.Literals(["stat", "open", "read"]),
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return `Could not ${this.operation} transcript file.`;
  }
}

export class NodeTranscriptFileSystem extends Context.Service<
  NodeTranscriptFileSystem,
  {
    readonly stat: (filePath: string) => Effect.Effect<TranscriptIdentity, TranscriptFileError>;
    readonly open: (filePath: string) => Effect.Effect<
      {
        readonly identity: Effect.Effect<TranscriptIdentity, TranscriptFileError>;
        readonly readAlloc: (
          size: number,
        ) => Effect.Effect<Option.Option<Uint8Array>, TranscriptFileError>;
      },
      TranscriptFileError,
      Scope.Scope
    >;
  }
>()("t3/project/NodeTranscriptFileSystem") {}

function transcriptIdentity(
  filePath: string,
  stats: import("node:fs").BigIntStats,
): TranscriptIdentity {
  return {
    filePath,
    size: Number(stats.size),
    mtimeMs: Number(stats.mtimeMs),
    device: Number(stats.dev),
    inode: stats.ino === 0n ? null : stats.ino.toString(),
    birthtimeMs: Number(stats.birthtimeMs),
  };
}

export const layer = Layer.sync(NodeTranscriptFileSystem, () =>
  NodeTranscriptFileSystem.of({
    stat: (filePath) =>
      Effect.tryPromise({
        try: () => NodeFSP.stat(filePath, { bigint: true }),
        catch: (cause) => new TranscriptFileError({ filePath, operation: "stat", cause }),
      }).pipe(Effect.map((stats) => transcriptIdentity(filePath, stats))),
    open: Effect.fn("NodeTranscriptFileSystem.open")(function* (filePath) {
      const handle = yield* Effect.acquireRelease(
        Effect.tryPromise({
          try: () => NodeFSP.open(filePath, "r"),
          catch: (cause) => new TranscriptFileError({ filePath, operation: "open", cause }),
        }),
        (handle) => Effect.promise(() => handle.close()),
      );
      // Identity checks and reads must use this handle even if the pathname is replaced.
      return {
        identity: Effect.tryPromise({
          try: () => handle.stat({ bigint: true }),
          catch: (cause) => new TranscriptFileError({ filePath, operation: "stat", cause }),
        }).pipe(Effect.map((stats) => transcriptIdentity(filePath, stats))),
        readAlloc: (size: number) =>
          Effect.tryPromise({
            try: async () => {
              const buffer = new Uint8Array(size);
              const { bytesRead } = await handle.read(buffer, 0, size, null);
              return bytesRead === 0 ? Option.none() : Option.some(buffer.subarray(0, bytesRead));
            },
            catch: (cause) => new TranscriptFileError({ filePath, operation: "read", cause }),
          }),
      };
    }),
  }),
);
