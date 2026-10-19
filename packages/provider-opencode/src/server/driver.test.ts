// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProviderInstanceId } from "@t3tools/contracts";
import type { OpenCodeSettings } from "../settings.ts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";
import * as HttpClient from "effect/http/HttpClient";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import * as ProviderLatestVersions from "@t3tools/provider-core/server/ProviderLatestVersions";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as ProviderEventLoggers from "@t3tools/provider-core/server/ProviderEventLoggers";
import * as ProviderMaintenance from "@t3tools/provider-core/server/maintenanceResolver";
import * as OpenCodeRuntime from "./OpenCodeRuntime.ts";
import {
  OPENCODE_2_RESPONSES,
  OPENCODE_2_WORKSPACE_RESPONSES,
  replayOpenCodeServer,
} from "./probeResponses.fixture.ts";
import { OpenCodeDriver, openCodeUpdateFor } from "./driver.ts";
import * as TestProviderHost from "@t3tools/provider-testing/TestProviderHost";

const serverStarts: Array<string> = [];
const reachedServer = (operation: string) =>
  Effect.sync(() => serverStarts.push(operation)).pipe(
    Effect.andThen(
      Effect.fail(
        new OpenCodeRuntime.OpenCodeRuntimeError({
          operation,
          detail: "reached a 1.x server path",
        }),
      ),
    ),
  );
const openCode2Runtime = {
  runOpenCodeCommand: () => Effect.succeed({ stdout: "opencode v2.0.18\n", stderr: "", code: 0 }),
  startOpenCodeServerProcess: () => reachedServer("start"),
  connectToOpenCodeServer: () => reachedServer("connect"),
} as unknown as OpenCodeRuntime.OpenCodeRuntime["Service"];

const layer = Layer.mergeAll(
  IdAllocator.layer,
  McpProviderSessions.layer,
  ProviderLatestVersions.layer,
  TestProviderHost.layer(),
  Layer.succeed(
    ProviderEventLoggers.ProviderEventLoggers,
    ProviderEventLoggers.NoOpProviderEventLoggers,
  ),
  Layer.succeed(OpenCodeRuntime.OpenCodeRuntime, openCode2Runtime),
).pipe(Layer.provideMerge(NodeServices.layer));

const create = (config: Partial<OpenCodeSettings>, http: HttpClient.HttpClient) =>
  OpenCodeDriver.create({
    instanceId: ProviderInstanceId.make("opencode-test"),
    displayName: undefined,
    environment: [],
    enabled: true,
    config: { ...OpenCodeDriver.defaultConfig(), ...config },
  }).pipe(Effect.provideService(HttpClient.HttpClient, http));

const noHttp = HttpClient.make(() => Effect.die("A local binary must not be probed over HTTP"));

it.layer(layer)("OpenCodeDriver runtime selection", (it) => {
  it.effect("never starts a 1.x server for an OpenCode 2 instance", () =>
    Effect.gen(function* () {
      serverStarts.length = 0;
      const instance = yield* create({}, noHttp);
      yield* Effect.flip(
        instance.textGeneration.generateThreadTitle({
          cwd: process.cwd(),
          message: "hello",
          modelSelection: { instanceId: instance.instanceId, model: "opencode/big-pickle" },
        }),
      );
      assert.deepStrictEqual(serverStarts, ["start"]);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "isolates workspace agents and keeps skills and commands when agent discovery fails",
    () =>
      Effect.gen(function* () {
        serverStarts.length = 0;
        const requested: Array<string> = [];
        let failAgents = false;
        const agentDirectories: Array<string | null> = [];
        const server = HttpClient.make((request, url) => {
          const directory = url.searchParams.get("location[directory]");
          if (url.pathname === "/api/agent") agentDirectories.push(directory);
          const responses = {
            ...OPENCODE_2_RESPONSES,
            ...OPENCODE_2_WORKSPACE_RESPONSES,
            "/api/agent": {
              status: failAgents ? 500 : 200,
              contentType: "application/json",
              body: JSON.stringify({
                location: { directory },
                data: [
                  { id: "build", mode: "primary", hidden: false },
                  {
                    id: directory === "/work" ? "reviewer" : "writer",
                    mode: "all",
                    hidden: false,
                  },
                  { id: "internal", mode: "primary", hidden: true },
                  { id: "explore", mode: "subagent", hidden: false },
                ].map((agent) => ({
                  ...agent,
                  name: agent.id,
                  request: { settings: {}, headers: {}, body: {} },
                  permissions: [],
                })),
              }),
            },
          };
          return replayOpenCodeServer(responses, "secret", requested).execute(request);
        });
        const instance = yield* create(
          { serverUrl: "http://127.0.0.1:4096", serverPassword: "secret" },
          server,
        );

        const global = structuredClone(yield* instance.snapshot.getSnapshot);
        const workspace = yield* instance.snapshotForCwd!("/work");
        assert.includeMembers(
          workspace.skills.map((skill) => skill.name),
          ["plum", "opencode"],
        );
        assert.deepStrictEqual(
          workspace.slashCommands.map((command) => command.name),
          ["compact", "init", "review", "bee"],
        );
        assert.include(requested, "/api/skill");
        const choices = (snapshot: typeof workspace) => {
          const descriptor = snapshot.optionDescriptors?.find((option) => option.id === "agent");
          return descriptor?.type === "select"
            ? descriptor.options.map((option) => option.id)
            : undefined;
        };
        assert.deepStrictEqual(choices(workspace), ["build", "reviewer"]);
        assert.deepStrictEqual(choices(yield* instance.snapshotForCwd!("/other")), [
          "build",
          "writer",
        ]);

        failAgents = true;
        const failed = yield* instance.snapshotForCwd!("/work");
        assert.deepStrictEqual(failed.skills, workspace.skills);
        assert.deepStrictEqual(failed.slashCommands, workspace.slashCommands);
        assert.isUndefined(failed.optionDescriptors);
        assert.deepStrictEqual(failed.models, global.models);

        failAgents = false;
        assert.deepStrictEqual(choices(yield* instance.snapshotForCwd!("/work")), [
          "build",
          "reviewer",
        ]);
        assert.deepStrictEqual(agentDirectories, ["/work", "/other", "/work", "/work"]);
        assert.deepStrictEqual(yield* instance.snapshot.getSnapshot, global);
        assert.deepStrictEqual(serverStarts, []);
      }).pipe(Effect.scoped),
  );

  it.effect("keeps workspace skills and commands before the deadline when agents hang", () =>
    Effect.gen(function* () {
      serverStarts.length = 0;
      const agentRequested = yield* Deferred.make<void>();
      let commandReads = 0;
      const server = HttpClient.make((request, url) => {
        if (url.pathname === "/api/agent") {
          return Deferred.succeed(agentRequested, undefined).pipe(Effect.andThen(Effect.never));
        }
        if (url.pathname === "/api/command") commandReads++;
        const responses = {
          ...OPENCODE_2_RESPONSES,
          ...OPENCODE_2_WORKSPACE_RESPONSES,
          // No scan events: discovery must use the supported ten-second fallback.
          "/api/event": { status: 200, contentType: "text/event-stream", body: "" },
          "/api/command":
            commandReads === 1
              ? {
                  status: 200,
                  contentType: "application/json",
                  body: '{"location":{"directory":"/work"},"data":[]}',
                }
              : OPENCODE_2_WORKSPACE_RESPONSES["/api/command"],
        };
        return replayOpenCodeServer(responses, "secret").execute(request);
      });
      const instance = yield* create(
        { serverUrl: "http://127.0.0.1:4096", serverPassword: "secret" },
        server,
      );
      const startedAt = yield* Clock.currentTimeMillis;
      const pending = yield* instance.snapshotForCwd!("/work").pipe(
        Effect.flatMap((workspace) =>
          Clock.currentTimeMillis.pipe(
            Effect.map((completedAt) => ({ workspace, elapsed: completedAt - startedAt })),
          ),
        ),
        Effect.forkChild,
      );
      yield* Deferred.await(agentRequested);
      yield* TestClock.adjust("20 seconds");
      const { workspace, elapsed } = yield* Fiber.join(pending);
      assert.isBelow(elapsed, 20_000);
      assert.strictEqual(commandReads, 2);
      assert.deepStrictEqual(
        workspace.skills.map((skill) => skill.name),
        ["opencode", "plum", "report"],
      );
      assert.deepStrictEqual(
        workspace.slashCommands.map((command) => command.name),
        ["compact", "init", "review", "bee"],
      );
      assert.isUndefined(workspace.optionDescriptors);
      assert.deepStrictEqual(serverStarts, []);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

  it.effect("answers capability reads without waiting on an unreachable server", () =>
    Effect.gen(function* () {
      const hang = HttpClient.make(() => Effect.never);
      const instance = yield* create({ serverUrl: "http://127.0.0.1:9" }, hang);

      const capabilities = yield* instance.orchestrationAdapter
        .getCapabilities()
        .pipe(Effect.forkChild);
      yield* Effect.yieldNow;
      assert.isDefined(capabilities.pollUnsafe());
      yield* Fiber.join(capabilities);
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );
});

// npm links `<prefix>/bin/opencode` into the package that installed it:
// `opencode-ai` for 1.x and `@opencode/cli` (whose bin is `bin/opencode.exe`) for 2.x.
const npmGlobalInstall = (prefix: string, packagePath: ReadonlyArray<string>, bin: string) => {
  const target = NodePath.join(prefix, "lib", "node_modules", ...packagePath, "bin", bin);
  NodeFS.mkdirSync(NodePath.dirname(target), { recursive: true });
  NodeFS.writeFileSync(target, "#!/bin/sh\n");
  NodeFS.chmodSync(target, 0o755);
  const link = NodePath.join(prefix, "bin", "opencode");
  NodeFS.mkdirSync(NodePath.dirname(link), { recursive: true });
  NodeFS.symlinkSync(target, link);
  return link;
};
const resolveUpdate = (generation: "v1" | "v2", binaryPath: string) =>
  ProviderMaintenance.resolveProviderMaintenanceCapabilitiesEffect(openCodeUpdateFor(generation), {
    binaryPath,
    env: { PATH: "" },
  }).pipe(
    Effect.provideService(
      ChildProcessSpawner.ChildProcessSpawner,
      ChildProcessSpawner.make(() => Effect.die("resolving an update must not spawn")),
    ),
    Effect.provide(NodeServices.layer),
  );

it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
  "updates an npm-global OpenCode 2 install as @opencode/cli and a 1.x one as opencode-ai",
  () =>
    Effect.gen(function* () {
      const root = NodeFS.realpathSync(
        NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-opencode-update-")),
      );
      const v2Prefix = NodePath.join(root, "v2");
      const v1Prefix = NodePath.join(root, "v1");
      const v2 = yield* resolveUpdate(
        "v2",
        npmGlobalInstall(v2Prefix, ["@opencode", "cli"], "opencode.exe"),
      );
      assert.deepStrictEqual(v2.update?.args, [
        "install",
        "-g",
        "--prefix",
        v2Prefix,
        "--allow-scripts=@opencode/cli",
        "@opencode/cli@latest",
      ]);
      const v1 = yield* resolveUpdate(
        "v1",
        npmGlobalInstall(v1Prefix, ["opencode-ai"], "opencode"),
      );
      assert.strictEqual(v1.update?.args.at(-1), "opencode-ai@latest");
      assert.isNull(
        (yield* resolveUpdate("v2", NodePath.join(v1Prefix, "bin", "opencode"))).update,
      );
      assert.isNull(
        (yield* resolveUpdate("v1", NodePath.join(v2Prefix, "bin", "opencode"))).update,
      );
    }),
);

const versionOutput: { current: string | undefined } = { current: undefined };
const versionProbes: Array<string> = [];
const changingRuntime = {
  runOpenCodeCommand: ({ binaryPath }: { readonly binaryPath: string }) =>
    Effect.sync(() => versionProbes.push(binaryPath)).pipe(
      Effect.andThen(
        versionOutput.current === undefined
          ? Effect.fail(
              new OpenCodeRuntime.OpenCodeRuntimeError({
                operation: "version",
                detail: "no version",
              }),
            )
          : Effect.succeed({ stdout: versionOutput.current, stderr: "", code: 0 }),
      ),
    ),
  startOpenCodeServerProcess: () => reachedServer("start"),
  connectToOpenCodeServer: () => reachedServer("connect"),
} as unknown as OpenCodeRuntime.OpenCodeRuntime["Service"];
const layerUpdate = Layer.mergeAll(
  IdAllocator.layer,
  McpProviderSessions.layer,
  ProviderLatestVersions.layer,
  TestProviderHost.layer(),
  Layer.succeed(
    ProviderEventLoggers.ProviderEventLoggers,
    ProviderEventLoggers.NoOpProviderEventLoggers,
  ),
  Layer.succeed(OpenCodeRuntime.OpenCodeRuntime, changingRuntime),
).pipe(Layer.provideMerge(NodeServices.layer));

it.layer(layerUpdate)("OpenCodeDriver updates", (it) => {
  it.effect("never runs the binary for a disabled instance's update check", () =>
    Effect.gen(function* () {
      versionProbes.length = 0;
      versionOutput.current = "opencode v2.0.18\n";
      const instance = yield* OpenCodeDriver.create({
        instanceId: ProviderInstanceId.make("opencode-disabled"),
        displayName: undefined,
        environment: [],
        enabled: false,
        config: { ...OpenCodeDriver.defaultConfig(), binaryPath: "opencode" },
      }).pipe(Effect.provideService(HttpClient.HttpClient, noHttp));
      const maintenance = yield* instance.snapshot.resolveMaintenance();
      yield* instance.snapshot.resolveMaintenance({ fresh: true });
      assert.isNull(maintenance.update);
      assert.deepStrictEqual(versionProbes, []);
    }).pipe(Effect.scoped),
  );

  it.effect.skipIf(HostProcess.Platform.defaultValue() === "win32")(
    "offers no package update for an unknown version and follows a changed one on a fresh read",
    () =>
      Effect.gen(function* () {
        const root = NodeFS.realpathSync(
          NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-opencode-driver-update-")),
        );
        const binaryPath = npmGlobalInstall(NodePath.join(root, "v1"), ["opencode-ai"], "opencode");
        const instance = yield* OpenCodeDriver.create({
          instanceId: ProviderInstanceId.make("opencode-update"),
          displayName: undefined,
          environment: [],
          enabled: true,
          config: { ...OpenCodeDriver.defaultConfig(), binaryPath },
        }).pipe(Effect.provideService(HttpClient.HttpClient, noHttp));

        versionOutput.current = undefined;
        const unknown = yield* instance.snapshot.resolveMaintenance({ fresh: true });
        assert.isNull(unknown.update);
        assert.isNull(unknown.packageName);

        versionOutput.current = "1.18.32\n";
        const v1 = yield* instance.snapshot.resolveMaintenance({ fresh: true });
        assert.strictEqual(v1.packageName, "opencode-ai");

        versionOutput.current = "opencode v2.0.18\n";
        const v2 = yield* instance.snapshot.resolveMaintenance({ fresh: true });
        assert.strictEqual(v2.packageName, "@opencode/cli");
        assert.isNull(v2.update);
      }).pipe(Effect.scoped),
  );
});
