import { RefreshIcon } from "~/components/ui/refresh-icon";
import { scopeProjectRef } from "@t3tools/client-runtime/environment";
import type { EnvironmentId } from "@t3tools/contracts";
import { createFileRoute, Link, useRouter } from "@tanstack/react-router";
import { useAuth } from "@clerk/react";
import { CheckIcon } from "lucide-react";
import { type ReactNode, useEffect, useEffectEvent, useRef, useState } from "react";

import { isLocalEnvironmentDisabled } from "../localEnvironment";
import { isElectron } from "../env";
import { NoProjectsHero } from "../components/NoProjectsHero";
import { sortScopedProjectsForSidebar } from "../components/Sidebar.logic";
import { Button } from "../components/ui/button";
import { CommandBlock } from "../components/CommandBlock";
import { useT3ConnectAuthPrompt } from "../components/clerk/useT3ConnectAuthPrompt";
import { CloudEnvironmentConnectRows } from "../components/cloud/CloudEnvironmentConnectList";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
} from "../components/ui/empty";
import { SidebarInset } from "../components/ui/sidebar";
import { WorkspacePageHeader } from "../components/WorkspacePageHeader";
import { useNewThreadHandler } from "../hooks/useHandleNewThread";
import {
  readProject,
  useAllEnvironmentShellsBootstrapped,
  useProjects,
  useThreadShells,
} from "../state/entities";
import { useEnvironments, useRelayEnvironmentDiscovery } from "../state/environments";
import { APP_DISPLAY_NAME, DESKTOP_DOWNLOAD_URL } from "~/branding";
import { hasCloudPublicConfig } from "~/cloud/publicConfig";

function ChatIndexRouteView() {
  const { authGateState } = Route.useRouteContext();
  const { environments, isReady } = useEnvironments();

  if (authGateState.status === "hosted-static") {
    if (!isReady) return null;
    if (environments.length === 0) return <HostedEmptyState />;
  }

  return <IndexDraftLanding />;
}

function IndexDraftLanding() {
  const projects = useProjects();
  const threads = useThreadShells();
  const bootstrapped = useAllEnvironmentShellsBootstrapped();
  const handleNewThread = useNewThreadHandler();
  const router = useRouter();
  // Read at effect time: this page stays mounted while a navigation away from
  // "/" is still loading, but the router already points at the new location.
  const isStillOnIndex = useEffectEvent(() => router.state.location.pathname === "/");
  // One start per attempt; a retry request opens the next attempt.
  const startedAttemptRef = useRef<number | null>(null);
  const [startState, setStartState] = useState({ failed: false, retryRequest: 0 });

  const mostRecentProject = bootstrapped
    ? (sortScopedProjectsForSidebar(projects, threads, "updated_at")[0] ?? null)
    : null;
  const attempt = startState.retryRequest;

  useEffect(() => {
    // Starting a draft after the user has left would replace the page they picked.
    if (mostRecentProject === null || startedAttemptRef.current === attempt || !isStillOnIndex()) {
      return;
    }
    const projectRef = scopeProjectRef(mostRecentProject.environmentId, mostRecentProject.id);
    if (readProject(projectRef) === null) {
      return;
    }
    startedAttemptRef.current = attempt;
    void handleNewThread(projectRef, { replace: true })
      .then((opened) => {
        if (opened === null) {
          setStartState((state) => ({ ...state, retryRequest: state.retryRequest + 1 }));
        }
      })
      .catch(() => {
        setStartState((state) => ({ ...state, failed: true }));
      });
  }, [attempt, handleNewThread, mostRecentProject]);

  if (!bootstrapped) {
    return null;
  }
  if (mostRecentProject !== null) {
    return startState.failed ? (
      <DraftStartError
        onRetry={() => {
          setStartState((state) => ({
            failed: false,
            retryRequest: state.retryRequest + 1,
          }));
        }}
      />
    ) : null;
  }
  return <NoProjectsHero />;
}

function DraftStartError({ onRetry }: { readonly onRetry: () => void }) {
  return (
    <SidebarInset className="h-dvh min-h-0 overflow-hidden overscroll-y-none">
      {isElectron ? <WorkspacePageHeader electron /> : null}
      <Empty className="flex-1">
        <EmptyHeader className="max-w-md">
          <EmptyTitle>Couldn’t start a new thread</EmptyTitle>
          <EmptyDescription>
            The project is still available. Try opening the draft again.
          </EmptyDescription>
          <div className="mt-5 flex justify-center">
            <Button size="sm" onClick={onRetry}>
              <RefreshIcon size="md" />
              Try again
            </Button>
          </div>
        </EmptyHeader>
      </Empty>
    </SidebarInset>
  );
}

export const Route = createFileRoute("/_chat/")({
  component: ChatIndexRouteView,
});

function HostedEmptyState() {
  const cloudEnabled = hasCloudPublicConfig();
  const localEnvironmentOff = isLocalEnvironmentDisabled();

  return (
    <SidebarInset className="h-dvh min-h-0 overflow-hidden overscroll-y-none">
      <div className="flex min-h-0 min-w-0 flex-1 flex-col overflow-x-hidden bg-background">
        <WorkspacePageHeader electron={isElectron} className="border-b border-border">
          <div className="flex items-center gap-2">
            <span className="text-sm font-medium text-foreground md:text-muted-foreground/60">
              {APP_DISPLAY_NAME}
            </span>
          </div>
        </WorkspacePageHeader>

        <Empty className="flex-1">
          <EmptyHeader>
            <EmptyTitle>
              {localEnvironmentOff ? "This computer is off" : "No computer connected"}
            </EmptyTitle>
            <EmptyDescription>
              {localEnvironmentOff
                ? "Turn it back on, or connect a different computer."
                : cloudEnabled
                  ? "Your agents run on your computer."
                  : "Your agents run on your computer. Start T3 Code there first."}
            </EmptyDescription>
          </EmptyHeader>
          <EmptyContent>
            {localEnvironmentOff ? (
              <Button render={<Link to="/settings/connections" />} size="sm">
                Open Connections
              </Button>
            ) : (
              <ol className="w-full space-y-4 text-left">
                {cloudEnabled ? <T3ConnectSteps /> : <PairingSteps />}
              </ol>
            )}
          </EmptyContent>
        </Empty>
      </div>
    </SidebarInset>
  );
}

function T3ConnectSteps() {
  const { isLoaded, isSignedIn } = useAuth({ treatPendingAsSignedOut: false });
  const { openAuthPrompt } = useT3ConnectAuthPrompt();
  // Adding the first computer unmounts this screen. Wait for all health checks
  // so every compatible registration is queued before that happens.
  const [autoAddedComputers] = useState(() => new Set<EnvironmentId>());
  const [selectedIds, setSelectedIds] = useState<ReadonlySet<EnvironmentId>>(() => new Set());
  const discovered = [...useRelayEnvironmentDiscovery().environments.values()];
  const discoverySettled =
    discovered.length > 0 && discovered.every(({ availability }) => availability !== "checking");
  return (
    <>
      <SetupStep number={1}>
        {isSignedIn ? (
          <p className="flex items-center gap-1.5 text-muted-foreground">
            <CheckIcon className="size-4" />
            Signed in
          </p>
        ) : (
          <Button size="sm" disabled={!isLoaded} onClick={openAuthPrompt}>
            Sign in
          </Button>
        )}
      </SetupStep>
      <SetupStep number={2}>
        <RunOnComputer command="npx t3 connect" />
        {isSignedIn ? (
          <div className="mt-4 space-y-1.5">
            <CloudEnvironmentConnectRows
              primaryEnvironmentId={null}
              savedEnvironments={[]}
              refreshWhileEmpty
              selection={{
                ...(discoverySettled ? { autoSelectedComputers: autoAddedComputers } : {}),
                selectedIds,
                onChange: (environmentId, selected) =>
                  setSelectedIds((current) => {
                    const next = new Set(current);
                    if (selected) next.add(environmentId);
                    else next.delete(environmentId);
                    return next;
                  }),
              }}
              empty={<p className="text-xs text-muted-foreground">Waiting for your computer…</p>}
            />
          </div>
        ) : null}
      </SetupStep>
    </>
  );
}

function PairingSteps() {
  return (
    <>
      <SetupStep number={1}>
        <RunOnComputer command="npx t3 pair" />
      </SetupStep>
      <SetupStep number={2}>
        <Button render={<Link to="/settings/connections" />} size="sm">
          Add pairing link
        </Button>
      </SetupStep>
    </>
  );
}

function SetupStep({
  number,
  children,
}: {
  readonly number: number;
  readonly children: ReactNode;
}) {
  return (
    <li className="flex gap-3">
      <span className="w-2 shrink-0 pt-1 text-muted-foreground">{number}</span>
      <div className="min-w-0 flex-1">{children}</div>
    </li>
  );
}

function RunOnComputer({ command }: { readonly command: string }) {
  return (
    <>
      <p>Run on your computer:</p>
      <CommandBlock command={command} className="mt-2" />
      <a
        className="mt-2 inline-block text-xs text-muted-foreground underline underline-offset-2 hover:text-foreground"
        href={DESKTOP_DOWNLOAD_URL}
        target="_blank"
        rel="noreferrer noopener"
      >
        Or download the desktop app
      </a>
    </>
  );
}
