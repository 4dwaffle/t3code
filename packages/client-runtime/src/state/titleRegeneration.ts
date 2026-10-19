import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type { Atom, AtomRegistry } from "effect/reactivity";

import type { EnvironmentThreadShell } from "./models.ts";

/** Arm before sending the command; abort when it is rejected. A null shell
    cancels observation, including when the caller loses its live connection. */
export function waitForTitleRegenerationFailure(input: {
  readonly registry: AtomRegistry.AtomRegistry;
  readonly atom: Atom.Atom<EnvironmentThreadShell | null>;
  readonly requestId: string;
  readonly timeoutMs: number;
  readonly signal?: AbortSignal;
}): Promise<string | null> {
  let unsubscribe: (() => void) | undefined;
  const observation = Effect.callback<string | null>((resume) => {
    let sawInFlight = false;
    const inspect = (thread: EnvironmentThreadShell | null) => {
      if (thread === null) return resume(Effect.succeed(null));
      if (thread.titleRegenerationFailure?.requestId === input.requestId) {
        return resume(Effect.succeed(thread.titleRegenerationFailure.message));
      }
      if (thread.titleRegeneration?.requestId === input.requestId) {
        sawInFlight = true;
      } else if (sawInFlight) {
        resume(Effect.succeed(null));
      }
    };
    unsubscribe = input.registry.subscribe(input.atom, inspect);
    inspect(input.registry.get(input.atom));
  }).pipe(Effect.ensuring(Effect.sync(() => unsubscribe?.())));
  // A queued generation may outlast this deadline. Only abandon requests that
  // never started (or whose successful completion arrived without its start).
  const unobserved = Effect.sleep(input.timeoutMs).pipe(
    Effect.andThen(() =>
      input.registry.get(input.atom)?.titleRegeneration?.requestId === input.requestId
        ? Effect.never
        : Effect.succeed(null),
    ),
  );
  return Effect.runPromiseExit(Effect.raceFirst(observation, unobserved), {
    signal: input.signal,
  }).then((exit) => (Exit.isSuccess(exit) ? exit.value : null));
}
