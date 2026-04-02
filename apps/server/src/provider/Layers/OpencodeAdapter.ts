import {
  type ProviderRuntimeEvent as ProviderRuntimeEventV2,
  type ProviderSession,
  EventId,
  RuntimeItemId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { DateTime, Effect, Layer, Queue, Random, Stream } from "effect";
import { createOpencode } from "@opencode-ai/sdk";

import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
} from "../Errors.ts";
import { OpencodeAdapter, type OpencodeAdapterShape } from "../Services/OpencodeAdapter.ts";
import type { ProviderThreadSnapshot } from "../Services/ProviderAdapter.ts";

const PROVIDER = "opencode" as const;

interface OpencodeSessionContext {
  readonly client: Awaited<ReturnType<typeof createOpencode>>["client"];
  readonly server: Awaited<ReturnType<typeof createOpencode>>["server"];
  session: ProviderSession;
  readonly opencodeSessionId: string;
  stopped: boolean;
}

export interface OpencodeAdapterLiveOptions {
  readonly client?: Awaited<ReturnType<typeof createOpencode>>["client"];
}

const makeOpencodeAdapter = Effect.fn("makeOpencodeAdapter")(function* (
  _options?: OpencodeAdapterLiveOptions,
) {
  const sessions = new Map<ThreadId, OpencodeSessionContext>();
  const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEventV2>();

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  const nextEventId = Effect.map(Random.nextUUIDv4, (id) => EventId.makeUnsafe(id));
  const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

  const offerRuntimeEvent = (event: ProviderRuntimeEventV2): Effect.Effect<void> =>
    Queue.offer(runtimeEventQueue, event).pipe(Effect.asVoid);

  const requireSession = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const ctx = sessions.get(threadId);
      if (!ctx || ctx.stopped) {
        return yield* new ProviderAdapterSessionNotFoundError({
          provider: PROVIDER,
          threadId,
        });
      }
      return ctx;
    });

  const startSession: OpencodeAdapterShape["startSession"] = Effect.fn("startSession")(
    function* (input) {
      const threadId = input.threadId;
      const existingCtx = sessions.get(threadId);
      if (existingCtx && !existingCtx.stopped) {
        return { ...existingCtx.session };
      }

      const createdAt = yield* nowIso;
      const session: ProviderSession = {
        provider: PROVIDER,
        status: "connecting",
        runtimeMode: input.runtimeMode,
        cwd: input.cwd,
        model: input.modelSelection?.model,
        threadId,
        createdAt,
        updatedAt: createdAt,
      };

      const stamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "session.started",
        eventId: stamp.eventId,
        provider: PROVIDER,
        createdAt: stamp.createdAt,
        threadId,
        payload: {},
        providerRefs: {},
      });

      const { client, server, opencodeSessionId } = yield* Effect.gen(function* () {
        const { client, server } = yield* Effect.tryPromise({
          try: () =>
            createOpencode({
              config: {
                permission: {
                  edit: "allow" as const,
                  bash: "allow" as const,
                  webfetch: "allow" as const,
                },
              },
            }),
          catch: (cause) =>
            new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId,
              detail: `Failed to start opencode: ${cause}`,
              cause,
            }),
        });

        const opencodeSession = yield* Effect.tryPromise({
          try: () => client.session.create({ body: { title: `t3-${threadId}` } }),
          catch: (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "session/create",
              detail: `Failed to create opencode session: ${cause}`,
              cause,
            }),
        });

        if (!opencodeSession.data) {
          return yield* new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId,
            detail: "Failed to create opencode session: no data returned",
          });
        }

        return { client, server, opencodeSessionId: opencodeSession.data.id };
      }).pipe(
        Effect.tapError((error) =>
          Effect.gen(function* () {
            const errorStamp = yield* makeEventStamp();
            yield* offerRuntimeEvent({
              type: "session.state.changed",
              eventId: errorStamp.eventId,
              provider: PROVIDER,
              createdAt: errorStamp.createdAt,
              threadId,
              payload: {
                state: "error",
                reason: `Failed to start: ${String(error)}`,
              },
              providerRefs: {},
            });
          }),
        ),
      );

      const updatedSession: ProviderSession = {
        ...session,
        status: "ready",
        updatedAt: yield* nowIso,
      };

      const ctx: OpencodeSessionContext = {
        client,
        server,
        session: updatedSession,
        opencodeSessionId,
        stopped: false,
      };
      sessions.set(threadId, ctx);

      const readyStamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "session.state.changed",
        eventId: readyStamp.eventId,
        provider: PROVIDER,
        createdAt: readyStamp.createdAt,
        threadId,
        payload: { state: "ready" },
        providerRefs: {},
      });

      return { ...updatedSession };
    },
  );

  const sendTurn: OpencodeAdapterShape["sendTurn"] = Effect.fn("sendTurn")(function* (input) {
    const ctx = yield* requireSession(input.threadId);
    const turnId = TurnId.makeUnsafe(yield* Random.nextUUIDv4);

    const updatedAt = yield* nowIso;
    ctx.session = {
      ...ctx.session,
      status: "running",
      activeTurnId: turnId,
      updatedAt,
    };

    const turnStamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "turn.started",
      eventId: turnStamp.eventId,
      provider: PROVIDER,
      createdAt: turnStamp.createdAt,
      threadId: input.threadId,
      turnId,
      payload: input.modelSelection?.model ? { model: input.modelSelection.model } : {},
      providerRefs: {},
    });

    const promptText = input.input ?? "";

    const result = yield* Effect.tryPromise({
      try: () =>
        ctx.client.session.prompt({
          path: { id: ctx.opencodeSessionId },
          body: {
            parts: [{ type: "text", text: promptText }],
            ...(input.modelSelection?.model
              ? {
                  model: {
                    providerID: input.modelSelection.model.split("/")[0] ?? "anthropic",
                    modelID:
                      input.modelSelection.model.split("/").slice(1).join("/") ||
                      input.modelSelection.model,
                  },
                }
              : {}),
          },
        }),
      catch: (cause) =>
        new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/prompt",
          detail: `Failed to send prompt: ${cause}`,
          cause,
        }),
    });

    const assistantParts = result.data?.parts ?? [];
    let assistantText = "";
    for (const part of assistantParts) {
      if (part.type === "text" && typeof part.text === "string") {
        assistantText += part.text;
      }
    }

    if (assistantText) {
      const deltaStamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "content.delta",
        eventId: deltaStamp.eventId,
        provider: PROVIDER,
        createdAt: deltaStamp.createdAt,
        threadId: input.threadId,
        turnId,
        payload: {
          streamKind: "assistant_text",
          delta: assistantText,
        },
        providerRefs: {},
      });
    }

    for (const part of assistantParts) {
      if (part.type === "tool") {
        const toolStamp = yield* makeEventStamp();
        const itemId = RuntimeItemId.makeUnsafe(yield* Random.nextUUIDv4);
        yield* offerRuntimeEvent({
          type: "item.started",
          eventId: toolStamp.eventId,
          provider: PROVIDER,
          createdAt: toolStamp.createdAt,
          threadId: input.threadId,
          turnId,
          itemId,
          payload: {
            itemType: "dynamic_tool_call",
            status: "completed",
            title: part.tool,
          },
          providerRefs: {},
        });
        const completedStamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          type: "item.completed",
          eventId: completedStamp.eventId,
          provider: PROVIDER,
          createdAt: completedStamp.createdAt,
          threadId: input.threadId,
          turnId,
          itemId,
          payload: {
            itemType: "dynamic_tool_call",
            status: "completed",
          },
          providerRefs: {},
        });
      }
    }

    const completedStamp = yield* makeEventStamp();
    yield* offerRuntimeEvent({
      type: "turn.completed",
      eventId: completedStamp.eventId,
      provider: PROVIDER,
      createdAt: completedStamp.createdAt,
      threadId: input.threadId,
      turnId,
      payload: {
        state: "completed",
        stopReason: null,
      },
      providerRefs: {},
    });

    const completedAt = yield* nowIso;
    ctx.session = {
      ...ctx.session,
      status: "ready",
      activeTurnId: undefined,
      updatedAt: completedAt,
    };

    return {
      threadId: input.threadId,
      turnId,
    };
  });

  const interruptTurn: OpencodeAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
    function* (threadId) {
      const ctx = yield* requireSession(threadId);
      yield* Effect.tryPromise({
        try: () => ctx.client.session.abort({ path: { id: ctx.opencodeSessionId } }),
        catch: (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session/abort",
            detail: `Failed to abort: ${cause}`,
            cause,
          }),
      });

      if (ctx.session.activeTurnId) {
        const stamp = yield* makeEventStamp();
        yield* offerRuntimeEvent({
          type: "turn.aborted",
          eventId: stamp.eventId,
          provider: PROVIDER,
          createdAt: stamp.createdAt,
          threadId,
          turnId: ctx.session.activeTurnId,
          payload: { reason: "interrupted" },
          providerRefs: {},
        });
      }
    },
  );

  const stopSession: OpencodeAdapterShape["stopSession"] = Effect.fn("stopSession")(
    function* (threadId) {
      const ctx = yield* requireSession(threadId);
      ctx.stopped = true;

      const stamp = yield* makeEventStamp();
      yield* offerRuntimeEvent({
        type: "session.exited",
        eventId: stamp.eventId,
        provider: PROVIDER,
        createdAt: stamp.createdAt,
        threadId,
        payload: { exitKind: "graceful" },
        providerRefs: {},
      });

      yield* Effect.sync(() => {
        try {
          ctx.server.close();
        } catch {}
      });
      sessions.delete(threadId);
    },
  );

  const respondToRequest: OpencodeAdapterShape["respondToRequest"] = Effect.fn("respondToRequest")(
    function* (_threadId, _requestId, _decision) {
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToRequest",
        detail: "OpenCode adapter does not support approval requests (permission=allow)",
      });
    },
  );

  const respondToUserInput: OpencodeAdapterShape["respondToUserInput"] = Effect.fn(
    "respondToUserInput",
  )(function* (_threadId, _requestId, _answers) {
    return yield* new ProviderAdapterRequestError({
      provider: PROVIDER,
      method: "respondToUserInput",
      detail: "OpenCode adapter does not support user input requests",
    });
  });

  const listSessions: OpencodeAdapterShape["listSessions"] = () =>
    Effect.sync(() => Array.from(sessions.values(), ({ session }) => ({ ...session })));

  const hasSession: OpencodeAdapterShape["hasSession"] = (threadId) =>
    Effect.sync(() => {
      const ctx = sessions.get(threadId);
      return ctx !== undefined && !ctx.stopped;
    });

  const readThread: OpencodeAdapterShape["readThread"] = Effect.fn("readThread")(
    function* (threadId) {
      yield* requireSession(threadId);
      return { threadId, turns: [] as ProviderThreadSnapshot["turns"] };
    },
  );

  const rollbackThread: OpencodeAdapterShape["rollbackThread"] = Effect.fn("rollbackThread")(
    function* (threadId, numTurns) {
      const ctx = yield* requireSession(threadId);
      yield* Effect.ignore(
        Effect.tryPromise(() =>
          ctx.client.session.revert({
            path: { id: ctx.opencodeSessionId },
            body: { messageID: "" },
          }),
        ),
      );
      void numTurns;
      return { threadId, turns: [] as ProviderThreadSnapshot["turns"] };
    },
  );

  const stopAll: OpencodeAdapterShape["stopAll"] = () =>
    Effect.forEach(
      sessions,
      ([, ctx]) =>
        Effect.sync(() => {
          ctx.stopped = true;
          try {
            ctx.server.close();
          } catch {}
        }),
      { discard: true },
    ).pipe(Effect.tap(() => Queue.shutdown(runtimeEventQueue)));

  yield* Effect.addFinalizer(() =>
    Effect.forEach(
      sessions,
      ([threadId, ctx]) =>
        Effect.gen(function* () {
          if (!ctx.stopped) {
            ctx.stopped = true;
            const stamp = yield* makeEventStamp();
            yield* offerRuntimeEvent({
              type: "session.exited",
              eventId: stamp.eventId,
              provider: PROVIDER,
              createdAt: stamp.createdAt,
              threadId,
              payload: { exitKind: "graceful" },
              providerRefs: {},
            });
            yield* Effect.sync(() => {
              try {
                ctx.server.close();
              } catch {}
            });
          }
        }),
      { discard: true },
    ).pipe(Effect.tap(() => Queue.shutdown(runtimeEventQueue))),
  );

  return {
    provider: PROVIDER,
    capabilities: {
      sessionModelSwitch: "in-session",
    },
    startSession,
    sendTurn,
    interruptTurn,
    readThread,
    rollbackThread,
    respondToRequest,
    respondToUserInput,
    stopSession,
    listSessions,
    hasSession,
    stopAll,
    streamEvents: Stream.fromQueue(runtimeEventQueue),
  } satisfies OpencodeAdapterShape;
});

export const OpencodeAdapterLive = Layer.effect(OpencodeAdapter, makeOpencodeAdapter());

export function makeOpencodeAdapterLive(options?: OpencodeAdapterLiveOptions) {
  return Layer.effect(OpencodeAdapter, makeOpencodeAdapter(options));
}
