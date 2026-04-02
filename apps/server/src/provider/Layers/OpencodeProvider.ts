import type {
  ModelCapabilities,
  OpencodeSettings,
  ServerProvider,
  ServerProviderModel,
} from "@t3tools/contracts";
import { Effect, Equal, Layer, Option, Result, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  buildServerProvider,
  DEFAULT_TIMEOUT_MS,
  detailFromResult,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
} from "../providerSnapshot.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import { OpencodeProvider } from "../Services/OpencodeProvider.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { ServerSettingsError } from "@t3tools/contracts";

const PROVIDER = "opencode" as const;

const EMPTY_CAPABILITIES: ModelCapabilities = {
  reasoningEffortLevels: [],
  supportsFastMode: false,
  supportsThinkingToggle: false,
  contextWindowOptions: [],
  promptInjectedEffortLevels: [],
};

const BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [
  {
    slug: "opencode-go/glm-5",
    name: "GLM-5",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "opencode-go/kimi-k2.5",
    name: "Kimi K2.5",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "opencode-go/mimo-v2-pro",
    name: "MiMo-V2-Pro",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "opencode-go/mimo-v2-omni",
    name: "MiMo-V2-Omni",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "opencode-go/minimax-m2.7",
    name: "MiniMax M2.7",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
  {
    slug: "opencode-go/minimax-m2.5",
    name: "MiniMax M2.5",
    isCustom: false,
    capabilities: EMPTY_CAPABILITIES,
  },
];

export function getOpencodeModelCapabilities(_model: string | null | undefined): ModelCapabilities {
  return EMPTY_CAPABILITIES;
}

const runOpencodeCommand = Effect.fn("runOpencodeCommand")(function* (args: ReadonlyArray<string>) {
  const settingsService = yield* ServerSettingsService;
  const opencodeSettings = yield* settingsService.getSettings.pipe(
    Effect.map((settings) => settings.providers.opencode),
  );
  const command = ChildProcess.make(opencodeSettings.binaryPath, [...args], {
    shell: process.platform === "win32",
  });
  return yield* spawnAndCollect(opencodeSettings.binaryPath, command);
});

export const checkOpencodeProviderStatus = Effect.fn("checkOpencodeProviderStatus")(
  function* (): Effect.fn.Return<
    ServerProvider,
    ServerSettingsError,
    ChildProcessSpawner.ChildProcessSpawner | ServerSettingsService
  > {
    const opencodeSettings = yield* Effect.service(ServerSettingsService).pipe(
      Effect.flatMap((service) => service.getSettings),
      Effect.map((settings) => settings.providers.opencode),
    );
    const checkedAt = new Date().toISOString();
    const models = providerModelsFromSettings(
      BUILT_IN_MODELS,
      PROVIDER,
      opencodeSettings.customModels,
    );

    if (!opencodeSettings.enabled) {
      return buildServerProvider({
        provider: PROVIDER,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "OpenCode is disabled in T3 Code settings.",
        },
      });
    }

    const versionProbe = yield* runOpencodeCommand(["--version"]).pipe(
      Effect.timeoutOption(DEFAULT_TIMEOUT_MS),
      Effect.result,
    );

    if (Result.isFailure(versionProbe)) {
      const error = versionProbe.failure;
      return buildServerProvider({
        provider: PROVIDER,
        enabled: opencodeSettings.enabled,
        checkedAt,
        models,
        probe: {
          installed: !isCommandMissingCause(error),
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: isCommandMissingCause(error)
            ? "OpenCode CLI (`opencode`) is not installed or not on PATH."
            : `Failed to execute OpenCode CLI health check: ${error instanceof Error ? error.message : String(error)}.`,
        },
      });
    }

    if (Option.isNone(versionProbe.success)) {
      return buildServerProvider({
        provider: PROVIDER,
        enabled: opencodeSettings.enabled,
        checkedAt,
        models,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: "OpenCode CLI is installed but failed to run. Timed out while running command.",
        },
      });
    }

    const version = versionProbe.success.value;
    const parsedVersion = parseGenericCliVersion(`${version.stdout}\n${version.stderr}`);

    if (version.code !== 0) {
      const detail = detailFromResult(version);
      return buildServerProvider({
        provider: PROVIDER,
        enabled: opencodeSettings.enabled,
        checkedAt,
        models,
        probe: {
          installed: true,
          version: parsedVersion,
          status: "error",
          auth: { status: "unknown" },
          message: detail
            ? `OpenCode CLI is installed but failed to run. ${detail}`
            : "OpenCode CLI is installed but failed to run.",
        },
      });
    }

    return buildServerProvider({
      provider: PROVIDER,
      enabled: opencodeSettings.enabled,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: parsedVersion,
        status: "ready",
        auth: { status: "unknown" },
      },
    });
  },
);

export const OpencodeProviderLive = Layer.effect(
  OpencodeProvider,
  Effect.gen(function* () {
    const serverSettings = yield* ServerSettingsService;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

    const checkProvider = checkOpencodeProviderStatus().pipe(
      Effect.provideService(ServerSettingsService, serverSettings),
      Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    );

    return yield* makeManagedServerProvider<OpencodeSettings>({
      getSettings: serverSettings.getSettings.pipe(
        Effect.map((settings) => settings.providers.opencode),
        Effect.orDie,
      ),
      streamSettings: serverSettings.streamChanges.pipe(
        Stream.map((settings) => settings.providers.opencode),
      ),
      haveSettingsChanged: (previous, next) => !Equal.equals(previous, next),
      checkProvider,
    });
  }),
);
