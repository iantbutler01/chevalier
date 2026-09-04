import * as native from "./native.js";

export type ProgrammaticExecEvent =
  | { readonly type: "stdout" | "stderr"; readonly text: string }
  | { readonly type: "exit"; readonly code: number }
  | { readonly type: "timeout" };

export interface ProgrammaticExecSession {
  write(data: string): Promise<void>;
  eof(): Promise<void>;
  signal(signal: number): Promise<void>;
  next(): Promise<ProgrammaticExecEvent | null>;
}

export interface ProgrammaticSandbox {
  startExec(options: {
    readonly command: string;
    readonly timeoutMs: number;
    readonly signal: AbortSignal;
  }): Promise<ProgrammaticExecSession>;
}

export interface ProgrammaticToolDescriptor {
  readonly name: string;
  readonly description: string;
  readonly schema: object;
}

export interface ProgrammaticExecutionOptions {
  readonly sandbox?: ProgrammaticSandbox;
  readonly tools: readonly ProgrammaticToolDescriptor[];
  readonly dispatch: (
    name: string,
    args: unknown,
    context: { readonly signal: AbortSignal },
  ) => Promise<unknown>;
  readonly signal?: AbortSignal;
  readonly timeoutMs?: number;
}

export interface ProgrammaticResult {
  readonly output: readonly unknown[];
}

type Request =
  | { op: "start"; id: number; command: string; timeoutMs: number }
  | { op: "call"; id: number; name: string; args: unknown }
  | { op: "cancel"; id: number }
  | { op: "write"; sessionId: number; data: string }
  | { op: "next"; sessionId: number }
  | { op: "signal"; sessionId: number; signal: number };

export const programmaticToolDescription = (
  tools: readonly ProgrammaticToolDescriptor[],
): string => native.programmaticDescription(tools);

export async function executeProgrammatic(
  code: string,
  options: ProgrammaticExecutionOptions,
): Promise<ProgrammaticResult> {
  const sandbox = options.sandbox;
  if (!sandbox) throw new Error("Programmatic execution can only be enabled with a sandbox");
  if (options.timeoutMs !== undefined && (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs < 1 || options.timeoutMs > 2_147_483_647)) {
    throw new Error("Invalid programmatic timeoutMs");
  }
  options.signal?.throwIfAborted();
  const sessions = new Map<number, ProgrammaticExecSession>();
  const controllers = new Map<number, AbortController>();
  const cancelled = new Set<number>();
  const controllerFor = (id: number) => {
    const controller = new AbortController();
    controllers.set(id, controller);
    if (cancelled.delete(id)) controller.abort();
    return controller;
  };
  const execution = new native.ProgrammaticExecution(async (request: Request) => {
    if (request.op === "cancel") {
      const controller = controllers.get(request.id);
      if (controller) controller.abort();
      else cancelled.add(request.id);
      return null;
    }
    if (request.op === "start") {
      const controller = controllerFor(request.id);
      const session = await sandbox.startExec({
        command: request.command,
        timeoutMs: request.timeoutMs,
        signal: controller.signal,
      });
      sessions.set(request.id, session);
      return null;
    }
    if (request.op === "call") {
      const controller = controllerFor(request.id);
      try {
        return (await options.dispatch(request.name, request.args, { signal: controller.signal })) ?? null;
      } finally {
        controllers.delete(request.id);
      }
    }
    const session = sessions.get(request.sessionId);
    if (!session) throw new Error("Unknown programmatic sandbox session");
    switch (request.op) {
      case "write": await session.write(request.data); return null;
      case "next": return session.next();
      case "signal": await session.signal(request.signal); return null;
    }
  });
  const abort = () => execution.cancel();
  options.signal?.addEventListener("abort", abort, { once: true });
  if (options.signal?.aborted) abort();
  try {
    return await execution.execute(code, options.tools, options.timeoutMs);
  } finally {
    options.signal?.removeEventListener("abort", abort);
  }
}
