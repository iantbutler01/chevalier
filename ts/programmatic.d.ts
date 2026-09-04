export type ProgrammaticExecEvent = {
    readonly type: "stdout" | "stderr";
    readonly text: string;
} | {
    readonly type: "exit";
    readonly code: number;
} | {
    readonly type: "timeout";
};
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
    readonly dispatch: (name: string, args: unknown, context: {
        readonly signal: AbortSignal;
    }) => Promise<unknown>;
    readonly signal?: AbortSignal;
    readonly timeoutMs?: number;
}
export interface ProgrammaticResult {
    readonly output: readonly unknown[];
}
export declare const programmaticToolDescription: (tools: readonly ProgrammaticToolDescriptor[]) => string;
export declare function executeProgrammatic(code: string, options: ProgrammaticExecutionOptions): Promise<ProgrammaticResult>;
