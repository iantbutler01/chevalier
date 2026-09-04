"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.programmaticToolDescription = void 0;
exports.executeProgrammatic = executeProgrammatic;
const native = __importStar(require("./native.js"));
const programmaticToolDescription = (tools) => native.programmaticDescription(tools);
exports.programmaticToolDescription = programmaticToolDescription;
async function executeProgrammatic(code, options) {
    const sandbox = options.sandbox;
    if (!sandbox)
        throw new Error("Programmatic execution can only be enabled with a sandbox");
    if (options.timeoutMs !== undefined && (!Number.isSafeInteger(options.timeoutMs) || options.timeoutMs < 1 || options.timeoutMs > 2_147_483_647)) {
        throw new Error("Invalid programmatic timeoutMs");
    }
    options.signal?.throwIfAborted();
    const sessions = new Map();
    const controllers = new Map();
    const cancelled = new Set();
    const controllerFor = (id) => {
        const controller = new AbortController();
        controllers.set(id, controller);
        if (cancelled.delete(id))
            controller.abort();
        return controller;
    };
    const execution = new native.ProgrammaticExecution(async (request) => {
        if (request.op === "cancel") {
            const controller = controllers.get(request.id);
            if (controller)
                controller.abort();
            else
                cancelled.add(request.id);
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
            }
            finally {
                controllers.delete(request.id);
            }
        }
        const session = sessions.get(request.sessionId);
        if (!session)
            throw new Error("Unknown programmatic sandbox session");
        switch (request.op) {
            case "write":
                await session.write(request.data);
                return null;
            case "next": return session.next();
            case "signal":
                await session.signal(request.signal);
                return null;
        }
    });
    const abort = () => execution.cancel();
    options.signal?.addEventListener("abort", abort, { once: true });
    if (options.signal?.aborted)
        abort();
    try {
        return await execution.execute(code, options.tools, options.timeoutMs);
    }
    finally {
        options.signal?.removeEventListener("abort", abort);
    }
}
