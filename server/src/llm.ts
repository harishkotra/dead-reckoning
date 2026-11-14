// server/llm.ts
//
// A thin wrapper around any OpenAI-compatible streaming endpoint (Ollama,
// LM Studio, vLLM, OpenAI itself, ...). Configuration comes from .env via
// OPENAI_BASE_URL, OPENAI_API_KEY and OPENAI_MODEL.
//
// The wrapper's only job is to turn a prompt into a sequence of streamed
// tokens, handing each one to an async onToken callback. It has no knowledge
// of sessions, journals or resumption — the gRPC server layers those on top.
// Empty delta chunks are skipped silently, which some providers emit.

import OpenAI from "openai";

/** Configuration required to talk to the OpenAI-compatible endpoint. */
export interface LlmConfig {
  baseUrl: string;
  apiKey: string;
  model: string;
}

/** Read LLM configuration from the environment. */
export function loadLlmConfig(): LlmConfig {
  const baseUrl = process.env.OPENAI_BASE_URL;
  const apiKey = process.env.OPENAI_API_KEY;
  const model = process.env.OPENAI_MODEL;
  if (!baseUrl || !apiKey || !model) {
    throw new Error(
      "Missing LLM configuration. Set OPENAI_BASE_URL, OPENAI_API_KEY and OPENAI_MODEL in .env",
    );
  }
  return { baseUrl, apiKey, model };
}

/**
 * Stream a completion for `prompt`, invoking `onToken` for every non-empty
 * delta chunk as it arrives. Resolves when the stream ends cleanly, or rejects
 * if the upstream stream fails (the caller decides how to handle a mid-stream
 * drop — that is the resumability problem this project solves).
 */
export async function streamCompletion(
  config: LlmConfig,
  prompt: string,
  onToken: (token: string) => Promise<void>,
  options: {
    /** Text already generated; the model is asked to continue from it. */
    continueFrom?: string;
    /** Abort the upstream request (Cancel RPC). */
    signal?: AbortSignal;
  } = {},
): Promise<void> {
  const client = new OpenAI({
    baseURL: config.baseUrl,
    apiKey: config.apiKey,
  });

  // Continuation (after a server restart) is deliberately implemented as a
  // prompt-level hand-off, not a hidden state transfer: the model is shown what
  // it already wrote and told to continue. That is exactly why naive resumption
  // is unsafe — the continuation is coherent, but not bit-identical to the run
  // that was interrupted. Same-session resumes never take this path: they
  // attach to the still-running job.
  const userContent = options.continueFrom
    ? `${prompt}\n\nYou have already written the following. Continue from exactly where it stops. Do not repeat it, do not summarise it, do not start over:\n\n${options.continueFrom}`
    : prompt;

  const stream = await client.chat.completions.create(
    {
      model: config.model,
      messages: [{ role: "user", content: userContent }],
      stream: true,
    },
    { signal: options.signal },
  );

  for await (const chunk of stream) {
    const delta = chunk.choices[0]?.delta?.content;
    // Some providers emit empty delta chunks (e.g. role-only chunks, or
    // whitespace) — skip them silently.
    if (!delta || delta.length === 0) {
      continue;
    }
    await onToken(delta);
  }
}
