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
): Promise<void> {
  const client = new OpenAI({
    baseURL: config.baseUrl,
    apiKey: config.apiKey,
  });

  const stream = await client.chat.completions.create({
    model: config.model,
    messages: [{ role: "user", content: prompt }],
    stream: true,
  });

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
