/**
 * Gives pi the Football-Ai tool surface.
 *
 * pi ships no MCP client, so this extension does not reimplement anything: it
 * asks the backend what tools exist (`GET /agent/tools`) and proxies each call
 * straight back to it (`POST /agent/tools/{name}`). The tools themselves are
 * the same Python functions the MCP server publishes to Claude Code, formatters
 * included, so both agents see identical numbers.
 *
 * Adding a tool is a backend-only change; nothing here needs to know the names.
 */

import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

const API_BASE = (process.env.FOOTBALL_AI_API || "http://127.0.0.1:8000").replace(/\/+$/, "");

// The backend is on loopback and answers in milliseconds warm, but a cold
// projection can rebuild features. Generous, yet bounded: a hung tool call
// otherwise leaves the user watching a spinner with no way to tell why.
const TOOL_TIMEOUT_MS = Number(process.env.FOOTBALL_AI_TOOL_TIMEOUT_MS || 60_000);

interface JsonSchema {
	type: string;
	items?: { type: string };
	default?: unknown;
}

interface ToolSpec {
	name: string;
	label: string;
	description: string;
	parameters: {
		type: "object";
		properties: Record<string, JsonSchema>;
		required: string[];
	};
}

/** JSON Schema from the backend -> typebox, which is what registerTool wants. */
function toTypebox(schema: JsonSchema): ReturnType<typeof Type.String> {
	switch (schema.type) {
		case "string":
			return Type.String();
		case "integer":
			return Type.Integer();
		case "number":
			return Type.Number();
		case "boolean":
			return Type.Boolean();
		case "array":
			return Type.Array(schema.items?.type === "string" ? Type.String() : Type.Any());
		default:
			return Type.Any();
	}
}

function buildParameters(spec: ToolSpec) {
	const shape: Record<string, unknown> = {};
	for (const [name, schema] of Object.entries(spec.parameters.properties)) {
		const built = toTypebox(schema);
		shape[name] = spec.parameters.required.includes(name) ? built : Type.Optional(built);
	}
	return Type.Object(shape as Parameters<typeof Type.Object>[0]);
}

async function fetchJson(path: string, init?: RequestInit, timeoutMs = TOOL_TIMEOUT_MS) {
	const timer = AbortSignal.timeout(timeoutMs);
	const response = await fetch(`${API_BASE}${path}`, { ...init, signal: timer });
	if (!response.ok) {
		throw new Error(`${path} -> HTTP ${response.status} ${await response.text()}`);
	}
	return response.json();
}

export default async function (pi: ExtensionAPI) {
	let specs: ToolSpec[];
	try {
		specs = (await fetchJson("/agent/tools", undefined, 10_000)) as ToolSpec[];
	} catch (error) {
		// Registering nothing would leave a mute agent that cheerfully answers
		// from memory. Better that startup says why it has no data.
		throw new Error(
			`Football-Ai backend unreachable at ${API_BASE}: ${error instanceof Error ? error.message : error}`,
		);
	}

	for (const spec of specs) {
		pi.registerTool({
			name: spec.name,
			label: spec.label || spec.name,
			description: spec.description,
			promptSnippet: spec.label || spec.name,
			parameters: buildParameters(spec),

			async execute(_toolCallId, params, signal) {
				if (signal?.aborted) throw new Error("aborted");
				const body = JSON.stringify({ arguments: params ?? {} });
				const result = (await fetchJson(`/agent/tools/${spec.name}`, {
					method: "POST",
					headers: { "content-type": "application/json" },
					body,
				})) as { text?: string; error?: string };

				if (result.error) {
					return { content: [{ type: "text", text: `Tool failed: ${result.error}` }], isError: true };
				}
				return {
					content: [{ type: "text", text: result.text ?? "" }],
					details: { tool: spec.name, arguments: params },
				};
			},
		});
	}
}
