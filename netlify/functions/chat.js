// Netlify serverless function — AI Property Concierge ("Windsor")
//
// Proxies chat requests to the Anthropic Claude API so the API key never
// reaches the browser. The key is read from the ANTHROPIC_API_KEY environment
// variable configured in the Netlify dashboard (Site settings → Environment
// variables).
//
// Request  (POST, JSON):  { "message": "user text", "history": [ {role, content}, ... ] }
// Response (SSE stream):   data: {"type":"content_block_delta","delta":{"text":"..."}}
//                          ...
//                          data: [DONE]
//
// The streaming shape matches what the front-end parser in index.html expects.

import Anthropic from "@anthropic-ai/sdk";

// Swap for "claude-haiku-4-5" or "claude-sonnet-4-6" if you want lower cost /
// faster replies — a concierge FAQ rarely needs the most powerful model.
const MODEL = "claude-opus-4-8";
const MAX_TOKENS = 400;
const HISTORY_MAX = 20;

const SYSTEM_PROMPT = `You are Windsor, the AI property concierge for JIN Properties — a luxury student rental portfolio in Windsor, Ontario, Canada. You help prospective student tenants find the right unit across our three houses.

JIN PROPERTIES PORTFOLIO (all furnished, walking distance to the University of Windsor):

653 Bridge Avenue — 2 units, 9 bedrooms total, 9 bathrooms total
  • Upper Unit: 5 bedrooms, 5 bathrooms, 1 kitchen, 1 living-dining room
  • Lower Unit: 4 bedrooms, 4 bathrooms, 1 kitchen, 1 living-dining room, 1 laundry

709 Bridge Avenue — 5 units, 16 bedrooms total, 16 bathrooms total
  • Front 2nd Floor Unit: 3 large bedrooms, 3 bathrooms, kitchen, living-dining room, laundry
  • Front Ground Floor Unit: 3 large bedrooms, 3 bathrooms, kitchen, living-dining room
  • Front Semi-Ground Unit: 3 bedrooms, 3 bathrooms, kitchen, living-dining room, laundry
  • Rear Upper Unit: 5 bedrooms, 5 bathrooms, kitchen, living-dining room, laundry
  • Rear Lower Unit: 2 bedrooms, 2 bathrooms, kitchen, laundry

721 Partington Avenue — 2 units, 8 bedrooms total, 8 bathrooms total
  • Upper Unit: 4 bedrooms, 4 bathrooms, kitchen, living-dining room, laundry
  • Lower Unit: 4 bedrooms, 4 bathrooms, kitchen, living-dining room, laundry

PRICING:
Rooms are priced in three tiers — Large, Medium, Small — per house. Specific rates are not yet published. When asked, say: "Pricing is being confirmed for each room tier — for the most up-to-date rates I'd recommend reaching out directly. I can connect you to the team right now."

NEIGHBOURHOOD:
Bridge Avenue and Partington Avenue are in Windsor's University District — quiet, walkable streets within easy walking distance of the University of Windsor campus, transit, and shops.

CONTACT:
Email: rickyjin88@gmail.com  |  Windsor, Ontario, Canada
Visitors can also book a viewing by filling out the form in the #contact section of this page.

RULES:
- Only answer questions about JIN Properties, the three houses, leasing, room sizes, availability, amenities, the booking process, and Windsor / University of Windsor general living info.
- If a question is unrelated, politely redirect: "I'm here to help you find your ideal student rental at JIN Properties — is there anything I can help you with on that front?"
- Never invent specific pricing or lease terms. Defer to the team for confirmation.
- Always offer a clear next step (book a viewing, contact the team, explore a specific house).
- Keep responses concise — 2 to 4 sentences max unless a comparison is requested.
- Warm, professional, first-person tone.
- When the user seems ready to commit, proactively offer to help them book a viewing.`;

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

function jsonResponse(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...CORS_HEADERS, "Content-Type": "application/json" },
  });
}

export default async (req) => {
  // CORS preflight
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: CORS_HEADERS });
  }

  if (req.method !== "POST") {
    return jsonResponse(405, { error: "Method not allowed. Use POST." });
  }

  if (!process.env.ANTHROPIC_API_KEY) {
    return jsonResponse(500, { error: "Server is missing ANTHROPIC_API_KEY." });
  }

  // Parse and validate the request body
  let payload;
  try {
    payload = await req.json();
  } catch {
    return jsonResponse(400, { error: "Invalid JSON body." });
  }

  const { message, history } = payload || {};

  if (typeof message !== "string" || !message.trim()) {
    return jsonResponse(400, { error: "Field 'message' is required." });
  }

  // Build the messages array: prior turns + the new user message.
  // Only keep clean {role, content} pairs and cap the length to control cost.
  const priorTurns = Array.isArray(history) ? history : [];
  const messages = priorTurns
    .filter(
      (m) =>
        m &&
        (m.role === "user" || m.role === "assistant") &&
        typeof m.content === "string"
    )
    .slice(-HISTORY_MAX)
    .map((m) => ({ role: m.role, content: m.content }));

  messages.push({ role: "user", content: message });

  const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

  // Stream the model's reply back to the browser as Server-Sent Events.
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    async start(controller) {
      try {
        const anthropicStream = client.messages.stream({
          model: MODEL,
          max_tokens: MAX_TOKENS,
          system: SYSTEM_PROMPT,
          messages,
        });

        for await (const event of anthropicStream) {
          if (
            event.type === "content_block_delta" &&
            event.delta.type === "text_delta"
          ) {
            const chunk = JSON.stringify({
              type: "content_block_delta",
              delta: { text: event.delta.text },
            });
            controller.enqueue(encoder.encode(`data: ${chunk}\n\n`));
          }
        }

        controller.enqueue(encoder.encode("data: [DONE]\n\n"));
      } catch (err) {
        const chunk = JSON.stringify({
          type: "error",
          error: err?.message || "Upstream error",
        });
        controller.enqueue(encoder.encode(`data: ${chunk}\n\n`));
      } finally {
        controller.close();
      }
    },
  });

  return new Response(stream, {
    status: 200,
    headers: {
      ...CORS_HEADERS,
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    },
  });
};
