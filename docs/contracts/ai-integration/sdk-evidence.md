# SDK transport evidence

These are pinned SDK/source facts. Execution coverage is defined by the
[transport corpus](../../../conformance/ai-transport/README.md).

| Package/version | Inspected source | Observation at pinned adapter boundary |
|---|---|---|
| openai 6.40.0 | `src/client.ts:446,456,942` | 600000 ms fetch-to-head timeout; SDK defaults two retries but adapter sets zero and shared helper owns configured retries |
| @anthropic-ai/sdk 0.91.1 | `src/client.ts:402,412,797`; `src/resources/messages/messages.ts:88` | streaming defaults 600000 ms fetch-to-head timeout; lower retries explicitly zero |
| @google/genai 1.52.0 | `dist/node/index.mjs:12926,13160,13305` | GenerateContent ApiClient has no default timeout or retries without retryOptions; pinned adapters don't set them and don't map neutral timeoutMs |
| @aws-sdk/client-bedrock-runtime 3.1048.0 | `dist-cjs/runtimeConfig.js:60` | SDK standard default three total attempts; AWS_MAX_ATTEMPTS/shared config may override; pinned adapter ignores neutral maxRetries/timeoutMs |

OpenAI/Anthropic clear their timer immediately after fetch resolves, before SSE
body consumption. Google retryOptions, when configured, default to five total
attempts; attempts=1 disables that owner. Do not use the separate bundled Google
NextGen client's one-minute default as GenerateContent evidence.

Bedrock uses NodeHttp2Handler by default; proxy/force-HTTP1 paths substitute
NodeHttpHandler. @smithy/node-http-handler 4.7.3 `dist-cjs/index.js:681,709,771`
has no default request/session timeout. A configured requestTimeout is inactivity
based. @smithy/core 3.24.3 `dist-cjs/submodules/retry/index.js:428,715` establishes
the three-attempt default and overrides. Native Rust must have one equivalent
attempt owner, not shared retry layered over another SDK retry loop.

## URL and payload observations

OpenAI `src/client.ts:1106` merges SDK default headers, auth, client defaults,
body headers and request headers. Platform/user-agent identity is a declared
fixture normalization. Azure `src/azure.ts:94,108,147,153` adds api-version,
uses API-key or bearer auth, and rewrites only a closed deployment-route set.
`/responses` is absent from that set: do not insert a deployment segment into
Azure Responses. The pinned pi adapter defaults apiVersion to v1 and supplies
an explicit normalized base URL. S04 owns the API module's option/environment
helpers and their configured-input precedence; S05 retains auth discovery and
provider/facade factory composition. This is not permission to read ambient
credentials directly from the pure core or to defer API helper exports to S05.

Google `dist/node/index.mjs:10304,15128` converts GenerateContentParameters before
POSTing to `{model}:streamGenerateContent?alt=sse`; `{model,contents,config}` is
not its wire body. Config converters at `:9959,10126` move systemInstruction,
tools, toolConfig, safetySettings and cachedContent to the body root, while
sampling and thinking fields populate generationConfig. Gemini and Vertex
converters differ. Model/endpoint routing is at `:2943,12947,13039`; API-key
headers and OAuth observation are at `:13327,19437`. Nested conversion requires the corresponding Google wire corpus.

Bedrock schema `dist-cjs/schemas/schemas_0.js:1477` specifies POST
`/model/{modelId}/converse-stream`. @smithy/signature-v4 5.4.3
`dist-cjs/index.js:220,235,245,495` supplies canonical-request/signing reference;
@smithy/core `dist-cjs/submodules/event-streams/index.js:234,264,511` supplies
binary framing, header types, and prelude/message CRC behavior. Source inspection
does not substitute for fixed-date synthetic signing vectors or decoder tests.

## Inspected file fingerprints

| Node-modules path | SHA-256 |
|---|---|
| `openai/src/client.ts` | `51865169dda5572cfc6f1849ec2e8edaf1fcf4724bcbec59c6925c9c163d761f` |
| `openai/src/azure.ts` | `b34ff71410fb2cf4976255c457072b8fc9854d72bc52af69a50a29b67b52a435` |
| `@anthropic-ai/sdk/src/client.ts` | `7836d3870ad4816d01f98c060f6068705381748c28742e75086c0c166634dc59` |
| `@google/genai/dist/node/index.mjs` | `748f86443c1c2c80e4ecf568d0c033ad9f1e9f34926c810e35c77e965a951c33` |
| `@aws-sdk/client-bedrock-runtime/dist-cjs/schemas/schemas_0.js` | `6acb87a23915762c46e483938aa46c18b40b1e97c58a44bbc753393eebb300c3` |

Node 26.8.1 embeds Undici 8.10.0, distinct from installed npm Undici 8.9.0.
Its `internal/deps/undici/undici` source SHA256 is
`9090d7cdbb8b47c2b040f337b05704b385d2ba06d3b79709691c5e8b13090f4d`.

## Fetch content obligations

- Add absent Accept-Encoding as `gzip, deflate` for HTTP and
  `br, gzip, deflate, zstd` for HTTPS; Range appends identity. Other finite
  request defaults must be checked at the actual network boundary.
- Decode gzip/x-gzip, zlib-or-raw deflate, Brotli and Zstandard in reverse coding
  order; more than five comma-separated codings reject during acquisition.
  An unknown coding disables the entire decoder chain. Header values remain.
- Skip decoding HEAD/CONNECT, null-body statuses and followed redirects.
  Gzip/deflate and Brotli use source-specific tolerant finish/flush settings;
  Zstandard uses its own end mode. Empty encoded streams have a special EOF path.

Actual loopback tests must cover compressed success/error bodies, partial output,
EOF/malformed input, cancellation and decoded resource bounds. Trusted bindings
returning preconstructed Responses require distinct treatment: redirect opt-out
alone is not authority to add network defaults or decode an already semantic
body. No hidden network fallback or arbitrary credential forwarding is allowed.

Google family source also does not call onResponse; generic adapter machinery
must not invent that callback. Vertex's ADC discovery belongs to the explicit
auth capability/factory boundary; exact S04 request/auth consumption versus S05
factory discovery retains its separate host capability boundary.

## Configured Bedrock evidence and stage constraints

Pinned wrapper blob is2e944fcd24613d2a549310508e109fccb510aee6. Installed
@aws-sdk/core httpAuthSchemes selects ambient bearer by key presence, maintains
clock-offset observations, and delegates signing per attempt. Installed
@smithy/signature-v4:495–528 consumes its credential provider inside signRequest;
the initial auth-stage identity observation alone does not prove a signing-time
refresh leaf is never called. The invocation cache/expiration boundary therefore
needs actual configured-leaf sequence tests. Smithy retry classification/quota
and adaptive policy remain implementation evidence obligations.
