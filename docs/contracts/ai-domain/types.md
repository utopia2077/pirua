# AI DTO contract

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Completeness basis

The inventory covers all 335 pinned `packages/ai` paths, the 699 unique public
export symbols over 123 subpaths, all Agent imports of `pi-ai`, and the Harness
source-type provenance block. Primary sources are `packages/ai/src/types.ts`,
`auth/types.ts`, `utils/diagnostics.ts`, and the pinned export manifests. API-
specific wire/options stay in `pi-ai-providers`; runtime traits/streams/stores
land in S03.

## Module ownership

| Module | S02 owns |
|---|---|
| `id` | open API/provider/model/image/tool identity newtypes and known constants |
| `json` | JSON aliases, object-only wrapper, `Presence`, safe numeric wrappers, `LiteralTrue` |
| `content` | text/thinking/image/tool-call blocks and text-signature DTO |
| `message` | user/assistant/tool-result/custom messages, stop reason, deferred handle |
| `usage` | usage/cost DTOs |
| `tool` | tool schema and constrained-sampling DTOs, not execution callbacks |
| `context` | provider-neutral message context |
| `event` | complete assistant-stream event union, not stream behavior |
| `model` | model/image-model/compat/routing/store-entry schemas, not catalog values |
| `options` | serializable scalar/JSON option DTOs and transient/durable split markers |
| `auth` | credential/auth-result/prompt/event/error-code DTOs, not callback/store behavior |
| `diagnostic` | diagnostic error and assistant diagnostic DTOs |

## Open identities

`ApiId`, `ProviderId`, `ImagesApiId`, `ImagesProviderId`, `ModelId`, tool names,
tool-call ids, diagnostic types, and raw stop reasons accept unknown non-empty
strings. Known API constants are:

```text
openai-completions
mistral-conversations
openai-responses
azure-openai-responses
openai-codex-responses
anthropic-messages
bedrock-converse-stream
google-generative-ai
google-vertex
pi-messages
```

Provider constants come from the provider manifest's 40 text registrations;
`faux` is a public test provider but not a closed `KnownProvider`. Preserve the
different serialized keys `id`, `model`, and `modelId` even when all use
`ModelId` in Rust.

## Content, tools, and messages

All DTO structs use camelCase serde field names and deny unknown fields unless
explicitly noted.

| Type | Exact fields |
|---|---|
| `TextSignatureV1` | `v:1`, `id`, optional `phase:"commentary"|"final_answer"` |
| `TextContent` | `type:"text"`, `text`, optional `textSignature` string |
| `ThinkingContent` | `type:"thinking"`, `thinking`, optional `thinkingSignature`, optional `redacted` |
| `ImageContent` | `type:"image"`, base64-convention `data`, `mimeType` |
| `ToolCall` | `type:"toolCall"`, `id`, `name`, object-only `arguments`, optional `thoughtSignature`, optional `namespace` |
| `UserMessage` | `role:"user"`, string or text/image-array `content`, `timestamp` |
| `AssistantMessage` | `role:"assistant"`, content blocks, `api`, `provider`, `model`, optional `responseModel`, `responseId`, `diagnostics`, required `usage`, `stopReason`, optional `deferred`, `errorMessage`, `rawStopReason`, `endTurn`, `timestamp` |
| `ToolResultMessage<D>` | `role:"toolResult"`, `toolCallId`, `toolName`, text/image content, presence-aware `details`, optional `usage`, `addedToolNames`, required `isError`, `timestamp` |
| `CustomMessage` | registered non-built-in `role` plus detached JSON object/value; no extra `custom` wire tag |
| `Context` | optional `systemPrompt`, `messages`, optional tools |
| `DeferredHandle` | `provider`, `modelId`, `api`, `id`, optional `expiresAt`, `pollAfterMs`, presence-aware `data` |

Built-in `Message` is closed over user/assistant/tool-result. Agent adds custom
roles through its own registry-aware envelope so custom data cannot shadow a
built-in role during deserialization.

`StopReason` is closed:

```text
pending | stop | length | toolUse | error | aborted | deferred
```

`pending` is never terminal.

For a terminal event, `reason` and the nested assistant `stopReason` must be the
same mapped value. Error and aborted terminals also require `errorMessage`.
Serde validates these relations in both directions so programmatically-created
contradictions cannot cross the wire.

`Tool<S=JsonSchema>` fields are `name`, `description`, `parameters`, and optional
`constrainedSampling`. The latter is either literal `false`,
`{type:"json_schema",strict:"prefer"|"require"}`, or
`{type:"grammar",variants:{openai_lark?,openai_regex?}}`. TypeBox functions and
symbols are not wire values.

## Assistant event union

One enum is internally tagged by `type`:

| Variant | Fields beyond `type` |
|---|---|
| `start` | `partial` |
| `text_start` | `contentIndex`, `partial` |
| `text_delta` | `contentIndex`, `delta`, `partial` |
| `text_end` | `contentIndex`, `content`, `partial` |
| `thinking_start` | `contentIndex`, `partial` |
| `thinking_delta` | `contentIndex`, `delta`, `partial` |
| `thinking_end` | `contentIndex`, `content`, `partial` |
| `toolcall_start` | `contentIndex`, `partial` |
| `toolcall_delta` | `contentIndex`, `delta`, `partial` |
| `toolcall_end` | `contentIndex`, `toolCall`, `partial` |
| `done` | reason `stop|length|toolUse|deferred`, final `message` |
| `error` | reason `aborted|error`, final error assistant message |

Content indices are `SafeUnsigned`. Unknown event/discriminant strings fail.
`PiMessagesEvent` is adapter-specific and waits for S04.

## Usage and costs

`Usage` has required `input`, `output`, `cacheRead`, `cacheWrite`, `totalTokens`,
optional `cacheWrite1h` and `reasoning`, and required cost fields `input`,
`output`, `cacheRead`, `cacheWrite`, `total`. Counts are `SafeUnsigned`; costs
are `FiniteF64`. Do not recompute or enforce comment-only subset relationships
on decode.

`ModelCostRates` has `input/output/cacheRead/cacheWrite`; `ModelCostTier` adds
`inputTokensAbove`; `ModelCost` adds optional tiers.

## Model schemas

Closed enums:

```text
ModelInput: text | image
ThinkingLevel: minimal | low | medium | high | xhigh | max
ModelThinkingLevel: off + ThinkingLevel
```

`ThinkingLevelMap` is a partial map whose present values are string or explicit
null. `Model` requires `id,name,api,provider,baseUrl,reasoning,input,cost,
contextWindow,maxTokens`; optional fields are `thinkingLevelMap`,
`samplingParams`, `headers`, and `compat`. Upstream model headers are merged into
resolved authentication and are therefore credential-capable. Rust represents
raw model-header intake as transient, keys-only-debug `RawModelHeaders`; exact
and ASCII-case-insensitive duplicate names are rejected. `ModelHeaderPolicy` is
a pure declarative ASCII-case-insensitive allowlist whose default is empty. A
storable `Model.headers` is immutable, serialize-only `PublicModelHeaders`
produced only by consuming raw headers plus that policy; it has no unchecked
map constructor or ordinary `Deserialize`. Ordinary `Model`, `ImagesModel`, and
`ModelsStoreEntry` deserialization applies the empty policy: absent and present-
empty headers succeed, while nonempty maps fail. `RawModel`, `RawImagesModel`,
and `RawModelsStoreEntry` own all policy-aware external and store restore paths,
preserving the upstream JSON map shape after successful conversion. Core
hard-rejects the case-insensitive names `authorization`,
`proxy-authorization`, `cookie`, `set-cookie`, `api-key`, `x-api-key`, and
`x-goog-api-key` regardless of host policy. A secret sentinel is tested under a
hard-denied or non-allowlisted name; core does not apply substring heuristics to
arbitrary values. If a later policy rejects headers in a cached entry, restore
fails closed before publication. Header-free models keep normal serde decoding.
`ImagesModel` uses the same rule and otherwise omits reasoning/context/max/
compat while adding required output kinds.

`baseUrl` is public model endpoint metadata except for one pinned empty sentinel.
Raw text-model intake maps nonempty strings to `PublicModelBaseUrl`; the exact
empty string maps to `ModelBaseUrl::RuntimeResolved` only for
`azure-openai-responses` and serializes back to `""`. Empty URLs for other text
APIs and every image API fail. Validation is structural: reject literal `?` or
`#`; after the first `//`, reject `@` before the next `/`, `?`, or `#`. Do not
parse or normalize, and do not require a scheme, host, port, or particular path.
Accepted bytes remain exact. Credential-specific endpoint overrides belong only
to transient `ModelAuth.baseUrl`; S03 converts the effective endpoint into a
generic non-Serde, fully redacted `ResolvedRequest<M>` used by text, deferred,
cancel, and image adapter paths rather than mutating a model DTO. Debug for
`PublicModelBaseUrl`, all raw/validated text/image/store models, `ModelAuth`,
containing `AuthResult`, and validation errors exposes no URL value.

Compatibility structs preserve all pinned names. In particular:

- OpenAI Completions: store/developer/reasoning/usage/finish/tool-result flags,
  max-token field, thinking format, chat-template maps/args, OpenRouter/Vercel
  routing, strict/grammar/cache/session-affinity/deferred flags.
- OpenAI Responses: developer/session/cache/strict/grammar/additional-tool/tool-
  search/explicit-cache flags.
- Anthropic: eager input, long cache, affinity, tool cache, temperature,
  adaptive thinking, empty signature, strict tools, fallback models, references.
- Bedrock: strict-mode flag.

Do not deserialize `compat` as an ambiguous untagged enum. Validate/select its
shape using the enclosing open `api` identity, while preserving its untagged
JSON object representation. Both deserialization and serialization perform the
validation; callers cannot skip it by omitting a later helper call.

OpenRouter routing keeps its snake_case keys; Vercel routing keeps `only` and
`order`. `ModelsStoreEntry` has `models`, optional `lastModified`, `checkedAt`,
and `etag`.

## Serializable options

Closed enums:

```text
Transport: sse | websocket | websocket-cached | auto
CacheRetention: none | short | long
ToolChoice: auto | none
DeferredWindow: 15m | 1h | 24h
```

Wire stream values include temperature, sampling params, max tokens, transport,
cache retention, session id, WebSocket connect timeout, metadata, tool choice,
reasoning, deferred bool/window, and thinking budgets. Runtime-only fields are
cancellation, telemetry context, fetch/HTTP transport, middleware, and payload/
response callbacks. `apiKey`, environment, bearer tokens, and secret headers are
transient auth/config.

Raw `ProviderHeaders` is an exact `map<string,string|null>` DTO; explicit null
suppresses a default header. It is not durable. Harness uses the same JSON map
shape only through its host-policy-classified public wrapper.

Provider-specific option unions and SDK-shaped tool choices remain in adapter
modules and are not S02 core types.

## Authentication DTOs

| Type | Exact fields |
|---|---|
| `ModelAuth` | optional secret `apiKey`, optional raw headers, optional `baseUrl`; transient |
| `ApiKeyCredential` | `type:"api_key"`, optional key/env |
| `OAuthCredentials` | required refresh/access/expiry plus flattened provider-specific JSON |
| `OAuthCredential` | OAuth fields plus `type:"oauth"` |
| `Credential` | closed tagged API-key/OAuth union |
| `CredentialInfo` | `providerId`, closed auth type |
| `AuthResult` | auth, optional env/source |
| `AuthCheck` | optional source and auth type |
| `AuthInfoLink` | url, optional label |
| `AuthPrompt` | text/secret/manual-code with message/placeholder or select with option ids/labels/descriptions; cancellation handle is runtime-only |
| `AuthEvent` | info, auth-url, device-code, or progress exact tagged variants |

Credential/config DTOs reject unknown fields except OAuth's explicit flattened
extras. No debug/serde output may expose a credential value in telemetry/errors.

## Diagnostics and errors

`DiagnosticErrorInfo` contains optional name/stack, required message, and
optional string-or-number code. `AssistantMessageDiagnostic` contains open type,
timestamp, optional error and object-only details. `ModelsErrorCode` is closed:

```text
model_source | model_validation | provider | stream | auth | oauth
```

`ModelsError` itself is a Rust runtime error, not a serialized JS `Error` object.

## Serde and validation obligations

- Optional normally means omitted; preserve present false/zero/empty values.
- Optional non-null fields reject explicit null rather than collapsing it into
  omission. The helper detects null before deserializing the inner type, so an
  optional inner `JsonValue` cannot accidentally admit null.
- Use `Presence` for optional nullable JSON/details and explicit-null maps.
- Deny unknown fields on closed types; explicit open maps are the only exception.
- Reject duplicate keys in open JSON/header/environment maps.
- Use deterministic `BTreeMap` for durable/wire records.
- Use safe numeric wrappers and reject out-of-range/non-finite input.
- Custom open ids reject empty but accept future values.
- A nonempty whitespace-only id is preserved verbatim; do not trim open ids.
- Do not derive serde for callbacks, streams, provider/store traits, abort
  handles, or runtime services.
- Golden fixtures cover every content/message/event/credential variant, required
  camelCase key, explicit null, unknown closed discriminant/field, safe numeric
  boundary, and open-id round trip.

The only optional fields with missing/null/value `Presence` semantics are
`DeferredHandle.data`, `ToolResultMessage.details`, and the object form of
`OpenRouterRouting.sort.partition`. Null also remains valid as a value inside
`ThinkingLevelMap`, raw `ProviderHeaders`, chat-template maps, OAuth extras, and
general nested JSON. Every other optional DTO field is non-null and rejects an
explicit null container.

## Explicit S02 exclusions

Do not copy generated `MODELS`, `IMAGE_MODELS`, provider `*.models.ts` values,
`BuiltinProvider`, timestamps, costs, or model ids. Do not implement streams,
providers, Models/credential stores, OAuth flows, transforms, retries, faux
behavior, or adapter-specific options. Those belong to separate runtime/provider
contracts. Catalog schema types do not establish value parity; the dated capture
proves captured values while absent historical inputs remain provenance-blocked.
