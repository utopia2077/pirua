# AI export inventory dispositions

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Decision

S03 closes the complete pinned packages/ai export manifest, not a deduplicated
list of Rust names. The manifest contains 699 unique occurrence keys formed by
subpath plus symbol name across 123 package subpaths:

| Subpath group | Occurrences |
|---|---:|
| root | 200 |
| compat | 260 |
| all other subpaths | 239 |
| Total | 699 |

The final disposition totals are:

| Disposition | Occurrences |
|---|---:|
| s02-dto | 146 |
| implemented | 123 |
| rust-nonapplicable | 22 |
| mapped-s04 | 163 |
| mapped-s05 | 239 |
| external-schema-projection | 6 |
| Total | 699 |

An occurrence is never removed because another subpath exports the same name.
Root, compat, and specific API/provider subpaths are separate compatibility
claims pointing to one implementation where appropriate.

## Rule priority

The checked generator applies explicit rules in this order. There is no
catch-all disposition.

1. the six exact external TypeBox occurrences;
2. every symbol whose resolved surface is ai.api-adapters;
3. the five S05 surfaces plus four retry-helper occurrences;
4. the 73 exact S02 DTO names on root and compat;
5. the 11 exact Rust-nonapplicable names on root and compat;
6. the 40 already implemented S03 names on root and compat;
7. the seven final model-helper names on root and compat;
8. the 29 final portable-domain occurrences with their exact origin/subpath.

Every occurrence must match exactly one rule. A zero-match or multi-match is a
hard error.

## External schema projections: 6

The exact keys are:

~~~text
.:Static
.:TSchema
.:Type
./compat:Static
./compat:TSchema
./compat:Type
~~~

Rust does not re-export TypeBox runtime identity. The S02 JSON/Serde DTOs and
S03 serialized JSON Schema validator are the replacement boundary.

## Mapped to S04: 163

Every occurrence whose selector-resolved surface is ai.api-adapters maps to
pi-ai-providers adapter implementations and pi-ai-transport utilities. The
inventory evidence names the S04 provider-trace suite and exact future owner;
it does not claim these symbols are already implemented.

The manifest generator must propagate origin line/declaration information and
resolve selector partitions. Origin path alone is insufficient because
utils/retry.ts is split across S03 and S05.

## Mapped to S05: 239

The path-resolved S05 surfaces are:

| Surface | Occurrences | Owner |
|---|---:|---|
| ai.catalog.projection | 39 | pi-ai catalog |
| ai.compat | 52 | pi-ai facade |
| ai.faux | 42 | pi-ai testkit |
| ai.oauth-images | 26 | pi-ai-providers OAuth/images |
| ai.provider-factories | 76 | pi-ai-providers registry |
| Subtotal | 235 | |

The remaining four occurrences are root and compat exports of RetryCallbacks
and retryAssistantCall. They map to the stateless S05 SDK compatibility helper.
RetryPolicy remains an S02 DTO, while isRetryableAssistantError is implemented
in S03. The retry selector, not whole-file routing, owns this split.

## S02 DTO projections: 146

Each of these 73 canonical names occurs once at root and once at compat:

~~~text
AnthropicAllowedFallbackModel
AnthropicMessagesCompat
Api
ApiKeyCredential
AssistantImages
AssistantMessage
AssistantMessageDiagnostic
AssistantMessageEvent
AuthCheck
AuthEvent
AuthInfoLink
AuthPrompt
AuthResult
AuthType
BedrockCompat
CacheRetention
ChatTemplateKwargValue
ConstrainedSamplingConfig
Context
Credential
CredentialInfo
DeferredHandle
DiagnosticErrorInfo
GrammarFormat
GrammarVariants
ImageContent
ImagesApi
ImagesContext
ImagesInputContent
ImagesModel
ImagesOutputContent
ImagesProviderId
ImagesStopReason
JsonValue
KnownApi
KnownImagesApi
KnownImagesProvider
KnownProvider
Message
Model
ModelAuth
ModelCost
ModelCostRates
ModelCostTier
ModelThinkingLevel
ModelsErrorCode
ModelsStoreEntry
OAuthCredential
OAuthCredentials
OpenAICompletionsCompat
OpenAIResponsesCompat
OpenRouterRouting
ProviderEnv
ProviderHeaders
ProviderId
RetryPolicy
SessionAffinityFormat
StopReason
TextContent
TextSignatureV1
ThinkingBudgets
ThinkingContent
ThinkingLevel
ThinkingLevelMap
ThinkingTokenBudgetField
Tool
ToolCall
ToolChoice
ToolResultMessage
Transport
Usage
UserMessage
VercelGatewayRouting
~~~

The target may be a semantic projection rather than the same Rust spelling,
for example Api to ApiId, KnownApi to ApiId constants, and ImagesInputContent
to InputContent. Each row names its concrete target module and S02
schema/golden evidence.

## Rust-nonapplicable projections: 22

Each of these 11 canonical names occurs once at root and once at compat:

~~~text
ApiOptionsMap
ApiStreamOptions
FetchFunction
ModelsRequestTransforms
ProviderResponse
ProviderStreamOptions
StringEnum
appendAssistantMessageDiagnostic
createAssistantMessageDiagnostic
extractDiagnosticError
formatThrownValue
~~~

ApiOptionsMap, ApiStreamOptions, and ProviderStreamOptions are TypeScript
conditional/intersection typing, not runtime contracts.

FetchFunction, ProviderResponse, and ModelsRequestTransforms exist for
JavaScript per-request HTTP/callback injection. S04 construction-time
transport/middleware capabilities replace them.

StringEnum is a TypeBox runtime schema constructor. Rust uses static types plus
serialized JSON Schema.

The four diagnostic helpers stringify arbitrary thrown values, copy message,
name, stack, and code, read ambient time, and mutate message diagnostics.
The contract exposes no general converter. Trusted callers
classify and sanitize values, use injected time, construct the existing DTO,
and append through ordinary Vec operations. Every nonapplicable row contains a
reason, replacement, decision reference, and revisit trigger.

## Already implemented S03 occurrences: 80

These 40 canonical names each occur at root and compat.

Stream:

~~~text
AssistantMessageEventStream
EventStream
createAssistantMessageEventStream
~~~

Auth and credentials:

~~~text
ApiKeyAuth
AuthContext
AuthInteraction
AuthOperationOptions
CredentialStore
InMemoryCredentialStore
OAuthAuth
ProviderAuth
ProviderAuthInteraction
~~~

Runtime options and adapter ports:

~~~text
DeferredCancelOptions
DeferredFetchOptions
ImagesFunction
ImagesOptions
ProviderImages
ProviderImagesOptions
ProviderRequestOptions
ProviderStreams
SimpleStreamOptions
StreamFunction
StreamOptions
~~~

Model store:

~~~text
InMemoryModelsStore
ModelsStore
ModelsStoreOperationOptions
~~~

Registry:

~~~text
CreateModelsOptions
Models
ModelsApiStreamOptions
ModelsDeferredCancelOptions
ModelsDeferredFetchOptions
ModelsError
ModelsPublication
ModelsRefreshOptions
ModelsRefreshResult
ModelsSimpleStreamOptions
MutableModels
Provider
RefreshModelsContext
createModels
~~~

Each row names its concrete pi-ai-core target and an accepted stream,
auth/store, or registry test id. A wildcard facade export alone is not
implementation evidence.

## Final S03 public occurrences: 43

Seven model-helper names occur at root and compat, producing 14 occurrences:

~~~text
CreateProviderOptions
createProvider
calculateCost
clampThinkingLevel
getSupportedThinkingLevels
hasApi
modelsAreEqual
~~~

The remaining 29 exact occurrences are:

| Origin | Canonical names | Occurrences | Rust projection |
|---|---|---:|---|
| api/transform-messages.ts | transformMessages | 1 | strict core transform plus pi-ai lax decoder |
| session-resources.ts | SessionResourceCleanup, registerSessionResourceCleanup, cleanupSessionResources | 6 | SessionResource trait and registry methods |
| utils/json-parse.ts | repairJson, parseJsonWithRepair, parseStreamingJson | 6 | JSON helper functions |
| utils/overflow.ts | getOverflowPatterns, isContextOverflow, isRecoverableLength | 6 | opaque pattern vector and classifier functions |
| utils/text.ts | contentText | 2 | content-text functions |
| utils/uuid.ts | uuidv7 | 2 | explicit UuidV7Generator generate method |
| utils/validation.ts | validateToolArguments, validateToolCall | 4 | validation functions |
| utils/retry.ts | isRetryableAssistantError | 2 | retry classifier |
| Total | | 29 | |

The upstream callable spelling may map to a safer Rust method or capability.
The row remains implemented only when an exact facade compile probe and causal
behavior id name that target.

## Non-package-public S03 module exports

These eight upstream module exports do not increase the 699 package occurrence
count but remain required supporting implementations:

~~~text
splitDeferredTools
ContextUsageEstimate
calculateContextTokens
estimateTextTokens
estimateTextAndImageContentTokens
estimateMessageTokens
estimateContextTokens
shortHash
~~~

Rust-only normalizer traits, lax decoder, UUID ports, validation report types,
and resource handles are supporting contracts. They are not invented upstream
export occurrences.

## Generated artifacts

The source mapping is:

~~~text
conformance/ai-domain/export-mapping.toml
~~~

Each rule records:

- stable rule id;
- exact name/subpath/origin/surface selector;
- disposition and owner slice;
- concrete Rust targets;
- evidence fixture or compile probe;
- expected occurrence count;
- reason, replacement, decision reference, and revisit trigger when
  nonapplicable.

The generated output is:

~~~text
conformance/ai-domain/inventory.json
~~~

It records schema version, pinned source commit, source-manifest digest, mapping
digest, expected count, disposition totals, and every occurrence with:

- key, subpath, name, kind, origin, origin line/declaration;
- selector-resolved surface;
- disposition, Rust target, owner, and evidence.

The shared upstream export generator must add origin line and declaration to
both AI and Agent symbol records. Both export manifests must retain that source
identity. Selector resolution then reuses the map's line-set
machinery. A special case keyed only by retry symbol name is forbidden because
future selector partitions would drift silently.

## Checked invariants

Generation/check mode rejects:

- an occurrence set different from the pinned manifest;
- any count other than 699 or the frozen disposition totals;
- missing root or compat duplicates;
- duplicate-name collapse;
- zero-match or multiple-match rules;
- a selector ownership count other than 195 ai.domain, 96 ai.models-auth, 163
  ai.api-adapters, four ai.retry-compat, 235 S05 surface rows, and six external
  projections;
- an implemented row without a concrete target plus live compile/behavior
  evidence;
- an S02 row without schema evidence;
- an S04/S05 row that claims present implementation;
- a nonapplicable row without reason/replacement/decision/revisit fields;
- an unknown missing origin unless it is one of the six exact TypeBox rows;
- stale source commit, source digest, mapping digest, or generated output.

Causal tests delete one root row, one compat row, and one evidence id; collapse
by name; assign isRetryableAssistantError to S05; create zero/double rule
matches; mutate source digests; claim an absent stub; and add a synthetic
upstream export. Each mutation must fail for its intended reason.
