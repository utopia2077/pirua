# Accepted scope

## Status

accepted

## Intent type

change

## Why now

`pi-mono` is defining a durable Harness v3 while its current harness source is
still an incomplete scaffold. Implementing the new runtime in Rust now provides
a chance to use Harness v3's total-state and Effects boundaries directly, rather
than translating code that the upstream specification already plans to delete.
The project owner also requires long-term upstream tracking and extensions that
can be implemented in languages other than JavaScript.

## Scope

- In:
  - 实现 `packages/ai` 的共享运行时，以及下述选定 provider 的完整接入；
    保留共享 API 适配器及其行为验证。
  - Implement the complete semantic surface of `packages/agent`, including
    legacy low-level Agent compatibility and the target Harness v3 runtime.
  - Port the headless runtime responsibilities from `packages/coding-agent`:
    model composition, coding tools, system-prompt/message adapters, legacy
    session import, and harness composition.
  - Support out-of-process, language-neutral tool, hook, provider, event, and
    host-action extensions.
  - Preserve an explicit mapping from upstream contracts, symbols, generated
    sources, and behavior tests to Rust owners and conformance evidence.
  - Use a 15-crate workspace while retaining finer upstream correspondence as
    internal Rust modules.
- Out:
  - 名单外的内置 provider 工厂、专属认证/OAuth、模型目录与产品注册；
    不再作为当前任务必须补齐的功能，也不因此删除已验收的共享 API 适配器。
  - 上游 `faux` provider 的公开兼容接口；离线验证所需的内部模拟 provider
    仍可使用，不增加产品 provider 范围。
  - A source-compatible reimplementation of every current JavaScript extension.
  - Porting the interactive TUI, CLI presentation, package manager, updater,
    clipboard, HTML export, and other user-interface infrastructure into the
    harness core.
  - Owning a privileged extension supervisor, transport authentication, OS
    sandbox, or credential broker inside the 15-crate workspace; protocol
    endpoints receive those trust facts from a host.
  - Exactly-once guarantees for provider or tool side effects.
  - Publishing a stable public crate or extension protocol before its explicit
    design decisions and conformance suite are accepted.
  - Blind line-by-line translation of the current unfinished harness scaffold.

### Selected provider scope

以下是项目所有者确认的唯一 provider 范围名单；后续计划、设计和覆盖清单引用
此名单。共 **6 个文本接入 + 1 个图像接入**。

| 类别 | 源码入口 | 产品 |
|---|---|---|
| text | `openai` | OpenAI API |
| text | `google` | Gemini 直接 API |
| text | `anthropic` | Claude / Anthropic |
| text | `deepseek` | DeepSeek |
| text | `openai-codex` | OpenAI Codex / ChatGPT 订阅接入 |
| text | `openrouter` | OpenRouter 文本接入 |
| images | `openrouter-images` | OpenRouter 图像接入 |

`openrouter-images` 是独立源码入口，其上游图像 provider ID 仍为 `openrouter`；
文本与图像命名空间分别核对。选择按接入入口生效，不额外按底层模型厂商过滤
OpenRouter 的模型集合。每个保留入口包含其所需的认证、选项映射、模型目录、
注册与端到端离线验证。其余 34 个内置文本入口全部排除，包括 Azure、Vertex、
Bedrock、Copilot 等其他接入方式。通用运行时、开放 ID 与扩展接口仍保持原契约。

保留完整上游身份清单；范围外内容必须记录为所有者明确排除，不能记成已实现或
Rust 不适用。覆盖率按选定范围统计，共享 wire/API 的历史来源身份保持完整。

## Affected modules / capabilities

### New capabilities

- `rust-ai-runtime`: Rust message, model, auth, stream, transport, provider,
  image, catalog, and compatibility implementation corresponding to
  `packages/ai`.
- `durable-harness-v3`: Total-state Rust implementation of Harness v3 storage,
  planning, effects, recovery, cancellation, compaction, navigation, and public
  lane surfaces.
- `language-neutral-extensions`: Versioned bidirectional protocol for tools,
  hooks, providers, passive events, cancellation, streaming, and host actions.
- `upstream-conformance`: Machine-readable ownership map and differential
  fixtures that classify and verify upstream changes.
- `coding-headless-runtime`: Rust composition of model runtime, coding tools,
  message/system-prompt adapters, and session lifecycle without UI concerns.

### Modified capabilities

- `agent-loop-compatibility`: Preserve current low-level Agent and agent-loop
  observable behavior while exposing Rust-native interfaces.
- `session-persistence`: Replace record-log recovery with Harness v3 entries,
  registers, usage ledger, total operation state, and migrate-on-open semantics.
- `provider-execution`: Replace provider SDK callbacks as the architectural
  boundary with Rust provider traits or extension-protocol capabilities.
- `builtin-provider-coverage`: 内置工厂、认证、目录、图像和注册装配收缩为
  Selected provider scope；不扩展其他 provider 的独立接入。
- `tool-execution`: Replace shared JavaScript function/object semantics with
  validated invocation, progress, cancellation, and settlement contracts.

### Removed capabilities

- `shared-javascript-object-contract`: Rust core and extensions will not share
  mutable JavaScript object identity, functions, prototypes, symbols, or cyclic
  values across the boundary.
- `record-log-recovery`: The new implementation will not reconstruct open
  operations by folding the current experimental `LaneRecord` history.

## Stakeholders / Approver

- Approver: project owner
- Affected teams: Rust harness maintainers, pi-mono upstream-sync maintainers,
  provider adapter maintainers, extension SDK maintainers, coding-agent host
  maintainers

## Success criteria

- The workspace contains no more than 15 production crates, a 42% reduction
  from the earlier 26-crate design, while every included upstream responsibility
  has exactly one Rust owner.
- Every Harness v3 Part 8 slice maps to a Rust crate/module and a conformance
  seam; unfinished upstream scaffold code is explicitly excluded from authority.
- Public AI and agent DTOs preserve upstream string discriminants, field names,
  error codes, ordering, cancellation, retry, and serialization semantics.
- Rust provider traces reproduce upstream request, stream-event, and terminal
  message behavior for the supported adapters without requiring the same SDKs.
- 选定的 6 个文本工厂与 1 个图像工厂通过对应离线行为与注册验证；
  名单外内容有明确范围排除记录，不进入产品内置列表或未完成工作分母。
- Memory, JSONL, and SQLite session implementations satisfy one shared backend
  conformance contract and Harness v3 crash/recovery invariants.
- At least Rust and TypeScript extension SDKs pass the same protocol fixtures;
  the protocol permits Python and Go implementations without an embedded
  JavaScript runtime.
- Upstream updates can be classified as contract, behavior, generated data,
  provider wire, host-only, compatibility-only, or disposable scaffold changes,
  with no unowned change category.
- Independent evidence validates each complete behavior contract; partial
  implementation cannot satisfy a complete coverage claim.

## Risks & open questions

- Harness v3 is still changing; spec/source conflicts must be preserved and
  resolved by slice authority rather than silently choosing the latest file.
- Exact public Rust naming remains provisional until the crate topology design
  is explicitly accepted for publication.
- Protocol provider v1 accepts no core-managed credential. A future signing or
  credential broker and an untrusted-process sandbox require security-owner
  decisions and a separately versioned profile.
- Reproducing TypeBox conversion requires behavior fixtures; generic JSON Schema
  validators do not have identical coercion semantics by default.
- Provider SDK upgrades can change retry, headers, streaming, and error
  normalization without changing high-level types.
- The pinned model-catalog projection is not reproducible from Git alone because
  upstream generator inputs include mutable external feeds. The checked-in
  dated capture and sanitized provenance snapshot support reproducible captured
  value parity, while the absent historical text inputs remain unrecoverable.

## Background

The upstream Harness v3 specification defines three durable stores—entries,
registers, and usage rows—and uses a complete `op.state` register as the durable
program counter. The current `packages/agent/src/harness/**` implementation uses
an older record-log model and is explicitly deletable by the Harness v3 build
order. In contrast, the existing `Agent` and low-level agent loop remain an
observable compatibility boundary.

`packages/ai` is already conceptually layered into domain types, the
`Models`/`Provider` runtime, shared API wire adapters, provider factories/auth,
and generated model data. The Rust layout preserves that layering while
consolidating ownership into fewer crates.

## Glossary

- **upstream**: the tracked `earendil-works/pi-mono` repository and revision.
- **contract mirror**: a Rust type or behavior whose observable semantics follow
  an upstream normative source.
- **projection**: generated code, public façade, or transport DTO derived from a
  canonical contract but not independently authoritative.
- **effect**: provider, tool, hook, timer, storage commit, or another operation
  that is not pure state computation.
- **extension generation**: one activated instance of an extension manifest and
  connection. An older generation may finish only an invocation whose exact
  live binding it still owns; a reply is stale after binding retirement,
  abandonment, duplicate terminal, or key mismatch, not merely because it is
  older.
