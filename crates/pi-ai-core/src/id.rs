//! Validated open string identities and known upstream constants.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// Error returned when an open identity is empty or otherwise reserved.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdError {
    message: &'static str,
}

impl IdError {
    const fn empty() -> Self {
        Self {
            message: "identity must be non-empty",
        }
    }

    const fn built_in_role() -> Self {
        Self {
            message: "custom role cannot shadow a built-in role",
        }
    }
}

impl fmt::Display for IdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for IdError {}

macro_rules! open_id {
    ($(#[$meta:meta])* $name:ident, $description:literal) => {
        $(#[$meta])*
        #[doc = $description]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Creates an identity after validating that it is not empty.
            pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
                let value = value.into();
                if value.is_empty() {
                    Err(IdError::empty())
                } else {
                    Ok(Self(value))
                }
            }

            /// Returns the wire string.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes the identity and returns the wire string.
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
            }
        }
    };
}

open_id!(ApiId, "An open provider API protocol identity.");
open_id!(ProviderId, "An open text-model provider identity.");
open_id!(ImagesApiId, "An open image-generation API identity.");
open_id!(
    ImagesProviderId,
    "An open image-generation provider identity."
);
open_id!(ModelId, "An open provider-scoped model identity.");
open_id!(ToolName, "An open tool name.");
open_id!(ToolCallId, "An open tool-call identity.");
open_id!(DiagnosticType, "An open diagnostic category.");
open_id!(RawStopReason, "An open provider-native stop reason.");
open_id!(
    DeferredId,
    "An open provider token for a deferred response."
);
open_id!(ResponseId, "An open provider response identity.");

/// An application-defined message role that cannot shadow a built-in role.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CustomRole(String);

impl CustomRole {
    /// Creates a validated custom role.
    pub fn new(value: impl Into<String>) -> Result<Self, IdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(IdError::empty());
        }
        if matches!(value.as_str(), "user" | "assistant" | "toolResult") {
            return Err(IdError::built_in_role());
        }
        Ok(Self(value))
    }

    /// Returns the wire role string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for CustomRole {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for CustomRole {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

impl ApiId {
    /// Handwritten known API identities at the pinned upstream revision.
    pub const KNOWN: &[&str] = &[
        Self::OPENAI_COMPLETIONS,
        Self::MISTRAL_CONVERSATIONS,
        Self::OPENAI_RESPONSES,
        Self::AZURE_OPENAI_RESPONSES,
        Self::OPENAI_CODEX_RESPONSES,
        Self::ANTHROPIC_MESSAGES,
        Self::BEDROCK_CONVERSE_STREAM,
        Self::GOOGLE_GENERATIVE_AI,
        Self::GOOGLE_VERTEX,
        Self::PI_MESSAGES,
    ];
    /// OpenAI-compatible Chat Completions API.
    pub const OPENAI_COMPLETIONS: &str = "openai-completions";
    /// Mistral Conversations API.
    pub const MISTRAL_CONVERSATIONS: &str = "mistral-conversations";
    /// OpenAI Responses API.
    pub const OPENAI_RESPONSES: &str = "openai-responses";
    /// Azure OpenAI Responses API.
    pub const AZURE_OPENAI_RESPONSES: &str = "azure-openai-responses";
    /// OpenAI Codex Responses API.
    pub const OPENAI_CODEX_RESPONSES: &str = "openai-codex-responses";
    /// Anthropic Messages API.
    pub const ANTHROPIC_MESSAGES: &str = "anthropic-messages";
    /// Amazon Bedrock Converse Stream API.
    pub const BEDROCK_CONVERSE_STREAM: &str = "bedrock-converse-stream";
    /// Google Generative AI API.
    pub const GOOGLE_GENERATIVE_AI: &str = "google-generative-ai";
    /// Google Vertex API.
    pub const GOOGLE_VERTEX: &str = "google-vertex";
    /// Native pi-messages API.
    pub const PI_MESSAGES: &str = "pi-messages";
}

impl ImagesApiId {
    /// Handwritten known image API identities at the pinned upstream revision.
    pub const KNOWN: &[&str] = &[Self::OPENROUTER_IMAGES];
    /// OpenRouter image-generation API.
    pub const OPENROUTER_IMAGES: &str = "openrouter-images";
}

impl ImagesProviderId {
    /// Handwritten known image-provider identities at the pinned upstream revision.
    pub const KNOWN: &[&str] = &[Self::OPENROUTER];
    /// OpenRouter image provider.
    pub const OPENROUTER: &str = "openrouter";
}

impl ProviderId {
    /// Handwritten known text-provider identities at the pinned upstream revision.
    pub const KNOWN: &[&str] = &[
        Self::AMAZON_BEDROCK,
        Self::ANT_LING,
        Self::ANTHROPIC,
        Self::GOOGLE,
        Self::GOOGLE_VERTEX,
        Self::OPENAI,
        Self::AZURE_OPENAI_RESPONSES,
        Self::OPENAI_CODEX,
        Self::RADIUS,
        Self::NVIDIA,
        Self::DEEPSEEK,
        Self::GITHUB_COPILOT,
        Self::XAI,
        Self::GROQ,
        Self::CEREBRAS,
        Self::OPENROUTER,
        Self::VERCEL_AI_GATEWAY,
        Self::ZAI,
        Self::ZAI_CODING_CN,
        Self::MISTRAL,
        Self::MINIMAX,
        Self::MINIMAX_CN,
        Self::MOONSHOTAI,
        Self::MOONSHOTAI_CN,
        Self::HUGGINGFACE,
        Self::FIREWORKS,
        Self::TOGETHER,
        Self::BASETEN,
        Self::OPENCODE,
        Self::OPENCODE_GO,
        Self::KIMI_CODING,
        Self::CLOUDFLARE_WORKERS_AI,
        Self::CLOUDFLARE_AI_GATEWAY,
        Self::QWEN_TOKEN_PLAN,
        Self::QWEN_TOKEN_PLAN_CN,
        Self::QWEN_TOKEN_PLAN_INDIVIDUAL,
        Self::XIAOMI,
        Self::XIAOMI_TOKEN_PLAN_CN,
        Self::XIAOMI_TOKEN_PLAN_AMS,
        Self::XIAOMI_TOKEN_PLAN_SGP,
    ];
    /// Amazon Bedrock.
    pub const AMAZON_BEDROCK: &str = "amazon-bedrock";
    /// Ant Ling.
    pub const ANT_LING: &str = "ant-ling";
    /// Anthropic.
    pub const ANTHROPIC: &str = "anthropic";
    /// Google Generative AI.
    pub const GOOGLE: &str = "google";
    /// Google Vertex.
    pub const GOOGLE_VERTEX: &str = "google-vertex";
    /// OpenAI.
    pub const OPENAI: &str = "openai";
    /// Azure OpenAI Responses.
    pub const AZURE_OPENAI_RESPONSES: &str = "azure-openai-responses";
    /// OpenAI Codex.
    pub const OPENAI_CODEX: &str = "openai-codex";
    /// Radius.
    pub const RADIUS: &str = "radius";
    /// NVIDIA.
    pub const NVIDIA: &str = "nvidia";
    /// DeepSeek.
    pub const DEEPSEEK: &str = "deepseek";
    /// GitHub Copilot.
    pub const GITHUB_COPILOT: &str = "github-copilot";
    /// xAI.
    pub const XAI: &str = "xai";
    /// Groq.
    pub const GROQ: &str = "groq";
    /// Cerebras.
    pub const CEREBRAS: &str = "cerebras";
    /// OpenRouter.
    pub const OPENROUTER: &str = "openrouter";
    /// Vercel AI Gateway.
    pub const VERCEL_AI_GATEWAY: &str = "vercel-ai-gateway";
    /// Z.AI.
    pub const ZAI: &str = "zai";
    /// Z.AI Coding China.
    pub const ZAI_CODING_CN: &str = "zai-coding-cn";
    /// Mistral.
    pub const MISTRAL: &str = "mistral";
    /// MiniMax.
    pub const MINIMAX: &str = "minimax";
    /// MiniMax China.
    pub const MINIMAX_CN: &str = "minimax-cn";
    /// Moonshot AI.
    pub const MOONSHOTAI: &str = "moonshotai";
    /// Moonshot AI China.
    pub const MOONSHOTAI_CN: &str = "moonshotai-cn";
    /// Hugging Face.
    pub const HUGGINGFACE: &str = "huggingface";
    /// Fireworks.
    pub const FIREWORKS: &str = "fireworks";
    /// Together.
    pub const TOGETHER: &str = "together";
    /// Baseten.
    pub const BASETEN: &str = "baseten";
    /// OpenCode.
    pub const OPENCODE: &str = "opencode";
    /// OpenCode Go.
    pub const OPENCODE_GO: &str = "opencode-go";
    /// Kimi Coding.
    pub const KIMI_CODING: &str = "kimi-coding";
    /// Cloudflare Workers AI.
    pub const CLOUDFLARE_WORKERS_AI: &str = "cloudflare-workers-ai";
    /// Cloudflare AI Gateway.
    pub const CLOUDFLARE_AI_GATEWAY: &str = "cloudflare-ai-gateway";
    /// Qwen Token Plan.
    pub const QWEN_TOKEN_PLAN: &str = "qwen-token-plan";
    /// Qwen Token Plan China.
    pub const QWEN_TOKEN_PLAN_CN: &str = "qwen-token-plan-cn";
    /// Qwen Token Plan Individual.
    pub const QWEN_TOKEN_PLAN_INDIVIDUAL: &str = "qwen-token-plan-individual";
    /// Xiaomi.
    pub const XIAOMI: &str = "xiaomi";
    /// Xiaomi Token Plan China.
    pub const XIAOMI_TOKEN_PLAN_CN: &str = "xiaomi-token-plan-cn";
    /// Xiaomi Token Plan Americas.
    pub const XIAOMI_TOKEN_PLAN_AMS: &str = "xiaomi-token-plan-ams";
    /// Xiaomi Token Plan Singapore.
    pub const XIAOMI_TOKEN_PLAN_SGP: &str = "xiaomi-token-plan-sgp";
}

#[cfg(test)]
mod tests {
    use super::{ApiId, CustomRole};

    #[test]
    fn open_ids_accept_future_values_but_reject_empty_values() {
        assert_eq!(
            ApiId::new("future-api").map(|id| id.into_string()),
            Ok("future-api".to_owned())
        );
        assert!(ApiId::new("").is_err());
        assert_eq!(
            ApiId::new("   ").map(|id| id.into_string()),
            Ok("   ".to_owned())
        );
    }

    #[test]
    fn custom_roles_cannot_shadow_builtin_roles() {
        assert!(CustomRole::new("artifact").is_ok());
        assert!(CustomRole::new("user").is_err());
        assert!(CustomRole::new("assistant").is_err());
        assert!(CustomRole::new("toolResult").is_err());
    }
}
