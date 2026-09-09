//! Generated from installed SDK3.1048.0 reachable ConverseStream schemas.
//! Source generator/receipt retained in the Bedrock conformance directory.
use super::sdk::Shape;
pub(super) fn shape(name: &str) -> Option<Shape> {
    Some(match name {
        "ConverseStreamRequest" => Shape::Object(
            &[
                ("modelId", "scalar_0"),
                ("messages", "Messages"),
                ("system", "SystemContentBlocks"),
                ("inferenceConfig", "InferenceConfiguration"),
                ("toolConfig", "ToolConfiguration"),
                ("guardrailConfig", "GuardrailStreamConfiguration"),
                ("additionalModelRequestFields", "scalar_15"),
                ("promptVariables", "PromptVariableMap"),
                ("additionalModelResponseFieldPaths", "scalar_64"),
                ("requestMetadata", "RequestMetadata"),
                ("performanceConfig", "PerformanceConfiguration"),
                ("serviceTier", "ServiceTier"),
                ("outputConfig", "OutputConfig"),
            ],
            false,
        ),
        "scalar_0" => Shape::Scalar,
        "Messages" => Shape::List("Message", false),
        "Message" => Shape::Object(&[("role", "scalar_0"), ("content", "ContentBlocks")], false),
        "ContentBlocks" => Shape::List("ContentBlock", false),
        "ContentBlock" => Shape::Object(
            &[
                ("text", "scalar_0"),
                ("image", "ImageBlock"),
                ("document", "DocumentBlock"),
                ("video", "VideoBlock"),
                ("audio", "AudioBlock"),
                ("toolUse", "ToolUseBlock"),
                ("toolResult", "ToolResultBlock"),
                ("guardContent", "GuardrailConverseContentBlock"),
                ("cachePoint", "CachePointBlock"),
                ("reasoningContent", "ReasoningContentBlock"),
                ("citationsContent", "CitationsContentBlock"),
                ("searchResult", "SearchResultBlock"),
            ],
            true,
        ),
        "ImageBlock" => Shape::Object(
            &[
                ("format", "scalar_0"),
                ("source", "ImageSource"),
                ("error", "ErrorBlock"),
            ],
            false,
        ),
        "ImageSource" => Shape::Object(
            &[("bytes", "scalar_21"), ("s3Location", "S3Location")],
            true,
        ),
        "scalar_21" => Shape::Blob,
        "S3Location" => Shape::Object(&[("uri", "scalar_0"), ("bucketOwner", "scalar_0")], false),
        "ErrorBlock" => Shape::Object(&[("message", "scalar_0")], false),
        "DocumentBlock" => Shape::Object(
            &[
                ("name", "scalar_0"),
                ("source", "DocumentSource"),
                ("format", "scalar_0"),
                ("context", "scalar_0"),
                ("citations", "CitationsConfig"),
            ],
            false,
        ),
        "DocumentSource" => Shape::Object(
            &[
                ("bytes", "scalar_21"),
                ("s3Location", "S3Location"),
                ("text", "scalar_0"),
                ("content", "DocumentContentBlocks"),
            ],
            true,
        ),
        "DocumentContentBlocks" => Shape::List("DocumentContentBlock", false),
        "DocumentContentBlock" => Shape::Object(&[("text", "scalar_0")], true),
        "CitationsConfig" => Shape::Object(&[("enabled", "scalar_2")], false),
        "scalar_2" => Shape::Scalar,
        "VideoBlock" => Shape::Object(&[("format", "scalar_0"), ("source", "VideoSource")], false),
        "VideoSource" => Shape::Object(
            &[("bytes", "scalar_21"), ("s3Location", "S3Location")],
            true,
        ),
        "AudioBlock" => Shape::Object(
            &[
                ("format", "scalar_0"),
                ("source", "AudioSource"),
                ("error", "ErrorBlock"),
            ],
            false,
        ),
        "AudioSource" => Shape::Object(
            &[("bytes", "scalar_21"), ("s3Location", "S3Location")],
            true,
        ),
        "ToolUseBlock" => Shape::Object(
            &[
                ("toolUseId", "scalar_0"),
                ("name", "scalar_0"),
                ("input", "scalar_15"),
                ("type", "scalar_0"),
            ],
            false,
        ),
        "scalar_15" => Shape::Document,
        "ToolResultBlock" => Shape::Object(
            &[
                ("toolUseId", "scalar_0"),
                ("content", "ToolResultContentBlocks"),
                ("status", "scalar_0"),
                ("type", "scalar_0"),
            ],
            false,
        ),
        "ToolResultContentBlocks" => Shape::List("ToolResultContentBlock", false),
        "ToolResultContentBlock" => Shape::Object(
            &[
                ("json", "scalar_15"),
                ("text", "scalar_0"),
                ("image", "ImageBlock"),
                ("document", "DocumentBlock"),
                ("video", "VideoBlock"),
                ("searchResult", "SearchResultBlock"),
            ],
            true,
        ),
        "SearchResultBlock" => Shape::Object(
            &[
                ("source", "scalar_0"),
                ("title", "scalar_0"),
                ("content", "SearchResultContentBlocks"),
                ("citations", "CitationsConfig"),
            ],
            false,
        ),
        "SearchResultContentBlocks" => Shape::List("SearchResultContentBlock", false),
        "SearchResultContentBlock" => Shape::Object(&[("text", "scalar_0")], false),
        "GuardrailConverseContentBlock" => Shape::Object(
            &[
                ("text", "GuardrailConverseTextBlock"),
                ("image", "GuardrailConverseImageBlock"),
            ],
            true,
        ),
        "GuardrailConverseTextBlock" => {
            Shape::Object(&[("text", "scalar_0"), ("qualifiers", "scalar_64")], false)
        }
        "scalar_64" => Shape::List("scalar_0", false),
        "GuardrailConverseImageBlock" => Shape::Object(
            &[
                ("format", "scalar_0"),
                ("source", "GuardrailConverseImageSource"),
            ],
            false,
        ),
        "GuardrailConverseImageSource" => Shape::Object(&[("bytes", "scalar_21")], true),
        "CachePointBlock" => Shape::Object(&[("type", "scalar_0"), ("ttl", "scalar_0")], false),
        "ReasoningContentBlock" => Shape::Object(
            &[
                ("reasoningText", "ReasoningTextBlock"),
                ("redactedContent", "scalar_21"),
            ],
            true,
        ),
        "ReasoningTextBlock" => {
            Shape::Object(&[("text", "scalar_0"), ("signature", "scalar_0")], false)
        }
        "CitationsContentBlock" => Shape::Object(
            &[
                ("content", "CitationGeneratedContentList"),
                ("citations", "Citations"),
            ],
            false,
        ),
        "CitationGeneratedContentList" => Shape::List("CitationGeneratedContent", false),
        "CitationGeneratedContent" => Shape::Object(&[("text", "scalar_0")], true),
        "Citations" => Shape::List("Citation", false),
        "Citation" => Shape::Object(
            &[
                ("title", "scalar_0"),
                ("source", "scalar_0"),
                ("sourceContent", "CitationSourceContentList"),
                ("location", "CitationLocation"),
            ],
            false,
        ),
        "CitationSourceContentList" => Shape::List("CitationSourceContent", false),
        "CitationSourceContent" => Shape::Object(&[("text", "scalar_0")], true),
        "CitationLocation" => Shape::Object(
            &[
                ("web", "WebLocation"),
                ("documentChar", "DocumentCharLocation"),
                ("documentPage", "DocumentPageLocation"),
                ("documentChunk", "DocumentChunkLocation"),
                ("searchResultLocation", "SearchResultLocation"),
            ],
            true,
        ),
        "WebLocation" => Shape::Object(&[("url", "scalar_0"), ("domain", "scalar_0")], false),
        "DocumentCharLocation" => Shape::Object(
            &[
                ("documentIndex", "scalar_1"),
                ("start", "scalar_1"),
                ("end", "scalar_1"),
            ],
            false,
        ),
        "scalar_1" => Shape::Scalar,
        "DocumentPageLocation" => Shape::Object(
            &[
                ("documentIndex", "scalar_1"),
                ("start", "scalar_1"),
                ("end", "scalar_1"),
            ],
            false,
        ),
        "DocumentChunkLocation" => Shape::Object(
            &[
                ("documentIndex", "scalar_1"),
                ("start", "scalar_1"),
                ("end", "scalar_1"),
            ],
            false,
        ),
        "SearchResultLocation" => Shape::Object(
            &[
                ("searchResultIndex", "scalar_1"),
                ("start", "scalar_1"),
                ("end", "scalar_1"),
            ],
            false,
        ),
        "SystemContentBlocks" => Shape::List("SystemContentBlock", false),
        "SystemContentBlock" => Shape::Object(
            &[
                ("text", "scalar_0"),
                ("guardContent", "GuardrailConverseContentBlock"),
                ("cachePoint", "CachePointBlock"),
            ],
            true,
        ),
        "InferenceConfiguration" => Shape::Object(
            &[
                ("maxTokens", "scalar_1"),
                ("temperature", "scalar_1"),
                ("topP", "scalar_1"),
                ("stopSequences", "scalar_64"),
            ],
            false,
        ),
        "ToolConfiguration" => {
            Shape::Object(&[("tools", "Tools"), ("toolChoice", "ToolChoice")], false)
        }
        "Tools" => Shape::List("Tool", false),
        "Tool" => Shape::Object(
            &[
                ("toolSpec", "ToolSpecification"),
                ("systemTool", "SystemTool"),
                ("cachePoint", "CachePointBlock"),
            ],
            true,
        ),
        "ToolSpecification" => Shape::Object(
            &[
                ("name", "scalar_0"),
                ("inputSchema", "ToolInputSchema"),
                ("description", "scalar_0"),
                ("strict", "scalar_2"),
            ],
            false,
        ),
        "ToolInputSchema" => Shape::Object(&[("json", "scalar_15")], true),
        "SystemTool" => Shape::Object(&[("name", "scalar_0")], false),
        "ToolChoice" => Shape::Object(
            &[
                ("auto", "AutoToolChoice"),
                ("any", "AnyToolChoice"),
                ("tool", "SpecificToolChoice"),
            ],
            true,
        ),
        "AutoToolChoice" => Shape::Object(&[], false),
        "AnyToolChoice" => Shape::Object(&[], false),
        "SpecificToolChoice" => Shape::Object(&[("name", "scalar_0")], false),
        "GuardrailStreamConfiguration" => Shape::Object(
            &[
                ("guardrailIdentifier", "scalar_0"),
                ("guardrailVersion", "scalar_0"),
                ("trace", "scalar_0"),
                ("streamProcessingMode", "scalar_0"),
            ],
            false,
        ),
        "PromptVariableMap" => Shape::Map("PromptVariableValues", false),
        "PromptVariableValues" => Shape::Object(&[("text", "scalar_0")], true),
        "RequestMetadata" => Shape::Map("scalar_0", false),
        "PerformanceConfiguration" => Shape::Object(&[("latency", "scalar_0")], false),
        "ServiceTier" => Shape::Object(&[("type", "scalar_0")], false),
        "OutputConfig" => Shape::Object(&[("textFormat", "OutputFormat")], false),
        "OutputFormat" => Shape::Object(
            &[("type", "scalar_0"), ("structure", "OutputFormatStructure")],
            false,
        ),
        "OutputFormatStructure" => Shape::Object(&[("jsonSchema", "JsonSchemaDefinition")], true),
        "JsonSchemaDefinition" => Shape::Object(
            &[
                ("schema", "scalar_0"),
                ("name", "scalar_0"),
                ("description", "scalar_0"),
            ],
            false,
        ),
        "ConverseStreamOutput" => Shape::Object(
            &[
                ("messageStart", "MessageStartEvent"),
                ("contentBlockStart", "ContentBlockStartEvent"),
                ("contentBlockDelta", "ContentBlockDeltaEvent"),
                ("contentBlockStop", "ContentBlockStopEvent"),
                ("messageStop", "MessageStopEvent"),
                ("metadata", "ConverseStreamMetadataEvent"),
                ("internalServerException", "InternalServerException"),
                ("modelStreamErrorException", "ModelStreamErrorException"),
                ("validationException", "ValidationException"),
                ("throttlingException", "ThrottlingException"),
                ("serviceUnavailableException", "ServiceUnavailableException"),
            ],
            true,
        ),
        "MessageStartEvent" => Shape::Object(&[("role", "scalar_0")], false),
        "ContentBlockStartEvent" => Shape::Object(
            &[
                ("start", "ContentBlockStart"),
                ("contentBlockIndex", "scalar_1"),
            ],
            false,
        ),
        "ContentBlockStart" => Shape::Object(
            &[
                ("toolUse", "ToolUseBlockStart"),
                ("toolResult", "ToolResultBlockStart"),
                ("image", "ImageBlockStart"),
            ],
            true,
        ),
        "ToolUseBlockStart" => Shape::Object(
            &[
                ("toolUseId", "scalar_0"),
                ("name", "scalar_0"),
                ("type", "scalar_0"),
            ],
            false,
        ),
        "ToolResultBlockStart" => Shape::Object(
            &[
                ("toolUseId", "scalar_0"),
                ("type", "scalar_0"),
                ("status", "scalar_0"),
            ],
            false,
        ),
        "ImageBlockStart" => Shape::Object(&[("format", "scalar_0")], false),
        "ContentBlockDeltaEvent" => Shape::Object(
            &[
                ("delta", "ContentBlockDelta"),
                ("contentBlockIndex", "scalar_1"),
            ],
            false,
        ),
        "ContentBlockDelta" => Shape::Object(
            &[
                ("text", "scalar_0"),
                ("toolUse", "ToolUseBlockDelta"),
                ("toolResult", "ToolResultBlocksDelta"),
                ("reasoningContent", "ReasoningContentBlockDelta"),
                ("citation", "CitationsDelta"),
                ("image", "ImageBlockDelta"),
            ],
            true,
        ),
        "ToolUseBlockDelta" => Shape::Object(&[("input", "scalar_0")], false),
        "ToolResultBlocksDelta" => Shape::List("ToolResultBlockDelta", false),
        "ToolResultBlockDelta" => {
            Shape::Object(&[("text", "scalar_0"), ("json", "scalar_15")], true)
        }
        "ReasoningContentBlockDelta" => Shape::Object(
            &[
                ("text", "scalar_0"),
                ("redactedContent", "scalar_21"),
                ("signature", "scalar_0"),
            ],
            true,
        ),
        "CitationsDelta" => Shape::Object(
            &[
                ("title", "scalar_0"),
                ("source", "scalar_0"),
                ("sourceContent", "CitationSourceContentListDelta"),
                ("location", "CitationLocation"),
            ],
            false,
        ),
        "CitationSourceContentListDelta" => Shape::List("CitationSourceContentDelta", false),
        "CitationSourceContentDelta" => Shape::Object(&[("text", "scalar_0")], false),
        "ImageBlockDelta" => {
            Shape::Object(&[("source", "ImageSource"), ("error", "ErrorBlock")], false)
        }
        "ContentBlockStopEvent" => Shape::Object(&[("contentBlockIndex", "scalar_1")], false),
        "MessageStopEvent" => Shape::Object(
            &[
                ("stopReason", "scalar_0"),
                ("additionalModelResponseFields", "scalar_15"),
            ],
            false,
        ),
        "ConverseStreamMetadataEvent" => Shape::Object(
            &[
                ("usage", "TokenUsage"),
                ("metrics", "ConverseStreamMetrics"),
                ("trace", "ConverseStreamTrace"),
                ("performanceConfig", "PerformanceConfiguration"),
                ("serviceTier", "ServiceTier"),
            ],
            false,
        ),
        "TokenUsage" => Shape::Object(
            &[
                ("inputTokens", "scalar_1"),
                ("outputTokens", "scalar_1"),
                ("totalTokens", "scalar_1"),
                ("cacheReadInputTokens", "scalar_1"),
                ("cacheWriteInputTokens", "scalar_1"),
                ("cacheDetails", "CacheDetailsList"),
            ],
            false,
        ),
        "CacheDetailsList" => Shape::List("CacheDetail", false),
        "CacheDetail" => Shape::Object(&[("ttl", "scalar_0"), ("inputTokens", "scalar_1")], false),
        "ConverseStreamMetrics" => Shape::Object(&[("latencyMs", "scalar_1")], false),
        "ConverseStreamTrace" => Shape::Object(
            &[
                ("guardrail", "GuardrailTraceAssessment"),
                ("promptRouter", "PromptRouterTrace"),
            ],
            false,
        ),
        "GuardrailTraceAssessment" => Shape::Object(
            &[
                ("modelOutput", "scalar_64"),
                ("inputAssessment", "GuardrailAssessmentMap"),
                ("outputAssessments", "GuardrailAssessmentListMap"),
                ("actionReason", "scalar_0"),
            ],
            false,
        ),
        "GuardrailAssessmentMap" => Shape::Map("GuardrailAssessment", false),
        "GuardrailAssessment" => Shape::Object(
            &[
                ("topicPolicy", "GuardrailTopicPolicyAssessment"),
                ("contentPolicy", "GuardrailContentPolicyAssessment"),
                ("wordPolicy", "GuardrailWordPolicyAssessment"),
                (
                    "sensitiveInformationPolicy",
                    "GuardrailSensitiveInformationPolicyAssessment",
                ),
                (
                    "contextualGroundingPolicy",
                    "GuardrailContextualGroundingPolicyAssessment",
                ),
                (
                    "automatedReasoningPolicy",
                    "GuardrailAutomatedReasoningPolicyAssessment",
                ),
                ("invocationMetrics", "GuardrailInvocationMetrics"),
                ("appliedGuardrailDetails", "AppliedGuardrailDetails"),
            ],
            false,
        ),
        "GuardrailTopicPolicyAssessment" => {
            Shape::Object(&[("topics", "GuardrailTopicList")], false)
        }
        "GuardrailTopicList" => Shape::List("GuardrailTopic", false),
        "GuardrailTopic" => Shape::Object(
            &[
                ("name", "scalar_0"),
                ("type", "scalar_0"),
                ("action", "scalar_0"),
                ("detected", "scalar_2"),
            ],
            false,
        ),
        "GuardrailContentPolicyAssessment" => {
            Shape::Object(&[("filters", "GuardrailContentFilterList")], false)
        }
        "GuardrailContentFilterList" => Shape::List("GuardrailContentFilter", false),
        "GuardrailContentFilter" => Shape::Object(
            &[
                ("type", "scalar_0"),
                ("confidence", "scalar_0"),
                ("action", "scalar_0"),
                ("filterStrength", "scalar_0"),
                ("detected", "scalar_2"),
            ],
            false,
        ),
        "GuardrailWordPolicyAssessment" => Shape::Object(
            &[
                ("customWords", "GuardrailCustomWordList"),
                ("managedWordLists", "GuardrailManagedWordList"),
            ],
            false,
        ),
        "GuardrailCustomWordList" => Shape::List("GuardrailCustomWord", false),
        "GuardrailCustomWord" => Shape::Object(
            &[
                ("match", "scalar_0"),
                ("action", "scalar_0"),
                ("detected", "scalar_2"),
            ],
            false,
        ),
        "GuardrailManagedWordList" => Shape::List("GuardrailManagedWord", false),
        "GuardrailManagedWord" => Shape::Object(
            &[
                ("match", "scalar_0"),
                ("type", "scalar_0"),
                ("action", "scalar_0"),
                ("detected", "scalar_2"),
            ],
            false,
        ),
        "GuardrailSensitiveInformationPolicyAssessment" => Shape::Object(
            &[
                ("piiEntities", "GuardrailPiiEntityFilterList"),
                ("regexes", "GuardrailRegexFilterList"),
            ],
            false,
        ),
        "GuardrailPiiEntityFilterList" => Shape::List("GuardrailPiiEntityFilter", false),
        "GuardrailPiiEntityFilter" => Shape::Object(
            &[
                ("match", "scalar_0"),
                ("type", "scalar_0"),
                ("action", "scalar_0"),
                ("detected", "scalar_2"),
            ],
            false,
        ),
        "GuardrailRegexFilterList" => Shape::List("GuardrailRegexFilter", false),
        "GuardrailRegexFilter" => Shape::Object(
            &[
                ("action", "scalar_0"),
                ("name", "scalar_0"),
                ("match", "scalar_0"),
                ("regex", "scalar_0"),
                ("detected", "scalar_2"),
            ],
            false,
        ),
        "GuardrailContextualGroundingPolicyAssessment" => {
            Shape::Object(&[("filters", "GuardrailContextualGroundingFilters")], false)
        }
        "GuardrailContextualGroundingFilters" => {
            Shape::List("GuardrailContextualGroundingFilter", false)
        }
        "GuardrailContextualGroundingFilter" => Shape::Object(
            &[
                ("type", "scalar_0"),
                ("threshold", "scalar_1"),
                ("score", "scalar_1"),
                ("action", "scalar_0"),
                ("detected", "scalar_2"),
            ],
            false,
        ),
        "GuardrailAutomatedReasoningPolicyAssessment" => Shape::Object(
            &[("findings", "GuardrailAutomatedReasoningFindingList")],
            false,
        ),
        "GuardrailAutomatedReasoningFindingList" => {
            Shape::List("GuardrailAutomatedReasoningFinding", false)
        }
        "GuardrailAutomatedReasoningFinding" => Shape::Object(
            &[
                ("valid", "GuardrailAutomatedReasoningValidFinding"),
                ("invalid", "GuardrailAutomatedReasoningInvalidFinding"),
                (
                    "satisfiable",
                    "GuardrailAutomatedReasoningSatisfiableFinding",
                ),
                ("impossible", "GuardrailAutomatedReasoningImpossibleFinding"),
                (
                    "translationAmbiguous",
                    "GuardrailAutomatedReasoningTranslationAmbiguousFinding",
                ),
                ("tooComplex", "GuardrailAutomatedReasoningTooComplexFinding"),
                (
                    "noTranslations",
                    "GuardrailAutomatedReasoningNoTranslationsFinding",
                ),
            ],
            true,
        ),
        "GuardrailAutomatedReasoningValidFinding" => Shape::Object(
            &[
                ("translation", "GuardrailAutomatedReasoningTranslation"),
                ("claimsTrueScenario", "GuardrailAutomatedReasoningScenario"),
                ("supportingRules", "GuardrailAutomatedReasoningRuleList"),
                ("logicWarning", "GuardrailAutomatedReasoningLogicWarning"),
            ],
            false,
        ),
        "GuardrailAutomatedReasoningTranslation" => Shape::Object(
            &[
                ("premises", "GuardrailAutomatedReasoningStatementList"),
                ("claims", "GuardrailAutomatedReasoningStatementList"),
                (
                    "untranslatedPremises",
                    "GuardrailAutomatedReasoningInputTextReferenceList",
                ),
                (
                    "untranslatedClaims",
                    "GuardrailAutomatedReasoningInputTextReferenceList",
                ),
                ("confidence", "scalar_1"),
            ],
            false,
        ),
        "GuardrailAutomatedReasoningStatementList" => {
            Shape::List("GuardrailAutomatedReasoningStatement", false)
        }
        "GuardrailAutomatedReasoningStatement" => Shape::Object(
            &[("logic", "scalar_0"), ("naturalLanguage", "scalar_0")],
            false,
        ),
        "GuardrailAutomatedReasoningInputTextReferenceList" => {
            Shape::List("GuardrailAutomatedReasoningInputTextReference", false)
        }
        "GuardrailAutomatedReasoningInputTextReference" => {
            Shape::Object(&[("text", "scalar_0")], false)
        }
        "GuardrailAutomatedReasoningScenario" => Shape::Object(
            &[("statements", "GuardrailAutomatedReasoningStatementList")],
            false,
        ),
        "GuardrailAutomatedReasoningRuleList" => {
            Shape::List("GuardrailAutomatedReasoningRule", false)
        }
        "GuardrailAutomatedReasoningRule" => Shape::Object(
            &[("identifier", "scalar_0"), ("policyVersionArn", "scalar_0")],
            false,
        ),
        "GuardrailAutomatedReasoningLogicWarning" => Shape::Object(
            &[
                ("type", "scalar_0"),
                ("premises", "GuardrailAutomatedReasoningStatementList"),
                ("claims", "GuardrailAutomatedReasoningStatementList"),
            ],
            false,
        ),
        "GuardrailAutomatedReasoningInvalidFinding" => Shape::Object(
            &[
                ("translation", "GuardrailAutomatedReasoningTranslation"),
                ("contradictingRules", "GuardrailAutomatedReasoningRuleList"),
                ("logicWarning", "GuardrailAutomatedReasoningLogicWarning"),
            ],
            false,
        ),
        "GuardrailAutomatedReasoningSatisfiableFinding" => Shape::Object(
            &[
                ("translation", "GuardrailAutomatedReasoningTranslation"),
                ("claimsTrueScenario", "GuardrailAutomatedReasoningScenario"),
                ("claimsFalseScenario", "GuardrailAutomatedReasoningScenario"),
                ("logicWarning", "GuardrailAutomatedReasoningLogicWarning"),
            ],
            false,
        ),
        "GuardrailAutomatedReasoningImpossibleFinding" => Shape::Object(
            &[
                ("translation", "GuardrailAutomatedReasoningTranslation"),
                ("contradictingRules", "GuardrailAutomatedReasoningRuleList"),
                ("logicWarning", "GuardrailAutomatedReasoningLogicWarning"),
            ],
            false,
        ),
        "GuardrailAutomatedReasoningTranslationAmbiguousFinding" => Shape::Object(
            &[
                (
                    "options",
                    "GuardrailAutomatedReasoningTranslationOptionList",
                ),
                (
                    "differenceScenarios",
                    "GuardrailAutomatedReasoningDifferenceScenarioList",
                ),
            ],
            false,
        ),
        "GuardrailAutomatedReasoningTranslationOptionList" => {
            Shape::List("GuardrailAutomatedReasoningTranslationOption", false)
        }
        "GuardrailAutomatedReasoningTranslationOption" => Shape::Object(
            &[("translations", "GuardrailAutomatedReasoningTranslationList")],
            false,
        ),
        "GuardrailAutomatedReasoningTranslationList" => {
            Shape::List("GuardrailAutomatedReasoningTranslation", false)
        }
        "GuardrailAutomatedReasoningDifferenceScenarioList" => {
            Shape::List("GuardrailAutomatedReasoningScenario", false)
        }
        "GuardrailAutomatedReasoningTooComplexFinding" => Shape::Object(&[], false),
        "GuardrailAutomatedReasoningNoTranslationsFinding" => Shape::Object(&[], false),
        "GuardrailInvocationMetrics" => Shape::Object(
            &[
                ("guardrailProcessingLatency", "scalar_1"),
                ("usage", "GuardrailUsage"),
                ("guardrailCoverage", "GuardrailCoverage"),
            ],
            false,
        ),
        "GuardrailUsage" => Shape::Object(
            &[
                ("topicPolicyUnits", "scalar_1"),
                ("contentPolicyUnits", "scalar_1"),
                ("wordPolicyUnits", "scalar_1"),
                ("sensitiveInformationPolicyUnits", "scalar_1"),
                ("sensitiveInformationPolicyFreeUnits", "scalar_1"),
                ("contextualGroundingPolicyUnits", "scalar_1"),
                ("contentPolicyImageUnits", "scalar_1"),
                ("automatedReasoningPolicyUnits", "scalar_1"),
                ("automatedReasoningPolicies", "scalar_1"),
            ],
            false,
        ),
        "GuardrailCoverage" => Shape::Object(
            &[
                ("textCharacters", "GuardrailTextCharactersCoverage"),
                ("images", "GuardrailImageCoverage"),
            ],
            false,
        ),
        "GuardrailTextCharactersCoverage" => {
            Shape::Object(&[("guarded", "scalar_1"), ("total", "scalar_1")], false)
        }
        "GuardrailImageCoverage" => {
            Shape::Object(&[("guarded", "scalar_1"), ("total", "scalar_1")], false)
        }
        "AppliedGuardrailDetails" => Shape::Object(
            &[
                ("guardrailId", "scalar_0"),
                ("guardrailVersion", "scalar_0"),
                ("guardrailArn", "scalar_0"),
                ("guardrailOrigin", "scalar_64"),
                ("guardrailOwnership", "scalar_0"),
            ],
            false,
        ),
        "GuardrailAssessmentListMap" => Shape::Map("GuardrailAssessmentList", false),
        "GuardrailAssessmentList" => Shape::List("GuardrailAssessment", false),
        "PromptRouterTrace" => Shape::Object(&[("invokedModelId", "scalar_0")], false),
        "InternalServerException" => Shape::Object(&[("message", "scalar_0")], false),
        "ModelStreamErrorException" => Shape::Object(
            &[
                ("message", "scalar_0"),
                ("originalStatusCode", "scalar_1"),
                ("originalMessage", "scalar_0"),
            ],
            false,
        ),
        "ValidationException" => Shape::Object(&[("message", "scalar_0")], false),
        "ThrottlingException" => Shape::Object(&[("message", "scalar_0")], false),
        "ServiceUnavailableException" => Shape::Object(&[("message", "scalar_0")], false),
        "ConverseStreamResponse" => Shape::Object(&[("stream", "ConverseStreamOutput")], false),
        _ => return None,
    })
}
