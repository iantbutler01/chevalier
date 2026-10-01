use chevalier::types::{MediaPart, MediaSource, MediaSourceKind, MultimodalMessage, Provider};
use chevalier::utils::{
    ConversationMessage, convert_messages_to_provider_format, convert_messages_to_responses_input,
    validate_image_input_supported,
};

const TINY_PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/p9sAAAAASUVORK5CYII=";

fn image_message() -> Vec<ConversationMessage> {
    vec![ConversationMessage::Multimodal(MultimodalMessage::user(
        vec![
            MediaPart::text("What color is this image?"),
            MediaPart::image(MediaSource::base64(TINY_PNG, "image/png")),
        ],
    ))]
}

#[test]
fn openai_chat_image_input_uses_content_parts() {
    let formatted = convert_messages_to_provider_format(&image_message(), Provider::OpenAI)
        .expect("conversion should succeed");

    assert_eq!(formatted[0]["role"], "user");
    assert_eq!(formatted[0]["content"][0]["type"], "text");
    assert_eq!(
        formatted[0]["content"][0]["text"],
        "What color is this image?"
    );
    assert_eq!(formatted[0]["content"][1]["type"], "image_url");
    assert_eq!(
        formatted[0]["content"][1]["image_url"]["url"],
        format!("data:image/png;base64,{TINY_PNG}")
    );
}

#[test]
fn openrouter_chat_image_input_uses_openai_compatible_content_parts() {
    let formatted = convert_messages_to_provider_format(&image_message(), Provider::OpenRouter)
        .expect("conversion should succeed");

    assert_eq!(formatted[0]["role"], "user");
    assert_eq!(formatted[0]["content"][1]["type"], "image_url");
    assert_eq!(
        formatted[0]["content"][1]["image_url"]["url"],
        format!("data:image/png;base64,{TINY_PNG}")
    );
}

#[test]
fn openai_responses_image_input_uses_input_image_parts() {
    let (_, input) =
        convert_messages_to_responses_input(&image_message(), Provider::OpenAIResponses)
            .expect("conversion should succeed");

    assert_eq!(input[0]["type"], "message");
    assert_eq!(input[0]["role"], "user");
    assert_eq!(input[0]["content"][0]["type"], "input_text");
    assert_eq!(input[0]["content"][0]["text"], "What color is this image?");
    assert_eq!(input[0]["content"][1]["type"], "input_image");
    assert_eq!(
        input[0]["content"][1]["image_url"],
        format!("data:image/png;base64,{TINY_PNG}")
    );
}

#[test]
fn openrouter_responses_image_input_uses_responses_compatible_parts() {
    let (_, input) =
        convert_messages_to_responses_input(&image_message(), Provider::OpenRouterResponses)
            .expect("conversion should succeed");

    assert_eq!(input[0]["type"], "message");
    assert_eq!(input[0]["role"], "user");
    assert_eq!(input[0]["content"][1]["type"], "input_image");
    assert_eq!(
        input[0]["content"][1]["image_url"],
        format!("data:image/png;base64,{TINY_PNG}")
    );
}

#[test]
fn anthropic_image_input_uses_base64_image_blocks() {
    let formatted = convert_messages_to_provider_format(&image_message(), Provider::Anthropic)
        .expect("conversion should succeed");

    assert_eq!(formatted[0]["role"], "user");
    assert_eq!(formatted[0]["content"][0]["type"], "text");
    assert_eq!(formatted[0]["content"][1]["type"], "image");
    assert_eq!(formatted[0]["content"][1]["source"]["type"], "base64");
    assert_eq!(
        formatted[0]["content"][1]["source"]["media_type"],
        "image/png"
    );
    assert_eq!(formatted[0]["content"][1]["source"]["data"], TINY_PNG);
}

#[test]
fn bedrock_claude_image_input_uses_anthropic_image_blocks() {
    let formatted = convert_messages_to_provider_format(&image_message(), Provider::Bedrock)
        .expect("conversion should succeed");

    assert_eq!(formatted[0]["role"], "user");
    assert_eq!(formatted[0]["content"][1]["type"], "image");
    assert_eq!(formatted[0]["content"][1]["source"]["type"], "base64");
    assert_eq!(
        formatted[0]["content"][1]["source"]["media_type"],
        "image/png"
    );
    assert_eq!(formatted[0]["content"][1]["source"]["data"], TINY_PNG);
}

#[test]
fn google_anthropic_image_input_uses_anthropic_image_blocks() {
    let formatted =
        convert_messages_to_provider_format(&image_message(), Provider::GoogleAnthropic)
            .expect("conversion should succeed");

    assert_eq!(formatted[0]["role"], "user");
    assert_eq!(formatted[0]["content"][1]["type"], "image");
    assert_eq!(formatted[0]["content"][1]["source"]["type"], "base64");
    assert_eq!(
        formatted[0]["content"][1]["source"]["media_type"],
        "image/png"
    );
    assert_eq!(formatted[0]["content"][1]["source"]["data"], TINY_PNG);
}

#[test]
fn google_gemini_image_input_uses_inline_data() {
    let formatted = convert_messages_to_provider_format(&image_message(), Provider::GoogleGenAI)
        .expect("conversion should succeed");

    assert_eq!(formatted[0]["role"], "user");
    assert_eq!(
        formatted[0]["parts"][0]["text"],
        "What color is this image?"
    );
    assert_eq!(
        formatted[0]["parts"][1]["inline_data"]["mime_type"],
        "image/png"
    );
    assert_eq!(formatted[0]["parts"][1]["inline_data"]["data"], TINY_PNG);
}

#[test]
fn provider_capability_reports_known_vision_models() {
    assert!(Provider::OpenAI.supports_image_input("gpt-4o-mini"));
    assert!(Provider::OpenAIResponses.supports_image_input("gpt-4.1-mini"));
    assert!(Provider::Anthropic.supports_image_input("claude-haiku-4-5-20251001"));
    assert!(Provider::GoogleGenAI.supports_image_input("gemini-2.5-flash"));
    assert!(Provider::GoogleAnthropic.supports_image_input("claude-3-5-sonnet-v2@20241022"));
    assert!(Provider::Bedrock.supports_image_input("anthropic.claude-3-5-sonnet-20241022-v2:0"));
    assert!(Provider::OpenRouter.supports_image_input("openai/gpt-4o-mini"));
    assert!(Provider::OpenRouterResponses.supports_image_input("openai/gpt-5.2"));
}

#[test]
fn provider_capability_reports_conservative_image_source_kinds() {
    assert_eq!(
        Provider::Anthropic
            .capabilities_for_model("claude-haiku-4-5-20251001")
            .image_input_sources,
        vec![MediaSourceKind::Base64]
    );
    assert_eq!(
        Provider::OpenAI
            .capabilities_for_model("gpt-4o-mini")
            .image_input_sources,
        vec![MediaSourceKind::Base64, MediaSourceKind::Url]
    );
    assert_eq!(
        Provider::OpenAIResponses
            .capabilities_for_model("gpt-4.1-mini")
            .image_input_sources,
        vec![
            MediaSourceKind::Base64,
            MediaSourceKind::Url,
            MediaSourceKind::FileId
        ]
    );
}

#[test]
fn provider_capability_rejects_unknown_models() {
    assert!(!Provider::OpenAI.supports_image_input("gpt-3.5-turbo"));
    assert!(!Provider::Anthropic.supports_image_input("claude-2.1"));
    assert!(!Provider::OpenRouter.supports_image_input("mistralai/mistral-small"));
}

#[test]
fn image_input_validation_fails_before_provider_dispatch_for_unsupported_models() {
    let error =
        validate_image_input_supported(&image_message(), Provider::OpenAI, "gpt-3.5-turbo", None)
            .expect_err("image validation should reject non-vision models");

    assert_eq!(
        error.to_string(),
        "Validation error: This Nym's current model cannot inspect images."
    );
}

#[test]
fn declared_image_input_overrides_the_capability_table_in_both_directions() {
    validate_image_input_supported(
        &image_message(),
        Provider::OpenRouter,
        "vendor/undocumented-vision-model",
        Some(true),
    )
    .expect("@vision=true must let images through a model the table does not know");

    validate_image_input_supported(&image_message(), Provider::OpenAI, "gpt-4o", Some(false))
        .expect_err("@vision=false must refuse images the table would have allowed");
}
