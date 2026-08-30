//! Example: Video Analysis Agent with Tools using Google Gemini
//!
//! Run with: GOOGLE_GEMINI_API_KEY=your_key cargo run --example video_upload -- path/to/video.mp4
//!
//! This demonstrates:
//! 1. Using the #[agentic] macro to define an agent function
//! 2. #[derive(Tool)] for type-safe tool definitions
//! 3. Uploading videos via Google File API
//! 4. Multi-turn conversation with tools and video context

use chevalier::Tool;
use chevalier::agentic;
use chevalier::providers::{FileState, GoogleGenAIClient};
use chevalier::runtime::{RunParams, ToolFunction};
use chevalier::types::{ChatRole, MediaPart, MediaSource, MultimodalMessage};
use chevalier::utils::ConversationMessage;
use serde::{Deserialize, Serialize};
use std::env;

// ============================================================================
// Tool Definitions using #[derive(Tool)]
// ============================================================================

/// Extract timestamps from a video for specific events or scenes
#[derive(Tool, Serialize, Deserialize, Debug)]
struct ExtractTimestamps {
    /// Description of what to find timestamps for (e.g., "when the dog appears")
    query: String,
}

/// Generate a summary of the video content
#[derive(Tool, Serialize, Deserialize, Debug)]
struct SummarizeVideo {
    /// Style of summary: "brief", "detailed", or "bullet_points"
    style: String,
    /// Maximum length in sentences (optional)
    max_sentences: Option<u32>,
}

/// Identify and describe objects or people in the video
#[derive(Tool, Serialize, Deserialize, Debug)]
struct IdentifyObjects {
    /// Type of objects to focus on: "all", "people", "text", "logos", "animals"
    object_type: String,
}

/// Transcribe any speech or text visible in the video
#[derive(Tool, Serialize, Deserialize, Debug)]
struct TranscribeContent {
    /// Whether to include timestamps with transcription
    include_timestamps: bool,
}

// ============================================================================
// Video Analysis Agent using #[agentic] macro
// ============================================================================

/// Analyze a video and answer questions about it using available tools
#[agentic(model = "gemini:gemini-2.0-flash")]
async fn analyze_video(
    video_uri: String,
    mime_type: String,
    user_query: String,
    runtime: Runtime,
) -> chevalier::error::Result<String> {
    // Register tools with the runtime using schema information
    runtime
        .register_tool_with_schema(
            ExtractTimestamps::tool_name(),
            ExtractTimestamps::description(),
            ExtractTimestamps::schema(),
            ToolFunction::Sync(Box::new(|args| {
                let query = args["query"].as_str().unwrap_or("events");
                Ok(format!(
                    "Searching for timestamps related to: '{}'. Found: 0:00 - intro, 0:15 - main content begins",
                    query
                ))
            })),
        )
        .await?;

    runtime
        .register_tool_with_schema(
            SummarizeVideo::tool_name(),
            SummarizeVideo::description(),
            SummarizeVideo::schema(),
            ToolFunction::Sync(Box::new(|args| {
                let style = args["style"].as_str().unwrap_or("brief");
                Ok(format!(
                    "Video summary ({}): This video contains visual content that has been analyzed.",
                    style
                ))
            })),
        )
        .await?;

    runtime
        .register_tool_with_schema(
            IdentifyObjects::tool_name(),
            IdentifyObjects::description(),
            IdentifyObjects::schema(),
            ToolFunction::Sync(Box::new(|args| {
                let obj_type = args["object_type"].as_str().unwrap_or("all");
                Ok(format!(
                    "Identified {} objects in the video frames.",
                    obj_type
                ))
            })),
        )
        .await?;

    runtime
        .register_tool_with_schema(
            TranscribeContent::tool_name(),
            TranscribeContent::description(),
            TranscribeContent::schema(),
            ToolFunction::Sync(Box::new(|args| {
                let with_ts = args["include_timestamps"].as_bool().unwrap_or(false);
                if with_ts {
                    Ok("Transcription with timestamps: [0:00] Hello... [0:05] ...".to_string())
                } else {
                    Ok("Transcription: Hello...".to_string())
                }
            })),
        )
        .await?;

    // Create the multimodal message with video
    let video_message = MultimodalMessage {
        role: ChatRole::User,
        parts: vec![
            MediaPart::Video {
                source: MediaSource::FileUri {
                    uri: video_uri,
                    mime_type: Some(mime_type),
                },
                metadata: None,
            },
            MediaPart::Text { text: user_query },
        ],
        cache_marker: None,
    };

    // Run the agent with video context
    let response = runtime
        .run(RunParams {
            prompt: None, // prompt is in the multimodal message
            system: Some("You are a video analysis assistant. Use the available tools to analyze the video and answer the user's question.".to_string()),
            history: Some(vec![ConversationMessage::Multimodal(video_message)]),
            output_type: None,
            output_schema: None,
            temperature: None,
            top_p: None,
            max_tokens: None,
            reasoning_effort: None,
            model: None,   // use default from agentic macro
            api_key: None, // use default from env
            timeout: None,
            retry_config: None,
            previous_response_id: None,
        })
        .await?;

    // Extract the text content from the response
    let content = response
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| response.to_string());

    Ok(content)
}

// ============================================================================
// Main: Upload video and run the agent
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get API key
    let api_key = env::var("GOOGLE_GEMINI_API_KEY").expect("GOOGLE_GEMINI_API_KEY must be set");

    // Get video path from args
    let args: Vec<String> = env::args().collect();
    let video_path = args
        .get(1)
        .expect("Usage: video_upload <path_to_video.mp4> [query]");

    let user_query = args.get(2).map(|s| s.to_string()).unwrap_or_else(|| {
        "Describe what's happening in this video and identify any key objects or people."
            .to_string()
    });

    // Read video file
    println!("Reading video file: {}", video_path);
    let video_bytes = std::fs::read(video_path)?;
    let file_size_mb = video_bytes.len() as f64 / (1024.0 * 1024.0);
    println!("Video size: {:.2} MB", file_size_mb);

    // Determine MIME type
    let mime_type = match video_path.rsplit('.').next() {
        Some("mp4") => "video/mp4",
        Some("mov") => "video/quicktime",
        Some("avi") => "video/x-msvideo",
        Some("webm") => "video/webm",
        _ => "video/mp4",
    };

    // Create client for file upload
    let client = GoogleGenAIClient::new(&api_key, "gemini-2.0-flash");

    // Upload the video
    println!("\nUploading video to Google File API...");
    let uploaded = client
        .upload_file(&video_bytes, mime_type, Some("agent-video"))
        .await?;

    println!("Upload complete!");
    println!("  File: {}", uploaded.name);
    println!("  URI: {}", uploaded.uri);

    // Wait for processing if needed
    if uploaded.state == FileState::Processing {
        println!("\nWaiting for video processing...");
        client
            .wait_for_file_processing(&uploaded.name, Some(120))
            .await?;
        println!("Processing complete!");
    }

    // Run the video analysis agent
    println!("\n--- Running Video Analysis Agent ---");
    println!("Query: {}", user_query);
    println!();

    let result = analyze_video(uploaded.uri.clone(), mime_type.to_string(), user_query).await?;

    println!("--- Agent Response ---");
    println!("{}", result);
    println!("----------------------");

    // Cleanup
    println!("\nCleaning up uploaded file...");
    client.delete_file(&uploaded.name).await?;
    println!("Done!");

    Ok(())
}
