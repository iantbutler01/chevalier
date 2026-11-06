use super::{Runtime, ToolFunction};
use crate::error::{Error, Result};
use regex::RegexBuilder;
use serde_json::json;

impl Runtime {
    /// Expose a small initial tool set and let the model discover the complete registry.
    /// Persist `loaded_tool_names` with conversation state and pass it here on resume.
    /// This changes schema visibility only; execution authorization belongs to the host.
    pub async fn enable_tool_search(&self, initially_loaded: &[String]) -> Result<()> {
        let mut loaded = initially_loaded.to_vec();
        if !loaded.iter().any(|name| name == "tool_search") {
            loaded.push("tool_search".into());
        }
        if !self.get_tool_schemas().await.contains_key("tool_search") {
            let schemas = self.tool_schemas.clone();
            let visible = self.discovered_tool_names.clone();
            self.register_tool_with_schema(
                "tool_search",
                "Search the entire tool catalog by space-separated case-insensitive search terms over names, descriptions and parameters. Every term must match; each term can use regular-expression alternatives. Matching tools are loaded for subsequent calls. Use alternatives such as calendar|appointment, or an exact tool name. Search again whenever another capability is needed.",
                json!({"type":"object","properties":{
                    "query":{"type":"string","description":"Space-separated search terms; regex alternatives such as calendar|appointment are supported"},
                    "limit":{"type":"integer","minimum":1,"description":"Maximum matches to load; defaults to 5"}
                },"required":["query"],"additionalProperties":false}),
                ToolFunction::Async(Box::new(move |args| {
                    let schemas = schemas.clone();
                    let visible = visible.clone();
                    Box::pin(async move {
                        let query = args["query"].as_str().ok_or_else(|| Error::NonRetryable("tool_search requires query".into()))?;
                        let patterns = query.split_whitespace().map(|term| {
                            RegexBuilder::new(term).case_insensitive(true).build()
                                .map_err(|error| Error::NonRetryable(format!("Invalid search pattern: {error}")))
                        }).collect::<Result<Vec<_>>>()?;
                        let limit = match args.get("limit") {
                            None | Some(serde_json::Value::Null) => 5,
                            Some(value) => value.as_u64().and_then(|value| usize::try_from(value).ok()).filter(|&value| value > 0)
                                .ok_or_else(|| Error::NonRetryable("limit must be a positive integer".into()))?,
                        };
                        let schemas = schemas.read().await;
                        let mut matches = schemas.values().filter(|tool| {
                            let parameters = tool.parameters.to_json_schema().to_string();
                            patterns.iter().all(|pattern| pattern.is_match(&tool.name) || pattern.is_match(&tool.description) || pattern.is_match(&parameters))
                        }).collect::<Vec<_>>();
                        matches.sort_by(|a,b| (a.name != query, &a.name).cmp(&(b.name != query, &b.name)));
                        let total = matches.len();
                        matches.truncate(limit);
                        let mut visible = visible.write().await;
                        let loaded = visible.as_mut().ok_or_else(|| Error::NonRetryable("Tool discovery is not enabled".into()))?;
                        for tool in &matches {
                            if !loaded.contains(&tool.name) { loaded.push(tool.name.clone()); }
                        }
                        Ok(json!({"total_matches":total,"tools":matches.iter().map(|tool| json!({
                            "name":tool.name,"description":tool.description,"parameters":tool.parameters.to_json_schema()
                        })).collect::<Vec<_>>(),"loaded_tool_names":loaded}).to_string())
                    })
                })),
            ).await?;
        }
        let schemas = self.tool_schemas.read().await;
        let mut seen = std::collections::HashSet::new();
        loaded.retain(|name| seen.insert(name.clone()));
        for name in &loaded {
            if !schemas.contains_key(name) {
                return Err(Error::ToolNotFound(name.clone()));
            }
        }
        *self.discovered_tool_names.write().await = Some(loaded);
        Ok(())
    }

    /// The ordered schema set to save alongside the host's conversation checkpoint.
    pub async fn loaded_tool_names(&self) -> Option<Vec<String>> {
        self.discovered_tool_names.read().await.clone()
    }
    pub(super) async fn effective_model_tool_names(&self) -> Option<Vec<String>> {
        let discovered = self.discovered_tool_names.read().await;
        let visible = self.model_tool_names.read().await;
        match (discovered.as_ref(), visible.as_ref()) {
            (Some(loaded), Some(visible)) => Some(
                loaded
                    .iter()
                    .filter(|name| visible.contains(name))
                    .cloned()
                    .collect(),
            ),
            (Some(loaded), None) => Some(loaded.clone()),
            (None, visible) => visible.cloned(),
        }
    }
}
