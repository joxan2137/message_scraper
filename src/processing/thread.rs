use anyhow::Result;
use serenity::all::{Http, Message};
use serenity::builder::GetMessages;
use tokio::time::Duration;
use crate::models::{ChannelInfo, Conversation, ConversationType};

pub async fn process_thread(http: &Http, channel_info: &ChannelInfo) -> Result<Vec<Conversation>> {
    let mut conversations = Vec::new();
    let mut messages = Vec::new();
    let mut last_id = None;

    loop {
        let mut request = GetMessages::default().limit(100);
        if let Some(id) = last_id {
            request = request.before(id);
        }

        let batch = channel_info.id.messages(http, request).await?;
        if batch.is_empty() {
            break;
        }

        last_id = batch.last().map(|m| m.id);
        messages.extend(batch);

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    if !messages.is_empty() {
        messages.sort_by_key(|m| m.timestamp);

        let thread_conversation = Conversation {
            messages: messages.iter().map(|m| m.content.clone()).collect(),
            context_type: ConversationType::Thread,
            participants: messages.iter().map(|m| m.author.name.clone()).collect(),
            timestamp: messages[0].timestamp.unix_timestamp(),
            quality_score: calculate_thread_quality(&messages),
        };

        conversations.push(thread_conversation);
    }

    Ok(conversations)
}

pub fn calculate_thread_quality(messages: &[Message]) -> f32 {
    let mut score = 1.0;

    let unique_participants: std::collections::HashSet<_> = messages.iter()
        .map(|m| m.author.id)
        .collect();
    score *= 1.0 + unique_participants.len() as f32 / 5.0;

    let avg_length: f32 = messages.iter()
        .map(|m| m.content.len())
        .sum::<usize>() as f32 / messages.len() as f32;
    score *= (1.0 + avg_length / 100.0).min(2.0);

    let code_blocks = messages.iter()
        .filter(|m| m.content.contains("```"))
        .count();
    score *= 1.0 + code_blocks as f32 / messages.len() as f32;

    score
} 