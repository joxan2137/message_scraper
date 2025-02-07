use serde::Serialize;
use std::collections::HashSet;
use serenity::all::Message;

#[derive(Debug, Serialize)]
pub struct Conversation {
    pub messages: Vec<String>,
    pub context_type: ConversationType,
    pub participants: Vec<String>,
    pub timestamp: i64,
    pub quality_score: f32,
}

#[derive(Debug, Serialize)]
pub enum ConversationType {
    Reply,
    Thread,
    TimeWindow,
} 