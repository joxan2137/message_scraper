use serenity::all::{ChannelId, ChannelType};

pub struct ChannelInfo {
    pub id: ChannelId,
    pub name: String,
    pub kind: ChannelType,
}

#[derive(PartialEq)]
pub enum ChannelChoice {
    Server,
    DirectMessages,
    Group,
}

pub struct ChannelProgress {
    pub messages: usize,
    pub conversations: usize,
    pub is_active: bool,
} 