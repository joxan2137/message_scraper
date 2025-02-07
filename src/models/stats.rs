use std::time::Duration;

#[derive(Default, Clone)]
pub struct ChannelStats {
    pub name: String,
    pub messages_processed: usize,
    pub conversations_found: usize,
    pub time_taken: Duration,
}

pub struct Stats {
    pub output_path: String,
    pub channels_processed: usize,
    pub total_messages: usize,
    pub total_conversations: usize,
    pub channel_stats: Vec<ChannelStats>,
    pub start_time: std::time::Instant,
}

impl Stats {
    pub fn new(output_path: String) -> Self {
        Self {
            output_path,
            channels_processed: 0,
            total_messages: 0,
            total_conversations: 0,
            channel_stats: Vec::new(),
            start_time: std::time::Instant::now(),
        }
    }

    pub fn add_channel_stats(&mut self, stats: ChannelStats) {
        self.channels_processed += 1;
        self.total_messages += stats.messages_processed;
        self.total_conversations += stats.conversations_found;
        self.channel_stats.push(stats);
    }

    pub fn print_stats(&self) {
        println!("\n📊 Scraping Statistics:");
        println!("⏱️  Time taken: {:.2?}", self.start_time.elapsed());
        println!("📁 Channels processed: {}", self.channels_processed);
        println!("💬 Total messages: {}", self.total_messages);
        println!("🗣️  Total conversations: {}", self.total_conversations);
        
        if let Ok(metadata) = std::fs::metadata(&self.output_path) {
            println!("💾 Output file size: {:.2} MB", metadata.len() as f64 / 1_000_000.0);
        }

        println!("\n📋 Per-channel breakdown:");
        for stats in &self.channel_stats {
            println!("\n#{}", stats.name);
            println!("  Messages: {}", stats.messages_processed);
            println!("  Conversations: {}", stats.conversations_found);
            println!("  Time: {:.2?}", stats.time_taken);
            if stats.messages_processed > 0 {
                println!("  Conversation rate: {:.1}%", 
                    (stats.conversations_found as f64 / stats.messages_processed as f64) * 100.0);
            }
        }
    }
} 