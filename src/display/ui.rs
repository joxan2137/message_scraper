use std::{collections::HashMap, io::stdout, time::Duration};
use std::io::Write;
use anyhow::Result;
use crossterm::{
    cursor,
    terminal::{Clear, ClearType},
    ExecutableCommand,
};
use colorful::{Colorful, RGB};
use crate::models::ChannelProgress;

pub struct Display {
    pub active_channels: HashMap<String, ChannelProgress>,
    pub total_messages: usize,
    pub total_conversations: usize,
    pub total_saved: usize,
    pub channels_processed: usize,
    pub total_channels: usize,
    pub elapsed: Duration,
    pub file_size: u64,
}

impl Display {
    pub fn new(total_channels: usize) -> Self {
        Self {
            active_channels: HashMap::new(),
            total_messages: 0,
            total_conversations: 0,
            total_saved: 0,
            channels_processed: 0,
            total_channels,
            elapsed: Duration::default(),
            file_size: 0,
        }
    }

    pub fn update_channel(&mut self, name: String, messages: usize, conversations: usize, is_active: bool) {
        let progress = self.active_channels.entry(name).or_insert(ChannelProgress {
            messages: 0,
            conversations: 0,
            is_active: true,
        });
        
        progress.messages = messages;
        progress.conversations = conversations;
        progress.is_active = is_active;

        self.total_messages = self.active_channels.values()
            .map(|p| p.messages)
            .sum();
        self.total_conversations = self.active_channels.values()
            .map(|p| p.conversations)
            .sum();
    }

    pub fn update(&mut self) -> Result<()> {
        let mut stdout = stdout();
        stdout.execute(cursor::SavePosition)?;
        stdout.execute(cursor::MoveTo(0, 0))?;
        stdout.execute(Clear(ClearType::FromCursorDown))?;

        let primary = RGB::new(79, 70, 229);     // Indigo
        let secondary = RGB::new(236, 72, 153);   // Pink
        let success = RGB::new(16, 185, 129);    // Emerald
        let warning = RGB::new(245, 158, 11);    // Amber
        let info = RGB::new(59, 130, 246);      // Blue
        let subtle = RGB::new(107, 114, 128);   // Gray

        // Active channels section
        writeln!(stdout, "\n  🔄 Active Channels:")?;
        let active_count = self.active_channels.values().filter(|p| p.is_active).count();
        if active_count == 0 {
            writeln!(stdout, "     {}", "None".to_string().color(subtle))?;
        } else {
            for (name, progress) in self.active_channels.iter().filter(|(_, p)| p.is_active) {
                writeln!(stdout, "     ├─ {} ({} msgs, {} convs)", 
                    name.to_string().color(info),
                    progress.messages.to_string().color(success),
                    progress.conversations.to_string().color(primary))?;
            }
        }

        // Recently completed channels
        let completed: Vec<_> = self.active_channels.iter()
            .filter(|(_, p)| !p.is_active)
            .take(3)
            .collect();

        if !completed.is_empty() {
            writeln!(stdout, "\n  ✅ Recently Completed:")?;
            for (name, progress) in completed {
                writeln!(stdout, "     ├─ {} ({} msgs, {} convs)", 
                    name.to_string().color(subtle),
                    progress.messages.to_string().color(subtle),
                    progress.conversations.to_string().color(subtle))?;
            }
        }

        // Overall progress section
        writeln!(stdout, "\n  📊 Overall Progress:")?;
        
        let progress = format!("{}/{}", self.channels_processed, self.total_channels);
        let percentage = (self.channels_processed as f64 / self.total_channels as f64) * 100.0;
        writeln!(stdout, "     ├─ 📂 Channels: {} ({:.1}%)", 
            progress.to_string().color(info),
            percentage)?;

        writeln!(stdout, "     ├─ 📨 Messages: {}", 
            self.total_messages.to_string().color(success))?;
        writeln!(stdout, "     ├─ 💭 Conversations: {}", 
            self.total_conversations.to_string().color(primary))?;
        writeln!(stdout, "     ├─ 💾 Saved: {}", 
            self.total_saved.to_string().color(secondary))?;
        writeln!(stdout, "     ├─ 📁 File Size: {:.2} MB", 
            (self.file_size as f64 / 1_000_000.0).to_string().color(warning))?;

        let minutes = self.elapsed.as_secs() / 60;
        let seconds = self.elapsed.as_secs() % 60;
        writeln!(stdout, "     └─ ⏱️  Time: {}m {}s", 
            minutes.to_string().color(subtle),
            seconds.to_string().color(subtle))?;

        stdout.execute(cursor::RestorePosition)?;
        Ok(())
    }

    pub fn show_shutdown_message(&mut self) -> Result<()> {
        let mut stdout = stdout();
        stdout.execute(cursor::SavePosition)?;
        stdout.execute(cursor::MoveTo(0, 0))?;
        stdout.execute(Clear(ClearType::FromCursorDown))?;

        let msg = "⚠️  Shutting down gracefully..."
            .color(RGB::new(231, 76, 60))
            .to_string();
        writeln!(stdout, "\n  {}\n", msg)?;
        
        self.update()?;
        stdout.execute(cursor::RestorePosition)?;
        Ok(())
    }
} 