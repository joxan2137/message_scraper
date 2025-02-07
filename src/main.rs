use std::{env, fs::{File, OpenOptions}, io::{Write, Seek, stdout}, sync::Arc, path::Path, time::Duration, collections::{HashMap, VecDeque}};
use anyhow::{Result, Context as _};
use serenity::{
    all::{ChannelType, Http, Message, MessageId, Channel}, 
    model::id::{ChannelId, GuildId},
    builder::GetMessages,
};
use tokio::sync::{broadcast, mpsc, Semaphore};
use chrono::Utc;
use tokio::signal::ctrl_c;
use crossterm::{
    cursor,
    terminal::{Clear, ClearType},
    ExecutableCommand,
    style::{Color, SetForegroundColor, ResetColor},
    QueueableCommand,
};
use colorful::{Colorful, RGB};
use futures::stream::{self, StreamExt};
use serde::Serialize;

#[derive(Default)]
struct ChannelStats {
    name: String,
    messages_processed: usize,
    conversations_found: usize,
    time_taken: Duration,
}

struct Stats {
    output_path: String,
    channels_processed: usize,
    total_messages: usize,
    total_conversations: usize,
    channel_stats: Vec<ChannelStats>,
    start_time: std::time::Instant,
}

impl Stats {
    fn new(output_path: String) -> Self {
        Self {
            output_path,
            channels_processed: 0,
            total_messages: 0,
            total_conversations: 0,
            channel_stats: Vec::new(),
            start_time: std::time::Instant::now(),
        }
    }

    fn add_channel_stats(&mut self, stats: ChannelStats) {
        self.channels_processed += 1;
        self.total_messages += stats.messages_processed;
        self.total_conversations += stats.conversations_found;
        self.channel_stats.push(stats);
    }

    fn print_stats(&self) {
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

struct Display {
    active_channels: HashMap<String, ChannelProgress>,
    total_messages: usize,
    total_conversations: usize,
    total_saved: usize,
    channels_processed: usize,
    total_channels: usize,
    elapsed: Duration,
    file_size: u64,
}

struct ChannelProgress {
    messages: usize,
    conversations: usize,
    is_active: bool,
}

impl Display {
    fn new(total_channels: usize) -> Self {
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

    fn update_channel(&mut self, name: String, messages: usize, conversations: usize, is_active: bool) {
        let progress = self.active_channels.entry(name).or_insert(ChannelProgress {
            messages: 0,
            conversations: 0,
            is_active: true,
        });
        
        progress.messages = messages;
        progress.conversations = conversations;
        progress.is_active = is_active;

        // Recalculate totals
        self.total_messages = self.active_channels.values()
            .map(|p| p.messages)
            .sum();
        self.total_conversations = self.active_channels.values()
            .map(|p| p.conversations)
            .sum();
    }

    fn update(&mut self) -> Result<()> {
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
        let error = RGB::new(239, 68, 68);      // Red

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
            .take(3)  // Show last 3 completed channels
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

    fn show_shutdown_message(&mut self) -> Result<()> {
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

fn get_output_path(server_name: &str) -> String {
    let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
    let base_path = format!("{}_conversations_{}.json", server_name, timestamp);
    
    let mut counter = 0;
    let mut path = base_path.clone();
    
    while Path::new(&path).exists() {
        counter += 1;
        path = format!("{}_conversations_{}_({}).json", server_name, timestamp, counter);
    }
    
    // Create the file with initial array brackets
    File::create(&path)
        .and_then(|mut f| f.write_all(b"[\n]"))
        .expect("Failed to create output file");
    
    path
}

#[derive(Debug, Serialize)]
struct Conversation {
    messages: Vec<String>,
    context_type: ConversationType,
    participants: Vec<String>,
    timestamp: i64,
    quality_score: f32,
}

#[derive(Debug, Serialize)]
enum ConversationType {
    Reply,
    Thread,
    TimeWindow,
}

struct MessageContext {
    window: VecDeque<Message>,
    window_size: Duration,
}

impl MessageContext {
    fn new(window_size: Duration) -> Self {
        Self {
            window: VecDeque::new(),
            window_size,
        }
    }

    fn add_message(&mut self, msg: Message) {
        // Remove messages outside the time window
        let cutoff = msg.timestamp.unix_timestamp() - self.window_size.as_secs() as i64;
        while let Some(front) = self.window.front() {
            if front.timestamp.unix_timestamp() < cutoff {
                self.window.pop_front();
            } else {
                break;
            }
        }
        self.window.push_back(msg);
    }

    fn find_conversations(&self) -> Vec<Conversation> {
        let mut conversations = Vec::new();
        let mut processed = std::collections::HashSet::new();

        // Look for conversations in the time window
        for (i, msg) in self.window.iter().enumerate() {
            if processed.contains(&msg.id) {
                continue;
            }

            // Look for messages that seem to be part of a conversation
            let mut conversation_msgs = Vec::new();
            let mut participants = Vec::new();
            
            // Add the initial message
            conversation_msgs.push(msg.content.clone());
            participants.push(msg.author.name.clone());
            processed.insert(msg.id);

            // Look for responses within 2 minutes and with similar content
            for other_msg in self.window.iter().skip(i + 1) {
                if processed.contains(&other_msg.id) {
                    continue;
                }

                let time_diff = (other_msg.timestamp.unix_timestamp() - msg.timestamp.unix_timestamp()).abs();
                if time_diff > 120 { // 2 minutes threshold
                    continue;
                }

                // Check if messages seem related (mentions, similar words, etc.)
                if is_likely_response(msg, other_msg) {
                    conversation_msgs.push(other_msg.content.clone());
                    participants.push(other_msg.author.name.clone());
                    processed.insert(other_msg.id);
                }
            }

            // If we found a conversation with at least 2 messages
            if conversation_msgs.len() >= 2 {
                conversations.push(Conversation {
                    messages: conversation_msgs,
                    context_type: ConversationType::TimeWindow,
                    participants,
                    timestamp: msg.timestamp.unix_timestamp(),
                    quality_score: 1.0, // Basic score for now
                });
            }
        }

        conversations
    }
}

fn is_likely_response(msg1: &Message, msg2: &Message) -> bool {
    // Check if msg2 mentions msg1's author
    if msg2.content.contains(&msg1.author.name) {
        return true;
    }

    // Check if messages have similar words (basic implementation)
    let words1: Vec<_> = msg1.content.split_whitespace().collect();
    let words2: Vec<_> = msg2.content.split_whitespace().collect();
    
    let common_words = words1.iter()
        .filter(|w| words2.contains(w))
        .count();
    
    let similarity = common_words as f32 / words1.len().max(words2.len()) as f32;
    similarity > 0.3 // Threshold for similarity
}

async fn process_thread(http: &Http, channel_info: &ChannelInfo) -> Result<Vec<Conversation>> {
    let mut conversations = Vec::new();
    let mut messages = Vec::new();
    let mut last_id = None;

    // Fetch all messages in the thread
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
        // Sort messages by timestamp
        messages.sort_by_key(|m| m.timestamp);

        // Create a conversation from the thread
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

fn calculate_thread_quality(messages: &[Message]) -> f32 {
    let mut score = 1.0;

    // Increase score based on number of participants
    let unique_participants: std::collections::HashSet<_> = messages.iter()
        .map(|m| m.author.id)
        .collect();
    score *= 1.0 + unique_participants.len() as f32 / 5.0;

    // Increase score based on message length and content
    let avg_length: f32 = messages.iter()
        .map(|m| m.content.len())
        .sum::<usize>() as f32 / messages.len() as f32;
    score *= (1.0 + avg_length / 100.0).min(2.0);

    // Increase score for code blocks
    let code_blocks = messages.iter()
        .filter(|m| m.content.contains("```"))
        .count();
    score *= 1.0 + code_blocks as f32 / messages.len() as f32;

    score
}

async fn save_conversations(conversations: &[Conversation], path: &str, _is_final: bool) -> Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    
    let file_size = file.metadata()?.len();
    
    if file_size <= 3 {
        file.set_len(0)?;
        file.write_all(b"[\n")?;
    } else {
        file.seek(std::io::SeekFrom::End(-2))?;
        file.write_all(b",\n")?;
    }
    
    for (i, conv) in conversations.iter().enumerate() {
        if i > 0 {
            file.write_all(b",\n")?;
        }
        let json = serde_json::to_string_pretty(&conv)?;
        file.write_all(json.as_bytes())?;
    }
    
    file.write_all(b"\n]")?;
    Ok(())
}

async fn fetch_messages(http: &Http, channel_id: ChannelId, last_id: Option<MessageId>) -> Result<Vec<Message>> {
    let request = serenity::builder::GetMessages::default()
        .limit(100);
    
    let request = if let Some(id) = last_id {
        request.before(id)
    } else {
        request
    };
    
    channel_id
        .messages(http, request)
        .await
        .map_err(Into::into)
}

struct ChannelInfo {
    id: ChannelId,
    name: String,
    kind: ChannelType,
}

async fn process_channel(
    http: &Http,
    channel_info: &ChannelInfo,
    conversation_tx: mpsc::Sender<Conversation>,
    mut shutdown_rx: broadcast::Receiver<()>,
    display: Arc<tokio::sync::Mutex<Display>>,
) -> Result<ChannelStats> {
    let start_time = std::time::Instant::now();
    let mut stats = ChannelStats {
        name: channel_info.name.clone(),
        messages_processed: 0,
        conversations_found: 0,
        time_taken: Duration::default(),
    };

    {
        let mut display = display.lock().await;
        display.update_channel(
            channel_info.name.clone(),
            0,
            0,
            true,
        );
        display.update()?;
    }

    // If this is a thread channel, process it as a thread
    if channel_info.kind == ChannelType::PublicThread || channel_info.kind == ChannelType::PrivateThread {
        if let Ok(thread_conversations) = process_thread(http, channel_info).await {
            stats.conversations_found = thread_conversations.len();
            for conv in thread_conversations {
                if conversation_tx.send(conv).await.is_err() {
                    break;
                }
            }
            stats.time_taken = start_time.elapsed();
            
            {
                let mut display = display.lock().await;
                display.update_channel(
                    channel_info.name.clone(),
                    stats.messages_processed,
                    stats.conversations_found,
                    true,
                );
                display.update()?;
            }
            
            return Ok(stats);
        }
    }
    
    let mut last_id = None;
    let mut context = MessageContext::new(Duration::from_secs(300)); // 5 minute window
    let mut processed_messages = std::collections::HashSet::new();

    // Regular channel processing
    loop {
        tokio::select! {
            messages_result = fetch_messages(http, channel_info.id, last_id) => {
                let messages = messages_result?;
                if messages.is_empty() {
                    break;
                }

                let new_messages: Vec<_> = messages
                    .iter()
                    .filter(|m| !processed_messages.contains(&m.id))
                    .collect();

                stats.messages_processed += new_messages.len();
                for msg in &new_messages {
                    processed_messages.insert(msg.id);
                }
                
                {
                    let mut display = display.lock().await;
                    display.update_channel(
                        channel_info.name.clone(),
                        stats.messages_processed,
                        stats.conversations_found,
                        true,
                    );
                    display.update()?;
                }
                
                // Process messages for context-based conversations
                for msg in &new_messages {
                    context.add_message((*msg).clone());
                }
                
                let context_conversations = context.find_conversations();
                let new_conversations = context_conversations.len();
                if new_conversations > 0 {
                    stats.conversations_found += new_conversations;
                    
                    {
                        let mut display = display.lock().await;
                        display.update_channel(
                            channel_info.name.clone(),
                            stats.messages_processed,
                            stats.conversations_found,
                            true,
                        );
                        display.update()?;
                    }
                    
                    for conv in context_conversations {
                        if conversation_tx.send(conv).await.is_err() {
                            break;
                        }
                    }
                }
                
                // Process replies
                for msg in &new_messages {
                    if let Some(referenced) = &msg.referenced_message {
                        let reply_conversation = Conversation {
                            messages: vec![referenced.content.clone(), msg.content.clone()],
                            context_type: ConversationType::Reply,
                            participants: vec![referenced.author.name.clone(), msg.author.name.clone()],
                            timestamp: referenced.timestamp.unix_timestamp(),
                            quality_score: 1.0,
                        };
                        
                        stats.conversations_found += 1;
                        
                        {
                            let mut display = display.lock().await;
                            display.update_channel(
                                channel_info.name.clone(),
                                stats.messages_processed,
                                stats.conversations_found,
                                true,
                            );
                            display.update()?;
                        }
                        
                        if conversation_tx.send(reply_conversation).await.is_err() {
                            break;
                        }
                    }
                }

                let new_last_id = messages.last().map(|m| m.id);
                if new_last_id == last_id {
                    break;
                }
                last_id = new_last_id;

                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
            Ok(()) = shutdown_rx.recv() => {
                break;
            }
        }
    }

    stats.time_taken = start_time.elapsed();
    
    {
        let mut display = display.lock().await;
        display.channels_processed += 1;
        display.update_channel(
            channel_info.name.clone(),
            stats.messages_processed,
            stats.conversations_found,
            false,
        );
        display.update()?;
    }
    
    Ok(stats)
}

async fn process_channels(
    http: &Http,
    channels: Vec<ChannelInfo>,
    conversation_tx: mpsc::Sender<Conversation>,
    shutdown_tx: Arc<broadcast::Sender<()>>,
    display: Arc<tokio::sync::Mutex<Display>>,
) -> Vec<ChannelStats> {
    let semaphore = Arc::new(Semaphore::new(2)); // Limit to 2 concurrent workers
    let mut channel_stats = Vec::new();

    let results = stream::iter(channels)
        .map(|channel| {
            let conversation_tx = conversation_tx.clone();
            let shutdown_rx = shutdown_tx.subscribe();
            let display = Arc::clone(&display);
            let semaphore = Arc::clone(&semaphore);

            async move {
                let _permit = semaphore.acquire().await.unwrap();
                let result = process_channel(
                    http,
                    &channel,
                    conversation_tx,
                    shutdown_rx,
                    display,
                ).await;
                
                match result {
                    Ok(stats) => Some(stats),
                    Err(e) => {
                        eprintln!("Error processing channel {}: {}", channel.name, e);
                        None
                    }
                }
            }
        })
        .buffer_unordered(2)
        .collect::<Vec<_>>()
        .await;

    channel_stats.extend(results.into_iter().flatten());

    channel_stats
}

#[derive(PartialEq)]
enum ChannelChoice {
    Server,
    DirectMessages,
    Group,
}

async fn select_channels(http: &Http, channels: Vec<Channel>) -> Result<Vec<Channel>> {
    let mut selected_channels = Vec::new();
    let mut channel_map: HashMap<usize, &Channel> = HashMap::new();
    
    stdout().queue(SetForegroundColor(Color::Cyan))?;
    println!("\nAvailable channels:");
    stdout().queue(ResetColor)?;
    
    for (i, channel) in channels.iter().enumerate() {
        let (name, kind) = match channel {
            Channel::Private(c) => {
                if c.kind == ChannelType::Private {
                    (c.recipient.name.clone(), "DM")
                } else {
                    let name = if c.name().is_empty() {
                        "Unnamed Group".to_string()
                    } else {
                        c.name()
                    };
                    (name, "Group")
                }
            },
            _ => continue,
        };
        println!("{}. {} ({})", i + 1, name, kind);
        channel_map.insert(i + 1, channel);
    }
    
    stdout().queue(SetForegroundColor(Color::Cyan))?;
    println!("\nEnter channel numbers to scrape (comma-separated, e.g., '1,3,5' or 'all' for all channels):");
    stdout().queue(ResetColor)?;
    stdout().flush()?;
    
    crossterm::terminal::disable_raw_mode()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    crossterm::terminal::enable_raw_mode()?;
    
    let input = input.trim();
    
    if input.eq_ignore_ascii_case("all") {
        return Ok(channels);
    }
    
    for num_str in input.split(',') {
        if let Ok(num) = num_str.trim().parse::<usize>() {
            if let Some(channel) = channel_map.get(&num) {
                selected_channels.push((*channel).clone());
            }
        }
    }
    
    if selected_channels.is_empty() {
        return Err(anyhow::anyhow!("No valid channels selected"));
    }
    
    Ok(selected_channels)
}

#[tokio::main]
async fn main() -> Result<()> {
    let token = env::var("DISCORD_TOKEN").context("DISCORD_TOKEN not found in environment")?;
    let http = Arc::new(Http::new(&token));
    
    crossterm::terminal::enable_raw_mode()?;
    stdout().execute(crossterm::terminal::EnterAlternateScreen)?;
    stdout().execute(cursor::Hide)?;
    
    stdout().queue(SetForegroundColor(Color::Cyan))?;
    println!("Select channel type:");
    println!("1. Server");
    println!("2. Direct Messages");
    println!("3. Group");
    print!("Enter choice (1-3): ");
    stdout().queue(ResetColor)?;
    stdout().flush()?;
    
    crossterm::terminal::disable_raw_mode()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    crossterm::terminal::enable_raw_mode()?;
    
    let choice = match input.trim() {
        "1" => ChannelChoice::Server,
        "2" => ChannelChoice::DirectMessages,
        "3" => ChannelChoice::Group,
        _ => {
            stdout().execute(cursor::Show)?;
            stdout().execute(crossterm::terminal::LeaveAlternateScreen)?;
            crossterm::terminal::disable_raw_mode()?;
            return Err(anyhow::anyhow!("Invalid choice"));
        }
    };

    let (channels, name) = match choice {
        ChannelChoice::Server => {
            stdout().queue(SetForegroundColor(Color::Cyan))?;
            print!("Enter the Discord server (guild) ID: ");
            stdout().queue(ResetColor)?;
            stdout().flush()?;
            
            crossterm::terminal::disable_raw_mode()?;
            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            crossterm::terminal::enable_raw_mode()?;
            
            let guild_id = match input.trim().parse() {
                Ok(id) => GuildId::new(id),
                Err(e) => {
                    stdout().execute(cursor::Show)?;
                    stdout().execute(crossterm::terminal::LeaveAlternateScreen)?;
                    crossterm::terminal::disable_raw_mode()?;
                    return Err(anyhow::anyhow!("Invalid guild ID: {}", e));
                }
            };
            
            let guild = match http.get_guild(guild_id).await {
                Ok(guild) => guild,
                Err(e) => {
                    stdout().queue(SetForegroundColor(Color::Red))?;
                    writeln!(stdout(), "Failed to access server: {}", e)?;
                    writeln!(stdout(), "Please check:")?;
                    writeln!(stdout(), "1. The guild ID is correct")?;
                    writeln!(stdout(), "2. You are in the server")?;
                    writeln!(stdout(), "3. The token is valid")?;
                    stdout().queue(ResetColor)?;
                    return Ok(());
                }
            };

            let guild_channels = guild_id.channels(&http).await?;
            let channels: HashMap<ChannelId, Channel> = guild_channels
                .into_iter()
                .map(|(id, channel)| (id, Channel::Guild(channel)))
                .collect();
            (channels, guild.name)
        },
        ChannelChoice::DirectMessages | ChannelChoice::Group => {
            let user = http.get_current_user().await?;
            let dm_channels = http.get_user_dm_channels().await?;
            
            let filtered_channels: Vec<_> = dm_channels
                .into_iter()
                .filter(|channel| {
                    match choice {
                        ChannelChoice::DirectMessages => channel.kind == ChannelType::Private,
                        ChannelChoice::Group => channel.kind == ChannelType::GroupDm,
                        _ => false,
                    }
                })
                .map(Channel::Private)
                .collect();
            
            if filtered_channels.is_empty() {
                stdout().queue(SetForegroundColor(Color::Red))?;
                writeln!(stdout(), "No {} found!", 
                    if choice == ChannelChoice::DirectMessages { "DMs" } else { "groups" })?;
                stdout().queue(ResetColor)?;
                return Ok(());
            }
            
            let selected_channels = select_channels(&http, filtered_channels).await?;
            let mut channels_map = HashMap::new();
            
            for channel in selected_channels {
                if let Channel::Private(c) = channel {
                    channels_map.insert(c.id, Channel::Private(c));
                }
            }
            
            (channels_map, format!("DMs_{}", user.name))
        }
    };

    let mut stats = Stats::new(get_output_path(&name));
    
    let mut accessible_channels = Vec::new();
    for (channel_id, channel) in channels {
        if channel_id.messages(&http, serenity::builder::GetMessages::default().limit(1)).await.is_ok() {
            let (channel_name, kind) = match &channel {
                Channel::Guild(guild_channel) => (guild_channel.name.clone(), guild_channel.kind),
                Channel::Private(private_channel) => {
                    (private_channel.recipient.name.clone(), private_channel.kind)
                },
                _ => (String::from("unknown"), ChannelType::Private),
            };
            
            let channel_info = ChannelInfo {
                id: channel_id,
                name: channel_name,
                kind,
            };
            accessible_channels.push(channel_info);
        }
    }
    let text_channels = accessible_channels;

    let display = Arc::new(tokio::sync::Mutex::new(Display::new(text_channels.len())));
    let start_time = std::time::Instant::now();

    let (conversation_tx, mut conversation_rx) = mpsc::channel(1000);
    let (shutdown_tx, _) = broadcast::channel(1);
    let shutdown_tx = Arc::new(shutdown_tx);
    
    let shutdown_tx_clone = Arc::clone(&shutdown_tx);
    let display_clone = Arc::clone(&display);
    tokio::spawn(async move {
        if let Ok(()) = ctrl_c().await {
            let mut display = display_clone.lock().await;
            display.show_shutdown_message().ok();
            let _ = shutdown_tx_clone.send(());
        }
    });

    let display_clone = Arc::clone(&display);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut display = display_clone.lock().await;
            display.elapsed = start_time.elapsed();
            display.update()?;
        }
        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    });

    let display_clone = Arc::clone(&display);
    let output_path = stats.output_path.clone();
    tokio::spawn(async move {
        loop {
            if let Ok(metadata) = std::fs::metadata(&output_path) {
                let mut display = display_clone.lock().await;
                display.file_size = metadata.len();
                display.update().ok();
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });

    let process_handle = tokio::spawn({
        let conversation_tx = conversation_tx.clone();
        let shutdown_tx = Arc::clone(&shutdown_tx);
        let display = Arc::clone(&display);
        async move {
            process_channels(
                &http,
                text_channels,
                conversation_tx,
                shutdown_tx,
                display,
            ).await
        }
    });

    let mut conversations = Vec::new();
    let mut total_saved = 0;
    let mut shutdown_rx = shutdown_tx.subscribe();
    
    loop {
        tokio::select! {
            Some(conversation) = conversation_rx.recv() => {
                conversations.push(conversation);
                
                if conversations.len() >= 10 {
                    if let Ok(()) = save_conversations(&conversations, &stats.output_path, false).await {
                        total_saved += conversations.len();
                        let mut display = display.lock().await;
                        display.total_saved = total_saved;
                        display.update()?;
                        conversations.clear();
                    }
                }
            }
            Ok(()) = shutdown_rx.recv() => {
                break;
            }
            else => {
                break;
            }
        }
    }

    if let Ok(channel_stats) = process_handle.await {
        for channel_stat in channel_stats {
            stats.add_channel_stats(channel_stat);
        }
    }

    if !conversations.is_empty() {
        if let Ok(()) = save_conversations(&conversations, &stats.output_path, true).await {
            total_saved += conversations.len();
            let mut display = display.lock().await;
            display.total_saved = total_saved;
            display.update()?;
        }
    }
    
    stdout().execute(cursor::Show)?;
    stdout().execute(crossterm::terminal::LeaveAlternateScreen)?;
    crossterm::terminal::disable_raw_mode()?;
    
    stats.print_stats();
    Ok(())
}

