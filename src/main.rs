use std::{env, fs::{File, OpenOptions}, io::{Write, stdin, BufRead, Seek}, sync::Arc, path::Path};
use anyhow::{Result, Context as _};
use serenity::{
    all::{ChannelType, Http, Message, MessageId}, 
    model::id::{ChannelId, GuildId},
};
use tokio::sync::{Mutex, mpsc, broadcast};
use futures::stream::{self, StreamExt};
use chrono::Utc;
use serde_json::json;
use tokio::signal::ctrl_c;
use futures::future::join_all;

const CONCURRENT_REQUESTS: usize = 5;
const SAVE_FREQUENCY: usize = 100;

struct MessageScraper {
    conversations: Arc<Mutex<Vec<[String; 2]>>>,
    target_guild: GuildId,
    total_messages: Arc<Mutex<usize>>,
    output_path: String,
}

fn get_output_path() -> String {
    let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
    let base_path = format!("discord_conversations_{}.json", timestamp);
    
    let mut counter = 0;
    let mut path = base_path.clone();
    
    while Path::new(&path).exists() {
        counter += 1;
        path = format!("discord_conversations_{}_({}).json", timestamp, counter);
    }
    
    // Create the file with initial array brackets
    File::create(&path)
        .and_then(|mut f| f.write_all(b"[\n]"))
        .expect("Failed to create output file");
    
    path
}

async fn save_conversations(conversations: &[[String; 2]], path: &str, is_final: bool) -> Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    
    // Get file size to check if we need to add a comma
    let file_size = file.metadata()?.len();
    
    // If file is empty or just contains "[\n]", write initial content
    if file_size <= 3 {
        file.set_len(0)?;
        file.write_all(b"[\n")?;
    } else {
        // Seek to just before the closing bracket
        file.seek(std::io::SeekFrom::End(-2))?;
        // Add comma if there's already content
        file.write_all(b",\n")?;
    }
    
    // Write conversations
    for (i, conv) in conversations.iter().enumerate() {
        if i > 0 {
            file.write_all(b",\n")?;
        }
        let json = serde_json::to_string_pretty(&conv)?;
        file.write_all(json.as_bytes())?;
    }
    
    // Close the array
    file.write_all(b"\n]")?;
    Ok(())
}

async fn fetch_messages(http: &Http, channel_id: ChannelId, last_id: Option<MessageId>) -> Result<Vec<Message>> {
    let request = serenity::builder::GetMessages::default();
    let _ = request.limit(100);
    if let Some(id) = last_id {
        let _ = request.before(id);
    }
    
    channel_id
        .messages(http, request)
        .await
        .map_err(Into::into)
}

async fn process_channel(
    http: Arc<Http>,
    channel_id: ChannelId,
    channel_name: String,
    tx: mpsc::Sender<(Message, Option<Message>)>,
    mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
    println!("\n🔍 Starting to scrape channel: {} ({})", channel_name, channel_id);

    // Get the first batch to find message ranges
    let initial_messages = fetch_messages(&http, channel_id, None).await?;
    if initial_messages.is_empty() {
        return Ok(());
    }

    // Process initial messages
    for msg in &initial_messages {
        if let Some(referenced) = &msg.referenced_message {
            if tx.send((msg.clone(), Some(*referenced.clone()))).await.is_err() {
                return Ok(());
            }
        }
    }

    // Get the last message ID from initial fetch
    let mut last_id = initial_messages.last().map(|m| m.id);
    let mut handles = Vec::new();
    
    // Spawn 5 concurrent tasks
    for task_id in 0..5 {
        let http = Arc::clone(&http);
        let tx = tx.clone();
        let channel_name = channel_name.clone();
        let mut shutdown = shutdown.resubscribe();
        let mut current_id = last_id;
        
        handles.push(tokio::spawn(async move {
            let mut messages_processed = 0;
            
            loop {
                tokio::select! {
                    result = fetch_messages(&http, channel_id, current_id) => {
                        match result {
                            Ok(messages) => {
                                if messages.is_empty() {
                                    println!("Task {} finished after processing {} messages", task_id, messages_processed);
                                    break;
                                }

                                messages_processed += messages.len();

                                // Process messages
                                for msg in &messages {
                                    if let Some(referenced) = &msg.referenced_message {
                                        if tx.send((msg.clone(), Some(*referenced.clone()))).await.is_err() {
                                            return;
                                        }
                                    }
                                }

                                current_id = messages.last().map(|m| m.id);
                                println!("📥 Task {} fetched {} messages from {} (Total: {})", 
                                    task_id,
                                    messages.len(), 
                                    channel_name,
                                    messages_processed);

                                // Add a small delay to avoid rate limits
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                            }
                            Err(e) => {
                                println!("⚠️ Error fetching messages in {} (Task {}): {}", channel_name, task_id, e);
                                // Add longer delay on error
                                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                            }
                        }
                    }
                    _ = shutdown.recv() => {
                        println!("🛑 Shutting down task {} for channel {}", task_id, channel_name);
                        return;
                    }
                }
            }
        }));
    }

    // Wait for all tasks to complete
    for handle in handles {
        let _ = handle.await;
    }
    
    println!("✅ Finished channel: {}", channel_name);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let token = env::var("DISCORD_TOKEN").context("DISCORD_TOKEN not found in environment")?;
    
    let http = Arc::new(Http::new(&token));
    println!("🔑 Using user token mode");
    
    println!("Enter the Discord server (guild) ID:");
    let mut input = String::new();
    stdin().lock().read_line(&mut input)?;
    
    let guild_id = input.trim().parse().context("Invalid guild ID")?;
    let guild_id = GuildId::new(guild_id);
    
    // Validate guild access
    let guild = match http.get_guild(guild_id).await {
        Ok(guild) => {
            println!("✅ Successfully found server: {}", guild.name);
            guild
        },
        Err(e) => {
            println!("❌ Failed to access server: {}", e);
            println!("Please check if:\n1. The guild ID is correct\n2. You are in the server\n3. The token is valid");
            return Ok(());
        }
    };
    
    println!("🚀 Starting message scraper for guild ID: {}", guild_id);
    
    let scraper = Arc::new(MessageScraper {
        conversations: Arc::new(Mutex::new(Vec::new())),
        target_guild: guild_id,
        total_messages: Arc::new(Mutex::new(0)),
        output_path: get_output_path(),
    });

    let channels = guild_id.channels(&http).await?;
    println!("📊 Found {} channels in total", channels.len());
    
    let text_channels: Vec<_> = channels.into_iter()
        .filter(|(_, c)| c.kind == ChannelType::Text)
        .collect();
    println!("📝 Found {} text channels to scrape", text_channels.len());

    // Create channel for collecting messages
    let (tx, mut rx) = mpsc::channel::<(Message, Option<Message>)>(1000);
    
    // Create a broadcast channel for shutdown signal
    let (shutdown_tx, _) = broadcast::channel(1);
    let shutdown_tx = Arc::new(shutdown_tx);
    
    // Process channels concurrently
    let mut handles = Vec::new();
    
    for (channel_id, channel) in text_channels {
        let http = Arc::clone(&http);
        let tx = tx.clone();
        let channel_name = channel.name.clone();
        let shutdown_rx = shutdown_tx.subscribe();
        
        handles.push(tokio::spawn(async move {
            if let Err(e) = process_channel(http, channel_id, channel_name, tx, shutdown_rx).await {
                eprintln!("Error processing channel {}: {}", channel_id, e);
            }
        }));
    }

    // Create a task for processing received messages
    let process_handle = tokio::spawn({
        let mut rx = rx;
        let mut shutdown_rx = shutdown_tx.subscribe();
        let output_path = scraper.output_path.clone();
        
        async move {
            let mut message_count = 0;
            let mut conversations = Vec::new();
            let mut total_saved = 0;

            loop {
                tokio::select! {
                    Some((msg, referenced)) = rx.recv() => {
                        message_count += 1;
                        
                        if let Some(ref_msg) = referenced {
                            conversations.push([
                                ref_msg.content,
                                msg.content,
                            ]);
                            
                            // Save when we have 100 conversations or more
                            if conversations.len() >= 100 {
                                if let Ok(()) = save_conversations(&conversations, &output_path, false).await {
                                    total_saved += conversations.len();
                                    println!("💾 Saved {} conversations (Total saved: {})", 
                                        conversations.len(), total_saved);
                                    conversations.clear();
                                }
                            }
                        }
                    }
                    Ok(()) = shutdown_rx.recv() => {
                        println!("🛑 Interrupted, saving remaining conversations...");
                        if !conversations.is_empty() {
                            if let Ok(()) = save_conversations(&conversations, &output_path, true).await {
                                total_saved += conversations.len();
                                println!("💾 Saved {} remaining conversations (Total saved: {})", 
                                    conversations.len(), total_saved);
                            }
                        }
                        return (message_count, total_saved);
                    }
                    else => {
                        // Save any remaining conversations before exiting
                        if !conversations.is_empty() {
                            if let Ok(()) = save_conversations(&conversations, &output_path, true).await {
                                total_saved += conversations.len();
                                println!("💾 Final save: {} conversations (Total saved: {})", 
                                    conversations.len(), total_saved);
                            }
                        }
                        return (message_count, total_saved);
                    }
                }
            }
        }
    });

    // Spawn Ctrl+C handler
    let shutdown_tx_clone = Arc::clone(&shutdown_tx);
    tokio::spawn(async move {
        if let Ok(()) = ctrl_c().await {
            println!("\n⚠️ Received Ctrl+C, gracefully shutting down...");
            let _ = shutdown_tx_clone.send(());
        }
    });

    // Wait for all channel tasks to complete
    for handle in handles {
        if let Err(e) = handle.await {
            eprintln!("Error joining channel task: {}", e);
        }
    }

    // Drop the sender so the receiver knows when to stop
    drop(tx);

    // Wait for the processing task to complete and get the stats
    let (message_count, total_saved) = process_handle.await.unwrap_or((0, 0));
    
    println!("\n🎉 Scraping completed!");
    println!("📊 Stats:");
    println!("- Total messages processed: {}", message_count);
    println!("- Total conversations saved: {}", total_saved);
    println!("💾 Conversations saved to: {}", scraper.output_path);
    
    Ok(())
}
