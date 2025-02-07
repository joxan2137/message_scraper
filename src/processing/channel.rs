use anyhow::Result;
use serenity::all::{Http, Message, MessageId, ChannelId, ChannelType};
use tokio::{sync::{mpsc, broadcast}, time::Duration};
use std::sync::Arc;
use futures::stream::{self, StreamExt};
use tokio::sync::Semaphore;

use crate::models::{ChannelInfo, ChannelStats, Conversation, ConversationType};
use crate::display::Display;
use super::thread::process_thread;

async fn process_channel_segment(
    http: &Http,
    channel_info: &ChannelInfo,
    conversation_tx: mpsc::Sender<Conversation>,
    mut shutdown_rx: broadcast::Receiver<()>,
    display: Arc<tokio::sync::Mutex<Display>>,
    start_id: Option<MessageId>,
    stats: Arc<tokio::sync::Mutex<ChannelStats>>,
) -> Result<Option<MessageId>> {
    let mut last_id = start_id;
    let mut processed_messages = std::collections::HashSet::new();

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

                {
                    let mut stats = stats.lock().await;
                    stats.messages_processed += new_messages.len();
                }

                for msg in &new_messages {
                    processed_messages.insert(msg.id);
                }
                
                {
                    let stats = stats.lock().await;
                    let mut display = display.lock().await;
                    display.update_channel(
                        channel_info.name.clone(),
                        stats.messages_processed,
                        stats.conversations_found,
                        true,
                    );
                    display.update()?;
                }
                
                // Process replies
                for msg in &new_messages {
                    if let Some(referenced) = &msg.referenced_message {
                        // Only count as conversation if from different authors
                        if msg.author.id != referenced.author.id {
                            let reply_conversation = Conversation {
                                messages: vec![referenced.content.clone(), msg.content.clone()],
                                context_type: ConversationType::Reply,
                                participants: vec![referenced.author.name.clone(), msg.author.name.clone()],
                                timestamp: referenced.timestamp.unix_timestamp(),
                                quality_score: 1.0,
                            };
                            
                            {
                                let mut stats = stats.lock().await;
                                stats.conversations_found += 1;
                            }
                            
                            {
                                let stats = stats.lock().await;
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
                }

                let new_last_id = messages.last().map(|m| m.id);
                if new_last_id == last_id {
                    break;
                }
                last_id = new_last_id;

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(()) = shutdown_rx.recv() => {
                break;
            }
        }
    }

    Ok(last_id)
}

pub async fn process_channel(
    http: &Http,
    channel_info: &ChannelInfo,
    conversation_tx: mpsc::Sender<Conversation>,
    mut shutdown_rx: broadcast::Receiver<()>,
    display: Arc<tokio::sync::Mutex<Display>>,
) -> Result<ChannelStats> {
    let start_time = std::time::Instant::now();
    let stats = Arc::new(tokio::sync::Mutex::new(ChannelStats {
        name: channel_info.name.clone(),
        messages_processed: 0,
        conversations_found: 0,
        time_taken: Duration::default(),
    }));

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

    if channel_info.kind == ChannelType::PublicThread || channel_info.kind == ChannelType::PrivateThread {
        if let Ok(thread_conversations) = process_thread(http, channel_info).await {
            let mut stats_guard = stats.lock().await;
            stats_guard.conversations_found = thread_conversations.len();
            for conv in thread_conversations {
                if conversation_tx.send(conv).await.is_err() {
                    break;
                }
            }
            stats_guard.time_taken = start_time.elapsed();
            
            {
                let mut display = display.lock().await;
                display.update_channel(
                    channel_info.name.clone(),
                    stats_guard.messages_processed,
                    stats_guard.conversations_found,
                    true,
                );
                display.update()?;
            }
            
            let result = stats_guard.clone();
            drop(stats_guard);
            return Ok(result);
        }
    }

    // Create two workers for this channel
    let worker1 = process_channel_segment(
        http,
        channel_info,
        conversation_tx.clone(),
        shutdown_rx.resubscribe(),
        Arc::clone(&display),
        None,
        Arc::clone(&stats),
    );

    let worker2 = process_channel_segment(
        http,
        channel_info,
        conversation_tx,
        shutdown_rx,
        Arc::clone(&display),
        None,
        Arc::clone(&stats),
    );

    // Run both workers concurrently
    let (res1, res2) = tokio::join!(worker1, worker2);
    res1?;
    res2?;

    let stats_guard = stats.lock().await;
    let mut result = stats_guard.clone();
    result.time_taken = start_time.elapsed();
    drop(stats_guard);
    
    {
        let mut display = display.lock().await;
        display.channels_processed += 1;
        display.update_channel(
            channel_info.name.clone(),
            result.messages_processed,
            result.conversations_found,
            false,
        );
        display.update()?;
    }
    
    Ok(result)
}

pub async fn process_channels(
    http: &Http,
    channels: Vec<ChannelInfo>,
    conversation_tx: mpsc::Sender<Conversation>,
    shutdown_tx: Arc<broadcast::Sender<()>>,
    display: Arc<tokio::sync::Mutex<Display>>,
) -> Vec<ChannelStats> {
    let mut channel_stats = Vec::new();
    let channel_count = channels.len();

    // Process all channels concurrently
    let results = stream::iter(channels)
        .map(|channel| {
            let conversation_tx = conversation_tx.clone();
            let shutdown_rx = shutdown_tx.subscribe();
            let display = Arc::clone(&display);

            async move {
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
        .buffer_unordered(channel_count) // Process all channels concurrently
        .collect::<Vec<_>>()
        .await;

    channel_stats.extend(results.into_iter().flatten());
    channel_stats
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