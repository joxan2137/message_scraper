use std::{collections::HashMap, io::{stdout, Write}};
use anyhow::Result;
use serenity::all::{Channel, ChannelType, Http};
use crossterm::{
    style::{Color, SetForegroundColor, ResetColor},
    QueueableCommand,
};

pub async fn select_channels(http: &Http, channels: Vec<Channel>) -> Result<Vec<Channel>> {
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