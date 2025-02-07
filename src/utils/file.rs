use std::{fs::{File, OpenOptions}, io::{Write, Seek}, path::Path};
use anyhow::Result;
use chrono::Utc;
use crate::models::Conversation;

pub fn get_output_path(server_name: &str) -> String {
    let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
    let base_path = format!("{}_conversations_{}.json", server_name, timestamp);
    
    let mut counter = 0;
    let mut path = base_path.clone();
    
    while Path::new(&path).exists() {
        counter += 1;
        path = format!("{}_conversations_{}_({}).json", server_name, timestamp, counter);
    }
    
    File::create(&path)
        .and_then(|mut f| f.write_all(b"[\n]"))
        .expect("Failed to create output file");
    
    path
}

pub async fn save_conversations(conversations: &[Conversation], path: &str, _is_final: bool) -> Result<()> {
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