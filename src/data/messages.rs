use anyhow::Result;
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

pub struct MessageStore {
    data_dir: PathBuf,
    connections: Mutex<HashMap<String, Connection>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageData {
    pub content: String,
    pub timestamp: u64,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMessage {
    pub id: String,
    pub author: String,
    #[serde(flatten)]
    pub data: MessageData,
}

impl MessageStore {
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&data_dir)?;
        Ok(Self {
            data_dir,
            connections: Mutex::new(HashMap::new()),
        })
    }

    /// Opens (or reuses an already-open) connection for a channel,
    /// creating the schema if this is the first time.
    fn with_channel<T>(
        &self,
        channel_id: &str,
        f: impl FnOnce(&Connection) -> Result<T>,
    ) -> Result<T> {
        let mut conns = self.connections.lock().unwrap();

        if !conns.contains_key(channel_id) {
            let path = self.data_dir.join(format!("{channel_id}.db"));
            let conn = Connection::open(&path)?;

            conn.execute(
                "CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    author_pubkey TEXT NOT NULL,
                    content TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    signature TEXT NOT NULL,
                )",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)",
                [],
            )?;

            conns.insert(channel_id.to_string(), conn);
        }

        let conn = conns.get(channel_id).unwrap();
        f(conn)
    }

    pub fn insert_message(&self, channel_id: &str, msg: &StoredMessage) -> Result<()> {
        self.with_channel(channel_id, |conn| {
            conn.execute(
                "INSERT INTO messages (id, author_pubkey, content, timestamp, signature)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    msg.id,
                    msg.author,
                    msg.data.content,
                    msg.data.timestamp,
                    msg.data.signature,
                ],
            )?;
            Ok(())
        })
    }

    /// Fetches the most recent `limit` messages, oldest-first (ready to render top-to-bottom).
    pub fn get_recent_messages(&self, channel_id: &str, limit: u32) -> Result<Vec<StoredMessage>> {
        self.with_channel(channel_id, |conn| {
            let mut stmt = conn.prepare(
                "SELECT id, author_pubkey, content, timestamp, signature
                 FROM messages
                 ORDER BY timestamp DESC
                 LIMIT ?1",
            )?;

            let rows = stmt.query_map(params![limit], |row| {
                Ok(StoredMessage {
                    id: row.get(0)?,
                    author: row.get(1)?,
                    data: MessageData {
                        content: row.get(2)?,
                        timestamp: row.get(3)?,
                        signature: row.get(4)?,
                    },
                })
            })?;

            let mut messages: Vec<StoredMessage> = rows.collect::<Result<_, _>>()?;
            messages.reverse(); // DESC query, then flip to oldest-first for display
            Ok(messages)
        })
    }
}
