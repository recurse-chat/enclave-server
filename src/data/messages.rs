use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};
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
    pub is_edited: bool,
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
                    is_edited INTEGER NOT NULL DEFAULT 0
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
                "INSERT INTO messages (id, author_pubkey, content, timestamp, signature, is_edited)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    msg.id,
                    msg.author,
                    msg.data.content,
                    msg.data.timestamp,
                    msg.data.signature,
                    msg.is_edited,
                ],
            )?;
            Ok(())
        })
    }

    /// Fetches a single message by id, or `None` if it doesn't exist.
    pub fn get_message(&self, channel_id: &str, id: &str) -> Result<Option<StoredMessage>> {
        self.with_channel(channel_id, |conn| {
            conn.query_row(
                "SELECT id, author_pubkey, content, timestamp, signature, is_edited
                 FROM messages WHERE id = ?1",
                params![id],
                |row| {
                    Ok(StoredMessage {
                        id: row.get(0)?,
                        author: row.get(1)?,
                        data: MessageData {
                            content: row.get(2)?,
                            timestamp: row.get(3)?,
                            signature: row.get(4)?,
                        },
                        is_edited: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
        })
    }

    /// Fetches the most recent `limit` messages by the `offset`.
    pub fn get_recent_messages(
        &self,
        channel_id: &str,
        limit: u32,
        chunk: u32,
    ) -> Result<Vec<StoredMessage>> {
        self.with_channel(channel_id, |conn| {
            let offset = chunk * limit;

            let mut stmt = conn.prepare(
                "SELECT id, author_pubkey, content, timestamp, signature, is_edited
                 FROM messages
                 ORDER BY timestamp DESC
                 LIMIT ?1 OFFSET ?2",
            )?;

            let rows = stmt.query_map(params![limit, offset], |row| {
                Ok(StoredMessage {
                    id: row.get(0)?,
                    author: row.get(1)?,
                    data: MessageData {
                        content: row.get(2)?,
                        timestamp: row.get(3)?,
                        signature: row.get(4)?,
                    },
                    is_edited: row.get(5)?,
                })
            })?;

            let mut messages: Vec<StoredMessage> = rows.collect::<Result<_, _>>()?;
            messages.reverse();
            Ok(messages)
        })
    }

    /// Updates a message's content and signature, and marks it as edited.
    /// Returns an error if no row with that id exists.
    pub fn update_message(
        &self,
        channel_id: &str,
        id: &str,
        new_content: &str,
        new_signature: &str,
    ) -> Result<()> {
        self.with_channel(channel_id, |conn| {
            let updated = conn.execute(
                "UPDATE messages
                 SET content = ?1, signature = ?2, is_edited = 1
                 WHERE id = ?3",
                params![new_content, new_signature, id],
            )?;

            if updated == 0 {
                anyhow::bail!("Message not found");
            }

            Ok(())
        })
    }

    /// Deletes a message. Returns an error if no row with that id existed.
    pub fn delete_message(&self, channel_id: &str, id: &str) -> Result<()> {
        self.with_channel(channel_id, |conn| {
            let deleted = conn.execute("DELETE FROM messages WHERE id = ?1", params![id])?;

            if deleted == 0 {
                anyhow::bail!("Message not found");
            }

            Ok(())
        })
    }
}
