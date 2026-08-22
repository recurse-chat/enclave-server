use std::{collections::HashMap, path::PathBuf};

use rusqlite::{Connection, params};
use tokio::sync::Mutex;

use crate::types::ClientMeta;

pub struct UserMetaStore {
    conn: Mutex<Connection>,
}

impl UserMetaStore {
    pub fn new(path: PathBuf) -> anyhow::Result<Self> {
        let conn = Connection::open(&path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS users (
                pubkey TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                avatar TEXT
            )",
            [],
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub async fn get_users(
        &self,
        pubkeys: &[String],
    ) -> anyhow::Result<HashMap<String, ClientMeta>> {
        let conn = self.conn.lock().await;
        let placeholders = pubkeys.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let query = format!(
            "SELECT pubkey, display_name, avatar FROM users WHERE pubkey IN ({placeholders})"
        );

        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map(rusqlite::params_from_iter(pubkeys), |row| {
            Ok((
                row.get::<_, String>(0)?,
                ClientMeta {
                    display_name: row.get(1)?,
                    avatar: row.get(2)?,
                },
            ))
        })?;

        rows.collect::<Result<HashMap<_, _>, _>>()
            .map_err(Into::into)
    }

    pub async fn upsert_user(&self, pubkey: &str, meta: &ClientMeta) -> anyhow::Result<()> {
        let conn = self.conn.lock().await;
        conn.execute(
            "INSERT INTO users (pubkey, display_name, avatar) VALUES (?1, ?2, ?3)
             ON CONFLICT(pubkey) DO UPDATE SET display_name = ?2, avatar = ?3",
            params![pubkey, meta.display_name, meta.avatar],
        )?;
        Ok(())
    }
}
