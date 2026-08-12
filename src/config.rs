use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::protocol::ServerMeta;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub meta: ServerMeta,
}

impl Config {
    pub fn new() -> Self {
        Self {
            meta: ServerMeta {
                name: "New Server".to_string(),
                description: String::new(),
            },
        }
    }

    pub async fn get() -> anyhow::Result<Self> {
        let config_path = PathBuf::from("config.json");

        if !config_path.exists() {
            let config = Config::new();
            tokio::fs::write(config_path, &serde_json::to_string_pretty(&config)?).await?;
            Ok(config)
        } else {
            Ok(serde_json::from_str(
                &tokio::fs::read_to_string(config_path).await?,
            )?)
        }
    }
}
