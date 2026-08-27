use std::{collections::HashSet, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::types::{Channel, ChannelKind, ServerMeta};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub meta: ServerMeta,
    pub port: u16,
    pub public_hostname: String,
    pub hostnames: HashSet<String>,
}

impl Config {
    pub fn new() -> Self {
        Self {
            meta: ServerMeta {
                name: "New Server".to_string(),
                description: String::new(),

                channels: vec![
                    Channel {
                        id: "text-channels".to_string(),
                        name: "Text Channels".to_string(),

                        data: ChannelKind::Category {
                            channels: vec![Channel {
                                id: "general".to_string(),
                                name: "General".to_string(),
                                data: ChannelKind::Text,
                            }],
                        },
                    },
                    Channel {
                        id: "voice-channels".to_string(),
                        name: "Voice Channels".to_string(),

                        data: ChannelKind::Category {
                            channels: vec![Channel {
                                id: "vc-1".to_string(),
                                name: "VC 1".to_string(),
                                data: ChannelKind::Voice { max_users: 255 },
                            }],
                        },
                    },
                ],
            },

            port: 3415,

            public_hostname: "localhost:3415".to_string(),

            hostnames: HashSet::from_iter(["localhost:3415".to_string()]),
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
