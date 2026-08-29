use std::sync::Arc;

use ed25519_dalek::VerifyingKey;

use crate::{protocol::ClientMethod, server::Server};

pub async fn join(
    server: &Arc<Server>,
    verifying_key: VerifyingKey,
    socket: &Arc<crate::ws::EnclaveWebSocket>,
    channel_id: String,
) -> anyhow::Result<()> {
    let user = server
        .sessions
        .get(&verifying_key)
        .await
        .ok_or_else(|| anyhow::anyhow!("Not connected"))?;

    let pin = server.voice.join(verifying_key, user, &channel_id).await;

    socket
        .send(&ClientMethod::JoinVoice {
            channel_id: channel_id.clone(),
            pin,
        })
        .await?;

    server
        .sessions
        .broadcast(&ClientMethod::UserJoinedVoice {
            channel_id,
            pubkey: crate::crypto::to_string(&verifying_key),
        })
        .await?;

    Ok(())
}

pub async fn leave(server: &Arc<Server>, verifying_key: VerifyingKey) -> anyhow::Result<()> {
    let Some(channel_id) = server.voice.remove(verifying_key).await else {
        return Ok(());
    };

    server
        .sessions
        .broadcast(&ClientMethod::UserLeftVoice {
            channel_id,
            pubkey: crate::crypto::to_string(&verifying_key),
        })
        .await?;

    Ok(())
}