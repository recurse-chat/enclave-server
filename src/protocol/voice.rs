use std::sync::Arc;

use ed25519_dalek::VerifyingKey;

use crate::{protocol::ClientMethod, server::Server};

pub async fn join(
    server: &Arc<Server>,
    verifying_key: VerifyingKey,
    socket: &Arc<crate::ws::EnclaveWebSocket>,
    channel_id: String,
) -> anyhow::Result<()> {
    {
        let pin = rand::random::<u64>() % (1 << 53);

        server
            .voice_pins
            .lock()
            .await
            .insert(pin, (verifying_key, channel_id.clone()));

        socket
            .send(&ClientMethod::JoinVoice {
                channel_id: channel_id.clone(),
                pin,
            })
            .await?;
    }

    server
        .broadcast(&ClientMethod::UserJoinedVoice {
            channel_id,
            pubkey: crate::crypto::to_string(&verifying_key),
        })
        .await?;

    Ok(())
}

pub async fn leave(server: &Arc<Server>, verifying_key: VerifyingKey) -> anyhow::Result<()> {
    let Some(channel_id) = ({
        let clients = server.clients.lock().await;

        let Some(client) = clients.get(&verifying_key) else {
            return Ok(());
        };

        client.voice.lock().await.take().map(|v| v.channel_id)
    }) else {
        return Ok(());
    };

    server
        .voice_pins
        .lock()
        .await
        .retain(|_, v| v.0 != verifying_key);

    server
        .broadcast(&ClientMethod::UserLeftVoice {
            channel_id,
            pubkey: crate::crypto::to_string(&verifying_key),
        })
        .await?;

    Ok(())
}
