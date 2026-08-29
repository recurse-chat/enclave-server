use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use ed25519_dalek::{Signer, VerifyingKey};

use crate::{server::Server, types::ClientMeta, ws::EnclaveWebSocket};

use super::*;

pub async fn initialize(
    server: &Arc<Server>,
    socket: &EnclaveWebSocket,
) -> anyhow::Result<(VerifyingKey, ClientMeta)> {
    let Some(ServerMethod::Initialize {
        public_key: public_key_string,
        signature,

        timestamp,
        hostname,
    }) = socket.read().await?
    else {
        log::warn!("Client sent the wrong method during initialization");

        socket
            .send(&ClientMethod::Error {
                error: Cow::Borrowed("Initialization required"),
            })
            .await?;

        return Err(anyhow::anyhow!(
            "Failed to initialize: Client sent the wrong method"
        ));
    };

    let server_timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;

    if server_timestamp.saturating_sub(timestamp) > 2000 {
        log::warn!(
            "Client timestamp wasn't correct (server {server_timestamp}, client {timestamp})"
        );

        socket
            .send(&ClientMethod::Error {
                error: Cow::Borrowed(
                    "Timestamp doesn't match, make sure it's in secs and is (<= 2secs)",
                ),
            })
            .await?;

        return Err(anyhow::anyhow!("Client tampstamp wasn't correct"));
    }

    if !server.config.hostnames.contains(&hostname) {
        log::warn!("Client sent invalid hostname: {hostname}");

        socket.send(
            &ClientMethod::Error {
                error: Cow::Owned(format!("Invalid Hostname, to avoid man-in-the-middle attacks, please use the correct hostname(s): {}", server.config.hostnames.clone().into_iter().collect::<Vec<_>>().join(", "))),
            },
        )
        .await?;

        return Err(anyhow::anyhow!("Client's hostname wasn't correct"));
    }

    let Ok(public_key) = crate::crypto::from_string(&public_key_string) else {
        log::warn!("Client sent an invalid public key: {public_key_string}");

        socket
            .send(&ClientMethod::Error {
                error: Cow::Borrowed("Invalid public key"),
            })
            .await?;

        return Err(anyhow::anyhow!("Invalid public key"));
    };

    if public_key
        .verify_strict(
            format!("{timestamp}@{hostname}").as_bytes(),
            &crate::crypto::from_string_sig(&signature)?,
        )
        .is_err()
    {
        log::warn!("Client signature verification failed for {public_key_string}");

        socket
            .send(&ClientMethod::Error {
                error: Cow::Borrowed("Invalid signature"),
            })
            .await?;

        return Err(anyhow::anyhow!("Invalid signature"));
    }

    log::info!("Client authenticated: {public_key_string} (hostname: {hostname})");

    {
        socket
            .send(&ClientMethod::Initialized {
                public_key: crate::crypto::to_string(&server.identity.key.verifying_key()),
                signature: crate::crypto::to_string_sig(
                    &server.identity.key.sign(
                        format!("{server_timestamp}@{hostname}@{public_key_string}").as_bytes(),
                    ),
                ),

                timestamp: server_timestamp,
                hostname,
            })
            .await?;
    }

    let Some(ServerMethod::Meta(meta)) = socket.read().await? else {
        log::warn!("Client {public_key_string} didn't send meta during initialization");

        socket
            .send(&ClientMethod::Error {
                error: Cow::Borrowed("Expected meta"),
            })
            .await?;

        return Err(anyhow::anyhow!(
            "Expected meta, client called another method"
        ));
    };

    for (pubkey, conn) in server.voice.participants.lock().await.iter() {
        socket
            .send(&ClientMethod::UserJoinedVoice {
                channel_id: conn.channel_id.clone(),
                pubkey: crate::crypto::to_string(pubkey),
            })
            .await?;
    }

    Ok((public_key, meta))
}
