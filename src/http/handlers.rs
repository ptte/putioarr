use crate::{
    // downloader::DownloadStatus,
    services::putio::{self, PutIOTransfer},
    services::transmission::{TransmissionRequest, TransmissionTorrent},
    AppData,
};
use actix_web::web;
use anyhow::Result;
use base64::Engine;
use colored::Colorize;
use lava_torrent::torrent::v1::Torrent;
use log::info;
use magnet_url::Magnet;
use serde_json::json;

pub(crate) async fn handle_torrent_add(
    api_token: &str,
    payload: &web::Json<TransmissionRequest>,
    app_data: &web::Data<AppData>,
) -> Result<Option<serde_json::Value>> {
    let arguments = payload.arguments.as_ref().unwrap().as_object().unwrap();

    // Extract labels sent by Radarr/Sonarr
    let labels: Vec<String> = arguments
        .get("labels")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let hash: Option<String> = if arguments.contains_key("metainfo") {
        // .torrent files
        let b64 = arguments["metainfo"].as_str().unwrap();
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        putio::upload_file(api_token, &bytes).await?;

        match Torrent::read_from_bytes(bytes) {
            Ok(t) => {
                info!(
                    "{}: torrent uploaded",
                    format!("[ffff: {}]", t.name).magenta()
                );
                Some(t.info_hash().to_lowercase())
            }
            Err(_) => {
                info!("New torrent uploaded");
                None
            }
        }
    } else {
        // Magnet links
        let magnet_url = arguments["filename"].as_str().unwrap();
        putio::add_transfer(api_token, magnet_url).await?;
        match Magnet::new(magnet_url) {
            Ok(m) if m.dn.is_some() => {
                info!(
                    "{}: magnet link uploaded",
                    format!("[ffff: {}]", urldecode::decode(m.dn.clone().unwrap())).magenta()
                );
                m.xt
                    .as_deref()
                    .and_then(|xt| xt.strip_prefix("urn:btih:"))
                    .map(|h| h.to_lowercase())
            }
            Ok(m) => {
                info!("unknown magnet link uploaded");
                m.xt
                    .as_deref()
                    .and_then(|xt| xt.strip_prefix("urn:btih:"))
                    .map(|h| h.to_lowercase())
            }
            Err(_) => {
                info!("unknown magnet link uploaded");
                None
            }
        }
    };

    if let Some(h) = hash {
        if !labels.is_empty() {
            let mut store = app_data.labels_store.write().unwrap();
            store.insert(h, labels);
            if let Ok(data) = serde_json::to_string(&*store) {
                let _ = std::fs::write(&app_data.labels_file, data);
            }
        }
    }

    Ok(None)
}

pub(crate) async fn handle_torrent_remove(
    api_token: &str,
    payload: &web::Json<TransmissionRequest>,
) -> Option<serde_json::Value> {
    // TODO: leanup all the unwrap stuff
    let arguments = payload.arguments.as_ref().unwrap().as_object().unwrap();
    let ids: Vec<&str> = arguments
        .get("ids")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap())
        .collect();
    let delete_local_data = arguments
        .get("delete-local-data")
        .unwrap()
        .as_bool()
        .unwrap();

    let putio_transfers: Vec<PutIOTransfer> = putio::list_transfers(api_token)
        .await
        .unwrap()
        .transfers
        .into_iter()
        .filter(|t| ids.contains(&t.hash.clone().unwrap_or(String::from("no_hash")).as_str()))
        .collect();

    for t in putio_transfers {
        putio::remove_transfer(api_token, t.id).await.unwrap();

        if t.userfile_exists && delete_local_data {
            putio::delete_file(api_token, t.file_id.unwrap())
                .await
                .unwrap();
        }
    }

    None
}

pub(crate) async fn handle_torrent_get(
    api_token: &str,
    app_data: &web::Data<AppData>,
) -> Option<serde_json::Value> {
    let transfers = putio::list_transfers(api_token).await.unwrap().transfers;

    let labels_snapshot: std::collections::HashMap<String, Vec<String>> = {
        let store = app_data.labels_store.read().unwrap();
        store.clone()
    };

    let transmission_transfers = transfers.into_iter().map(|t| {
        let labels_snapshot = labels_snapshot.clone();
        let download_dir = app_data.config.download_directory.clone();
        let api_token = api_token.to_string();
        let file_id = t.file_id;
        async move {
            let mut tt: TransmissionTorrent = t.into();
            tt.download_dir = download_dir;
            // Use the put.io file name instead of the transfer name so that Radarr/Sonarr
            // compute the correct outputPath. Transfer names often include site prefixes
            // (e.g. "www.UIndex.org - ...") that differ from the actual downloaded file name.
            if let Some(fid) = file_id {
                if let Ok(resp) = putio::list_files(&api_token, fid).await {
                    tt.name = resp.parent.name;
                }
            }
            if let Some(labels) = labels_snapshot.get(&tt.hash_string.to_lowercase()) {
                tt.labels = labels.clone();
            }
            tt
        }
    });
    let transmission_transfers: Vec<TransmissionTorrent> =
        futures::future::join_all(transmission_transfers).await;

    let torrents = json!(transmission_transfers);

    let mut arguments = serde_json::Map::new();
    arguments.insert(String::from("torrents"), torrents);

    Some(json!(arguments))
}
