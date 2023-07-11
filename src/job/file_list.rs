// Copyright 2022 Zinc Labs Inc. and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use tokio::time;

use crate::common::file::scan_files_new;
use crate::common::infra::cluster;
use crate::common::infra::config::CONFIG;
use crate::common::infra::storage;
use crate::common::infra::wal;
use crate::common::meta::StreamType;

pub async fn run() -> Result<(), anyhow::Error> {
    if !cluster::is_ingester(&cluster::LOCAL_NODE_ROLE) {
        return Ok(()); // not an ingester, no need to init job
    }

    // create wal dir
    fs::create_dir_all(&CONFIG.common.data_wal_dir)?;

    let mut interval = time::interval(time::Duration::from_secs(CONFIG.limit.file_push_interval));
    interval.tick().await; // trigger the first run
    loop {
        interval.tick().await;
        if let Err(e) = move_file_list_to_storage().await {
            log::error!("Error moving file_list to remote: {}", e);
        }
    }
}

/*
 * upload compressed file_list to storage & delete moved files from local
 */
async fn move_file_list_to_storage() -> Result<(), anyhow::Error> {
    let pattern = format!("{}/file_list/*.json", &CONFIG.common.data_wal_dir);
    let files = scan_files_new(&pattern)?;

    for file in files {
        let local_file = file.to_string_lossy();
        let file_name = match file.file_name() {
            Some(base_name) => base_name.to_string_lossy(),
            None => {
                log::error!("[JOB] Failed to parse basename from {}", local_file);
                continue;
            }
        };
        // check the file is using for write
        if wal::check_in_use("", "", StreamType::Filelist, &file_name) {
            continue;
        }
        log::info!("[JOB] convert file_list: {}", local_file);

        match upload_file(&local_file, &file_name).await {
            Ok(_) => match fs::remove_file(&file) {
                Ok(_) => {}
                Err(e) => {
                    log::error!("[JOB] Failed to remove file_list {}: {}", local_file, e)
                }
            },
            Err(e) => log::error!("[JOB] Error while uploading file_list to storage {}", e),
        }
    }
    Ok(())
}

async fn upload_file(path_str: &str, file_key: &str) -> Result<(), anyhow::Error> {
    let mut file = fs::File::open(path_str).unwrap();
    let file_meta = file.metadata().unwrap();
    let file_size = file_meta.len();
    log::info!("[JOB] File_list upload begin: local: {}", path_str);
    if file_size == 0 {
        if let Err(e) = fs::remove_file(path_str) {
            log::error!("[JOB] File_list failed to remove: {}, {}", path_str, e);
        }
        return Err(anyhow::anyhow!("File_list is empty: {}", path_str));
    }

    let mut encoder = zstd::Encoder::new(Vec::new(), 3)?;
    std::io::copy(&mut file, &mut encoder)?;
    let compressed_bytes = encoder.finish().unwrap();

    let file_columns = file_key.split('_').collect::<Vec<&str>>();
    let new_file_key = format!(
        "file_list/{}/{}/{}/{}/{}.zst",
        file_columns[1], file_columns[2], file_columns[3], file_columns[4], file_columns[5]
    );

    let result = storage::put(&new_file_key, bytes::Bytes::from(compressed_bytes)).await;
    match result {
        Ok(_output) => {
            log::info!("[JOB] File_list upload succeeded: {}", new_file_key);
            Ok(())
        }
        Err(err) => {
            log::error!("[JOB] File_list upload error: {:?}", err);
            Err(anyhow::anyhow!(err))
        }
    }
}
