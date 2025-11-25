// for running Codes :
// clickhouse-client --user=mehran --password='mehran.crypto9' --multiquery < init_database.sql

//drop database: DROP DATABASE IF EXISTS pajohesh_btc;

use clickhouse::{Client, Row};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio;
use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt};

static BASE_URL: &str = "https://blockstream.info/api";

// DB Structs
// -----------------------------------------------------

#[derive(Row, Serialize, Deserialize)]
struct WalletRow {
    address: String,
    balance: String,
    nonce: u64,
    last_seen_block: u64,
    #[serde(rename = "type")]
    wallet_type: String,
    defi: String,
    sensitive: u8,
}

#[derive(Row, Serialize, Deserialize)]
struct TransactionRow {
    hash: String,
    block_number: u64,
    from_addr: String,
    to_addr: String,
    value: String,
}

#[derive(Row, Serialize, Deserialize)]
struct OwnerRow {
    address: String,
    person_name: String,
    person_id: u16,
    personal_id: u16,
}

// Blockstream API Structs
// -----------------------------------------------------

#[derive(Deserialize)]
struct BlockTx {
    txid: String,
    vin: Vec<Vin>,
    vout: Vec<Vout>,
}

#[derive(Deserialize)]
struct Vin {
    prevout: Option<Vout>,
}

#[derive(Deserialize)]
struct Vout {
    scriptpubkey_address: Option<String>,
    value: u64,
}

// API Helper Functions
// -----------------------------------------------------

async fn get_block_hash_by_height(height: u64) -> Result<String> {
    let url = format!("{}/block-height/{}", BASE_URL, height);
    Ok(reqwest::get(url).await?.text().await?.trim().to_string())
}

async fn get_block_txs(block_hash: &str) -> Result<Vec<BlockTx>> {
    let url = format!("{}/block/{}/txs", BASE_URL, block_hash);
    Ok(reqwest::get(url).await?.json::<Vec<BlockTx>>().await?)
}

fn btc_from_sats(sats: u64) -> f64 {
    sats as f64 / 100_000_000.0
}

#[derive(Deserialize)]
struct UTXO { value: u64 }

async fn get_wallet_balance(address: &str) -> Result<f64> {
    let url = format!("{}/address/{}/utxo", BASE_URL, address);
    let utxos = reqwest::get(url).await?.json::<Vec<UTXO>>().await?;
    Ok(btc_from_sats(utxos.iter().map(|u| u.value).sum()))
}

// MAIN
// -----------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let clickhouse = Arc::new(
        Client::default()
            .with_url("http://localhost:8123")
            .with_user("mehran")
            .with_password("mehran.crypto9")
            .with_database("pajohesh_btc")
    );

    let start_block: u64 = 831000;
    let total_txs = 200;
    let mut tx_count = 0;

    for block_height in start_block..start_block + 10 {
        if tx_count >= total_txs { break; }

        let block_hash = get_block_hash_by_height(block_height).await?;
        let txs = get_block_txs(&block_hash).await?;

        let mut tasks = FuturesUnordered::new();

        for tx in txs {
            if tx_count >= total_txs { break; }

            let clickhouse = clickhouse.clone();
            tasks.push(tokio::spawn(async move {
                process_tx(&clickhouse, block_height, tx).await
            }));

            tx_count += 1;
            println!("Tx: {}", tx_count);
        }

        while let Some(res) = tasks.next().await {
            res??;
        }
    }

    println!("Done: {} BTC txs fetched", tx_count);
    Ok(())
}


// Process Transaction
// -----------------------------------------------------

async fn process_tx(
    clickhouse: &Arc<Client>,
    block_number: u64,
    tx: BlockTx,
) -> Result<()> {

    let from_addr = tx.vin
        .iter()
        .filter_map(|v| v.prevout.as_ref()?.scriptpubkey_address.clone())
        .next()
        .unwrap_or_default();

    let to_addr = tx.vout
        .iter()
        .filter_map(|v| v.scriptpubkey_address.clone())
        .next()
        .unwrap_or_default();

    let total_value_sats: u64 = tx.vout.iter().map(|v| v.value).sum();

    let tx_row = TransactionRow {
        hash: tx.txid.clone(),
        block_number,
        from_addr: from_addr.clone(),
        to_addr: to_addr.clone(),
        value: btc_from_sats(total_value_sats).to_string(),
    };

    let mut insert_tx = clickhouse.insert::<TransactionRow>("transactions").await?;
    insert_tx.write(&tx_row).await?;
    insert_tx.end().await?;

    if !from_addr.is_empty() {
        save_wallet_clickhouse(clickhouse, &from_addr, block_number).await?;
    }
    if !to_addr.is_empty() {
        save_wallet_clickhouse(clickhouse, &to_addr, block_number).await?;
    }

    Ok(())
}

// Save wallet + owner_info
// -----------------------------------------------------

async fn save_wallet_clickhouse(
    clickhouse: &Arc<Client>,
    address: &String,
    block_number: u64,
) -> Result<()> {

    let balance = get_wallet_balance(address).await?;

    let wallet_row = WalletRow {
        address: address.clone(),
        balance: balance.to_string(),
        nonce: 0,                       // BTC: no nonce
        last_seen_block: block_number,
        wallet_type: "wallet".into(),
        defi: "".into(),
        sensitive: 0,
    };

    let owner = OwnerRow {
        address: address.clone(),
        person_name: "".into(),
        person_id: 0,
        personal_id: 0,
    };

    let mut insert_wallet = clickhouse.insert::<WalletRow>("wallet_info").await?;
    insert_wallet.write(&wallet_row).await?;
    insert_wallet.end().await?;

    let mut insert_owner = clickhouse.insert::<OwnerRow>("owner_info").await?;
    insert_owner.write(&owner).await?;
    insert_owner.end().await?;

    Ok(())
}
