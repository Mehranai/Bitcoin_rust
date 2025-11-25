// for running Codes :
// clickhouse-client --user=mehran --password='mehran.crypto9' --multiquery < init_database.sql

//drop database: DROP DATABASE IF EXISTS pajohesh;

use clickhouse::{Client, Row};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio;
use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt};

static BASE_URL: &str = "https://blockstream.info/api";

// ---------------------------------------------
// DB Structs
// ---------------------------------------------

#[derive(Row, Serialize, Deserialize)]
struct WalletRow {
    address: String,
    balance: f64,
    last_seen_block: u64,
}

#[derive(Row, Serialize, Deserialize)]
struct OwnerRow {
    address: String,
    person_name: String,
    person_id: u16,
    personal_id: u16,
}

#[derive(Row, Serialize, Deserialize)]
struct TransactionRow {
    txid: String,
    block_number: u64,
    vin_addresses: Vec<String>,
    vout_addresses: Vec<String>,
    value: f64,
}

// ---------------------------------------------
// API Structs
// ---------------------------------------------

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

// ---------------------------------------------
// API Functions (Blockstream)
// ---------------------------------------------


async fn get_block_hash_by_height(height: u64) -> Result<String> {
    let url = format!("{}/block-height/{}", BASE_URL, height);
    let resp = reqwest::get(&url).await?.text().await?;
    Ok(resp.trim().to_string())
}

async fn get_block_txs(block_hash: &str) -> Result<Vec<BlockTx>> {
    let url = format!("{}/block/{}/txs", BASE_URL, block_hash);
    let resp = reqwest::get(&url).await?.json::<Vec<BlockTx>>().await?;
    Ok(resp)
}

fn btc_from_sats(sats: u64) -> f64 {
    sats as f64 / 100_000_000.0
}

#[derive(Deserialize)]
struct UTXO {
    value: u64,
}

// Get wallet balance (Using UTx0)
async fn get_wallet_balance(address: &str) -> Result<f64> {
    let url = format!("{}/address/{}/utxo", BASE_URL, address);
    let list = reqwest::get(&url).await?.json::<Vec<UTXO>>().await?;

    let total_sats: u64 = list.iter().map(|u| u.value).sum();
    Ok(btc_from_sats(total_sats))
}

// ---------------------------------------------
// Main Logic
// ---------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let clickhouse = Arc::new(
        Client::default()
            .with_url("http://localhost:8123")
            .with_user("mehran")
            .with_password("mehran.crypto9")
            .with_database("pajohesh"),
    );

    let start_block: u64 = 830_000;
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
            println!("Tx #{}", tx_count);
        }

        while let Some(res) = tasks.next().await {
            res??;
        }
    }

    println!("Done: {} Bitcoin transactions fetched", tx_count);
    Ok(())
}

// ---------------------------------------------
// Process Transaction
// ---------------------------------------------

async fn process_tx(
    clickhouse: &Arc<Client>,
    block_number: u64,
    tx: BlockTx,
) -> Result<()> {

    let vin_addrs: Vec<String> = tx.vin.iter()
        .filter_map(|v| v.prevout.as_ref())
        .filter_map(|p| p.scriptpubkey_address.clone())
        .collect();

    let vout_addrs: Vec<String> = tx.vout.iter()
        .filter_map(|v| v.scriptpubkey_address.clone())
        .collect();

    let total_value_sats: u64 = tx.vout.iter().map(|v| v.value).sum();

    let row = TransactionRow {
        txid: tx.txid.clone(),
        block_number,
        vin_addresses: vin_addrs.clone(),
        vout_addresses: vout_addrs.clone(),
        value: btc_from_sats(total_value_sats),
    };

    let mut insert = clickhouse.insert::<TransactionRow>("btc_transactions").await?;
    insert.write(&row).await?;
    insert.end().await?;

    // Save wallets (vin + vout)
    for addr in vin_addrs.into_iter().chain(vout_addrs.into_iter()) {
        save_wallet_clickhouse(clickhouse, &addr, block_number).await?;
    }

    Ok(())
}

// ---------------------------------------------
// Save Wallet + Owner (like Ethereum version)
// ---------------------------------------------

async fn save_wallet_clickhouse(
    clickhouse: &Arc<Client>,
    address: &String,
    block_number: u64,
) -> Result<()> {

    let balance = get_wallet_balance(address).await?;

    let row = WalletRow {
        address: address.clone(),
        balance,
        last_seen_block: block_number,
    };

    let owner = OwnerRow {
        address: address.clone(),
        person_name: "".to_string(),
        person_id: 0,
        personal_id: 0,
    };

    let mut insert_wallet = clickhouse.insert::<WalletRow>("wallet_info").await?;
    insert_wallet.write(&row).await?;
    insert_wallet.end().await?;

    let mut insert_owner = clickhouse.insert::<OwnerRow>("owner_info").await?;
    insert_owner.write(&owner).await?;
    insert_owner.end().await?;

    Ok(())
}
