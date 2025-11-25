CREATE DATABASE IF NOT EXISTS pajohesh_btc;

CREATE TABLE IF NOT EXISTS pajohesh_btc.wallet_info (
    address String,
    balance String,
    nonce UInt64,
    last_seen_block UInt64,
    type String,
    defi String,
    sensitive UInt8
) ENGINE = ReplacingMergeTree()
ORDER BY address;

CREATE TABLE IF NOT EXISTS pajohesh_btc.transactions (
    hash String,
    block_number UInt64,
    from_addr String,
    to_addr String,
    value String
) ENGINE = MergeTree()
ORDER BY block_number;

CREATE TABLE IF NOT EXISTS pajohesh_btc.owner_info (
    address String,
    person_name String,
    person_id UInt16,
    personal_id UInt16
) ENGINE = ReplacingMergeTree()
ORDER BY address;

