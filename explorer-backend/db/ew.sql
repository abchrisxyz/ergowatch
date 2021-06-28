BEGIN;

DROP SCHEMA IF EXISTS ew CASCADE;
CREATE SCHEMA ew;

-------------------------------------------------------------------------------
-- Oracle Pools
-------------------------------------------------------------------------------
CREATE TABLE ew.oracle_pools
(
    id integer PRIMARY KEY,
    name text,
    datapoint_address text NOT NULL,
    epoch_prep_address text NOT NULL,
    live_epoch_address text NOT NULL,
    deposits_address text NOT NULL,
    pool_nft_id text NOT NULL,
    participant_token_id text NOT NULL,
    deviation_range integer NOT NULL,
    consensus_number integer NOT NULL
);


CREATE TABLE ew.oracle_pools_prep_boxes
(
    pool_id integer REFERENCES ew.oracle_pools(id),
    box_id text,
    PRIMARY KEY (pool_id, box_id)
);

INSERT INTO ew.oracle_pools
(
    id,
    name,
    datapoint_address,
    epoch_prep_address,
    live_epoch_address,
    deposits_address,
    pool_nft_id,
    participant_token_id,
    deviation_range,
    consensus_number
)
VALUES
(
    1,
    'ERGUSD',
    'AucEQEJ3Y5Uhmu4o8dsrHy28nRTgX5sVtXvjpMTqdMQzBR3uRVcvCFbv7SeGuPhQ16AXBP7XWdMShDdhRy4cayZgxHSkdAVuTiZRvj6WCfmhXJ4LY2E46CytRAnkiYubCdEroUUX2niMLhjNmDUn4KmXWSrKngrfGwHSaD8RJUMEp5AGADaChRU6kAnh9nstkDN3',
    'EfS5abyDe4vKFrJ48K5HnwTqa1ksn238bWFPe84bzVvCGvK1h2B7sgWLETtQuWwzVdBaoRZ1HcyzddrxLcsoM5YEy4UnqcLqMU1MDca1kLw9xbazAM6Awo9y6UVWTkQcS97mYkhkmx2Tewg3JntMgzfLWz5mACiEJEv7potayvk6awmLWS36sJMfXWgnEfNiqTyXNiPzt466cgot3GLcEsYXxKzLXyJ9EfvXpjzC2abTMzVSf1e17BHre4zZvDoAeTqr4igV3ubv2PtJjntvF2ibrDLmwwAyANEhw1yt8C8fCidkf3MAoPE6T53hX3Eb2mp3Xofmtrn4qVgmhNonnV8ekWZWvBTxYiNP8Vu5nc6RMDBv7P1c5rRc3tnDMRh2dUcDD7USyoB9YcvioMfAZGMNfLjWqgYu9Ygw2FokGBPThyWrKQ5nkLJvief1eQJg4wZXKdXWAR7VxwNftdZjPCHcmwn6ByRHZo9kb4Emv3rjfZE',
    'NTkuk55NdwCXkF1e2nCABxq7bHjtinX3wH13zYPZ6qYT71dCoZBe1gZkh9FAr7GeHo2EpFoibzpNQmoi89atUjKRrhZEYrTapdtXrWU4kq319oY7BEWmtmRU9cMohX69XMuxJjJP5hRM8WQLfFnffbjshhEP3ck9CKVEkFRw1JDYkqVke2JVqoMED5yxLVkScbBUiJJLWq9BSbE1JJmmreNVskmWNxWE6V7ksKPxFMoqh1SVePh3UWAaBgGQRZ7TWf4dTBF5KMVHmRXzmQqEu2Fz2yeSLy23sM3pfqa78VuvoFHnTFXYFFxn3DNttxwq3EU3Zv25SmgrWjLKiZjFcEcqGgH6DJ9FZ1DfucVtTXwyDJutY3ksUBaEStRxoUQyRu4EhDobixL3PUWRcxaRJ8JKA9b64ALErGepRHkAoVmS8DaE6VbroskyMuhkTo7LbrzhTyJbqKurEzoEfhYxus7bMpLTePgKcktgRRyB7MjVxjSpxWzZedvzbjzZaHLZLkWZESk1WtdM25My33wtVLNXiTvficEUbjA23sNd24pv1YQ72nY1aqUHa2',
    '4L1NEtpkMq6NeZhy2pk6omYvewovcHTm7CbxKm9djsbobAHSdDe6TVfmnW5THVpSHrG6rWovqv7838reswYE3UYkykWaNnhoyBGHFCdZvWqa2TVRtHiWcVaner6giUp55ZpELLuj9TtKePv6zrtMV5YE1o2kjrQEgGDoGHBGNuyx6ymXkSimcAQo1oD4f4tTcuBfWfp',
    '011d3364de07e5a26f0c4eef0852cddb387039a921b7154ef3cab22c6eda887f',
    '8c27dd9d8a35aac1e3167d58858c0a8b4059b277da790552e37eba22df9b9035',
    5,
    4
);

CREATE FUNCTION ew.oracle_pools_get_prep_boxes(_epoch_prep_address text, _live_epoch_address text)
    RETURNS TABLE (box_id text) AS
    $$
        WITH live_epoch_boxes AS (
            SELECT box_id
            FROM node_outputs
            WHERE main_chain
                AND address = _live_epoch_address
        )
        SELECT os.box_id
        FROM node_outputs os
        JOIN node_inputs ins ON ins.tx_id = os.tx_id
        JOIN live_epoch_boxes lbs ON lbs.box_id = ins.box_id
        WHERE os.main_chain AND ins.main_chain
            AND os.address = _epoch_prep_address
        ;
    $$
    LANGUAGE SQL IMMUTABLE;

CREATE PROCEDURE ew.oracle_pools_update_prep_boxes()
    LANGUAGE SQL
    AS $$
        INSERT INTO ew.oracle_pools_prep_boxes(pool_id, box_id)
        SELECT ops.id, pbs.box_id
        FROM ew.oracle_pools ops,
        LATERAL ew.oracle_pools_get_prep_boxes(ops.epoch_prep_address, ops.live_epoch_address) pbs
        ON CONFLICT DO NOTHING;
    $$;


CREATE TABLE ew.oracle_pools_oracle_address_hashes
(
    address text PRIMARY KEY,
    hash text UNIQUE NOT NULL
);

INSERT INTO ew.oracle_pools_oracle_address_hashes (address, hash) VALUES
    -- ERGUSD oracles
    ('9eh9WDsRAsujyFx4x7YeSoxrLCqmhuQihDwgsWVqEuXte7QJRCU', '0216e6cca588bed47a7630cba9d662a4be8a2e1991a45ed54ba64093e03dcd9013'),
    ('9em1ShUCkTa43fALGgzwKQ5znuXY2dMnnfHqq4bX3wSWytH11t7', '021fab219a58d2e1e8edfd3e2ad7cf09a35687246c084477db0bce5412f43acdbe'),
    ('9fckoJSnYpR38EkCzintbJoKaDwWN86wCmNdByiWyeQ22Hq5Sbj', '0290a0538b85768adb3dfc1fe6e4162adf43c6ae313ada0d1a7b71275de2b87364'),
    ('9fPRvaMYzBPotu6NGvZn4A6N4J2jDmRGs4Zwc9UhFFeSXgRJ8pS', '02725e8878d5198ca7f5853dddf35560ddab05ab0a26adae7e664b84162c9962e5'),
    ('9fQHnth8J6BgVNs9BQjxj5s4e5JGCjiH4fYTBA52ZWrMh6hz2si', '0274524ee849e4e45f58c46164ac609902bb374fc9375f097ee1af2ef1152ab9bf'),
    ('9fzRcctiWfzoJyqGtPWqoXPuxSmFw6zpnjtsQ1B6jSN514XqH4q', '02c1d434dac8765fc1269af82958d8aa350da53907096b35f7747cc372a7e6e69d'),
    ('9g4Kek6iWspXPAURU3zxT4RGoKvFdvqgxgkANisNFbvDwK1KoxW', '02caad8ef6771ad15ebb0a2aa9b7e84b9c48962976061d1af3e73767203d2f2bb1'),
    ('9gqhrGQN3eQmTFAW9J6KNX8ffUhe8BmTesE45b9nBmL7VJohhtY', '0331b99a9fcc7bceb0a238446cdab944402dd4b2e79f9dcab898ec3b46aea285c8'),
    ('9gXPZWxQZQpKsBCW2QCiBjJbhtghxEFtA9Ba6WygnKmrD4g2e8B', '03082348fd5d0c27d7aa89cd460a58fea2932f12147a04985e500bd9ad64695d58'),
    ('9eY1GWpJ7qwMkfVtnt8gZDnSvNW9VPqt15vePUmRrcr2zCRpGQ4', '020224bd8e95bb60ec042b5172d3cc9dd79f74f99700934010cda16642a50bd7af'),
    ('9hD4D5rAcTyMuw7eVSENfRBmdCZiz3cwmW8xSnoEvZ1H64rFGMn', '036234820eb840b9246442f022ed1ef15ac80f2c5ac28314bcd8ff682c2703128f');


-------------------------------------------------------------------------------
-- SigmaUSD
-------------------------------------------------------------------------------
CREATE TABLE ew.sigmausd_bank_boxes
(
    idx integer PRIMARY KEY,
    box_id text UNIQUE NOT NULL
);

CREATE PROCEDURE ew.sigmausd_update_bank_boxes()
    LANGUAGE SQL
    AS $$
        INSERT INTO ew.sigmausd_bank_boxes (idx, box_id)
            WITH RECURSIVE box_series(idx, box_id) AS (
                -- Start from latest known bank box
                SELECT idx, box_id
                FROM (
                    SELECT idx, box_id FROM ew.sigmausd_bank_boxes bbs
                    ORDER BY idx DESC
                    LIMIT 1
                ) last_bank_box

                UNION

                SELECT idx+1 AS idx
                    , os.box_id
                FROM box_series bxs
                JOIN node_inputs i ON i.box_id = bxs.box_id
                JOIN node_outputs os ON os.tx_id = i.tx_id AND os.address = 'MUbV38YgqHy7XbsoXWF5z7EZm524Ybdwe5p9WDrbhruZRtehkRPT92imXer2eTkjwPDfboa1pR3zb3deVKVq3H7Xt98qcTqLuSBSbHb7izzo5jphEpcnqyKJ2xhmpNPVvmtbdJNdvdopPrHHDBbAGGeW7XYTQwEeoRfosXzcDtiGgw97b2aqjTsNFmZk7khBEQywjYfmoDc9nUCJMZ3vbSspnYo3LarLe55mh2Np8MNJqUN9APA6XkhZCrTTDRZb1B4krgFY1sVMswg2ceqguZRvC9pqt3tUUxmSnB24N6dowfVJKhLXwHPbrkHViBv1AKAJTmEaQW2DN1fRmD9ypXxZk8GXmYtxTtrj3BiunQ4qzUCu1eGzxSREjpkFSi2ATLSSDqUwxtRz639sHM6Lav4axoJNPCHbY8pvuBKUxgnGRex8LEGM8DeEJwaJCaoy8dBw9Lz49nq5mSsXLeoC4xpTUmp47Bh7GAZtwkaNreCu74m9rcZ8Di4w1cmdsiK1NWuDh9pJ2Bv7u3EfcurHFVqCkT3P86JUbKnXeNxCypfrWsFuYNKYqmjsix82g9vWcGMmAcu5nagxD4iET86iE2tMMfZZ5vqZNvntQswJyQqv2Wc6MTh4jQx1q2qJZCQe4QdEK63meTGbZNNKMctHQbp3gRkZYNrBtxQyVtNLR8xEY8zGp85GeQKbb37vqLXxRpGiigAdMe3XZA4hhYPmAAU5hpSMYaRAjtvvMT3bNiHRACGrfjvSsEG9G2zY5in2YWz5X9zXQLGTYRsQ4uNFkYoQRCBdjNxGv6R58Xq74zCgt19TxYZ87gPWxkXpWwTaHogG1eps8WXt8QzwJ9rVx6Vu9a5GjtcGsQxHovWmYixgBU8X9fPNJ9UQhYyAWbjtRSuVBtDAmoV1gCBEPwnYVP5GCGhCocbwoYhZkZjFZy6ws4uxVLid3FxuvhWvQrVEDYp7WRvGXbNdCbcSXnbeTrPMey1WPaXX'
            )
            SELECT idx, box_id
            FROM box_series
            ON CONFLICT DO NOTHING;
    $$;

-- First SigmaUSD bank box
INSERT INTO ew.sigmausd_bank_boxes(idx, box_id)
VALUES (0, '96dec7ca2812ee8bb6c0e1969aef383f2ee4f79510c587e83f3ac59a0aff1678');

/*
SigmaUSD history
----------------
The history is split across three tables:

    - Bank tx history (reserves, circulating supplies, accumulated feed)
      Changes with bank txs only.

    - Cumulative bank tx history (total fees, ...)

    - Bank ratio's history (liabilities, equity, SigUSD and SigRSV prices)
      Change with bank txs and oracle postings.

Tx history only includes heights where at least 1 bank tx is mined.
Tx history can have more than 1 record (tx) per height.
Tx and cumulative tx history is split across two tables for easier updates.

Ratio history only includes heights where:
    a) oracle price is posted, or
    b) one or more bank txs are mined
Ration history has only 1 record per height (only the last tx matters).
*/

-- SigmaUSD bank transactions
CREATE TABLE ew.sigmausd_history_transactions (
    bank_box_idx integer PRIMARY KEY REFERENCES ew.sigmausd_bank_boxes(idx),
    height integer,
    reserves numeric,
    circ_sigusd numeric,
    circ_sigrsv bigint,
    d_erg numeric,
    d_usd numeric,
    d_rsv bigint,
    fee numeric,
    CHECK(fee >= 0)
);


-- SigmaUSD bank cumulative stats
CREATE TABLE ew.sigmausd_history_transactions_cumulative (
    bank_box_idx integer PRIMARY KEY REFERENCES ew.sigmausd_bank_boxes(idx),
    cum_usd_fee numeric,
    cum_rsv_fee numeric,
    cum_usd_erg_in numeric,
    cum_usd_erg_out numeric,
    cum_rsv_erg_in numeric,
    cum_rsv_erg_out numeric,
    f_usd numeric,
    f_rsv numeric,
    f_fee numeric
);


CREATE TABLE ew.sigmausd_history_ratios (
    height integer PRIMARY KEY,
    oracle_price numeric,
    rsv_price numeric,
    liabs numeric,
    equity numeric
);

-- Inserts new records in sigmausd history tables
CREATE PROCEDURE ew.sigmausd_update_history()
    AS $$
        --------------------------------
        -- 1. Update transaction history
        --------------------------------
        WITH last_processed_bank_box AS (
            -- Limit to new boxes only,
            -- but include last processed box so we can derive
            -- changes for first new box as well.
            SELECT COALESCE(MAX(bank_box_idx), -1) AS idx
            FROM ew.sigmausd_history_transactions
        ), bank_boxes AS (
            SELECT bbs.idx
                , nos.creation_height
                , nos.settlement_height
                , ROUND(nos.value / 1000000000., 9) AS reserves
                , ROUND((nos.additional_registers #>> '{R4,renderedValue}')::numeric / 100, 2) AS circ_sigusd
                , (nos.additional_registers #>> '{R5,renderedValue}')::bigint AS circ_sigrsv
            FROM ew.sigmausd_bank_boxes bbs
            JOIN node_outputs nos ON nos.box_id = bbs.box_id AND nos.main_chain
            -- Limit to new boxes only,
            -- but include last processed box so we can derive
            -- changes for first new box as well.
            WHERE bbs.idx >= (SELECT idx FROM last_processed_bank_box)
        ), ergusd_oracle_pool_price_boxes AS (
            -- Retrieve oracle price postings (prep boxes).
            SELECT nos.settlement_height
                , 1 / (nos.additional_registers #>> '{R4,renderedValue}')::numeric * 1000000000 AS price
            FROM ew.oracle_pools_prep_boxes bxs
            JOIN node_outputs nos ON nos.box_id = bxs.box_id
            WHERE nos.main_chain
                AND pool_id = 1 -- TODO: Avoid hard coded values.
                -- Oldest oracle box that we need is the one that existed when
                -- the oldest bank box was created.
                AND nos.settlement_height >= (SELECT MIN(creation_height) FROM bank_boxes)
        ), combined_bank_boxes_and_oracle_prices AS (
            SELECT bbs.*
                -- Highest oracle height under or at bank tx creation height
                , (
                    SELECT settlement_height
                    FROM ergusd_oracle_pool_price_boxes
                    WHERE settlement_height <= bbs.creation_height
                    ORDER BY settlement_height DESC LIMIT 1
                ) AS oracle_height
                , (
                    SELECT price
                    FROM ergusd_oracle_pool_price_boxes
                    WHERE settlement_height <= bbs.creation_height
                    ORDER BY settlement_height DESC LIMIT 1
                ) AS oracle_price
            FROM bank_boxes bbs
        ), add_diffs AS (
            SELECT a.*
                , a.reserves - b.reserves AS d_erg
                , a.circ_sigusd - b.circ_sigusd AS d_usd
                , a.circ_sigrsv - b.circ_sigrsv AS d_rsv
            FROM combined_bank_boxes_and_oracle_prices a
            LEFT JOIN combined_bank_boxes_and_oracle_prices b ON b.idx = a.idx - 1
        ), add_liabs AS (
            SELECT *
                , (circ_sigusd - d_usd) / oracle_price AS old_liabs
                , circ_sigusd / oracle_price AS liabs
            FROM add_diffs
        ), add_equity AS (
            SELECT *
                , (reserves - d_erg) - old_liabs AS old_equity
                , reserves - liabs AS equity
            FROM add_liabs
        ), add_rsv_price AS (
            SELECT *
                , CASE WHEN (circ_sigrsv - d_rsv) > 0 THEN old_equity / (circ_sigrsv - d_rsv) ELSE 0.001 END AS old_rsv_price
                , CASE WHEN circ_sigrsv > 0 THEN equity / circ_sigrsv ELSE 0.001 END AS rsv_price
            FROM add_equity
        ), add_fee AS (
            SELECT *
                , d_erg - (d_usd / oracle_price) - (d_rsv * old_rsv_price)  AS fee
            FROM add_rsv_price
        )
        INSERT INTO ew.sigmausd_history_transactions
        (
            bank_box_idx,
            height,
            reserves,
            circ_sigusd,
            circ_sigrsv,
            d_erg,
            d_usd,
            d_rsv,
            fee
        )
        SELECT idx
            , settlement_height
            , reserves
            , circ_sigusd
            , circ_sigrsv
            , d_erg
            , d_usd
            , d_rsv
            , fee
        FROM add_fee
        -- Ignore first idx as already processed
        ORDER BY idx OFFSET 1;

        -------------------------------------------
        -- 2. Update cumulative transaction history
        -------------------------------------------
        WITH cumsum_for_new_boxes AS (
            SELECT th.bank_box_idx
                , reserves
                , SUM(fee * (d_usd <> 0)::int) OVER w AS cum_usd_fee
                , SUM(fee * (d_rsv <> 0)::int) OVER w AS cum_rsv_fee

                , SUM(GREATEST(0,d_erg) * (d_usd > 0)::int) OVER w AS cum_usd_erg_in
                , SUM(GREATEST(0,-d_erg) * (d_usd < 0)::int) OVER w AS cum_usd_erg_out

                , SUM(GREATEST(0,d_erg) * (d_rsv > 0)::int) OVER w AS cum_rsv_erg_in
                , SUM(GREATEST(0,-d_erg) * (d_rsv < 0)::int) OVER w AS cum_rsv_erg_out
            FROM ew.sigmausd_history_transactions th
            LEFT JOIN ew.sigmausd_history_transactions_cumulative ch ON ch.bank_box_idx = th.bank_box_idx
            WHERE ch.bank_box_idx IS NULL
            WINDOW w AS (ORDER BY th.bank_box_idx)
        ), adjusted_cumsums AS (
            -- Add cumsum form last record.
            -- Coalesce to 0 for initial run.
            SELECT nbs.bank_box_idx
                , nbs.reserves
                , nbs.cum_usd_fee + COALESCE(lcr.cum_usd_fee, 0) AS cum_usd_fee
                , nbs.cum_rsv_fee + COALESCE(lcr.cum_rsv_fee, 0) AS cum_rsv_fee
                , nbs.cum_usd_erg_in + COALESCE(lcr.cum_usd_erg_in, 0) AS cum_usd_erg_in
                , nbs.cum_usd_erg_out + COALESCE(lcr.cum_usd_erg_out, 0) AS cum_usd_erg_out
                , nbs.cum_rsv_erg_in + COALESCE(lcr.cum_rsv_erg_in, 0) AS cum_rsv_erg_in
                , nbs.cum_rsv_erg_out + COALESCE(lcr.cum_rsv_erg_out, 0) AS cum_rsv_erg_out
            FROM cumsum_for_new_boxes nbs
            -- Last cumulative record
            LEFT JOIN (SELECT * FROM ew.sigmausd_history_transactions_cumulative ORDER BY bank_box_idx DESC LIMIT 1) lcr ON TRUE
        ), add_reserve_fractions AS (
            SELECT *
                , GREATEST(0, cum_usd_erg_in - cum_usd_erg_out - cum_usd_fee) / reserves AS f_usd
                , (cum_rsv_erg_in - cum_rsv_erg_out - cum_rsv_fee + 0.001 + LEAST(0, cum_usd_erg_in - cum_usd_erg_out - cum_usd_fee)) / reserves AS f_rsv
                , (cum_usd_fee + cum_rsv_fee) / reserves AS f_fee
            FROM adjusted_cumsums
        )
        INSERT INTO ew.sigmausd_history_transactions_cumulative
        (
            bank_box_idx,
            cum_usd_fee,
            cum_rsv_fee,
            cum_usd_erg_in,
            cum_usd_erg_out,
            cum_rsv_erg_in,
            cum_rsv_erg_out,
            f_usd,
            f_rsv,
            f_fee
        )
        SELECT bank_box_idx
            , cum_usd_fee
            , cum_rsv_fee
            , cum_usd_erg_in
            , cum_usd_erg_out
            , cum_rsv_erg_in
            , cum_rsv_erg_out
            , f_usd
            , f_rsv
            , f_fee
        FROM add_reserve_fractions
        ORDER BY bank_box_idx;

        ----------------------------
        -- 3. Update ratio's history
        ----------------------------
        WITH new_bank_transactions AS (
            SELECT bank_box_idx,
                height,
                reserves,
                circ_sigusd,
                circ_sigrsv
            FROM ew.sigmausd_history_transactions
            -- Limit to new heights only,
            WHERE height > (SELECT COALESCE(MAX(height), -1) FROM ew.sigmausd_history_ratios)
                -- Only keep last box within each block (txs within a block have same timestamp)
                AND (height, bank_box_idx) IN (
                        SELECT height, MAX(bank_box_idx)
                        FROM ew.sigmausd_history_transactions
                        WHERE height > (SELECT COALESCE(MAX(height), -1) FROM ew.sigmausd_history_ratios)
                        GROUP BY 1
                    )
        ), ergusd_oracle_pool_price_boxes AS (
            -- Retrieve oracle price postings (prep boxes).
            SELECT nos.settlement_height AS height
                , 1 / (nos.additional_registers #>> '{R4,renderedValue}')::numeric * 1000000000 AS price
            FROM ew.oracle_pools_prep_boxes bxs
            JOIN node_outputs nos ON nos.box_id = bxs.box_id
            WHERE nos.main_chain
                AND pool_id = 1 -- TODO: Avoid hard coded values.
                -- Oldest oracle box that we need is the one that existed when
                -- the oldest bank box was created.
                AND nos.settlement_height >= (SELECT COALESCE(MAX(height), -1) FROM ew.sigmausd_history_ratios)
        ), combined_oracle_prices_and_bank_transactions AS (
            SELECT op.height
                , op.price AS oracle_price
                , bt.reserves
                , bt.circ_sigusd
                , bt.circ_sigrsv
            FROM ergusd_oracle_pool_price_boxes op
            -- Join each oracle box to latest bank tx at time of oracle box.
            -- This will also discard oracle bank boxes prior to first bank tx.
            JOIN new_bank_transactions bt ON bt.height <= op.height
            WHERE (op.height, bt.height) IN (
                SELECT op.height
                    , MAX(bt.height)
                FROM ergusd_oracle_pool_price_boxes op
                LEFT JOIN new_bank_transactions bt ON bt.height <= op.height
                GROUP BY 1
            )
        ), add_liabs AS (
            SELECT *
                , circ_sigusd / oracle_price AS liabs
            FROM combined_oracle_prices_and_bank_transactions
        ), add_equity AS (
            SELECT *
                , reserves - liabs AS equity
            FROM add_liabs
        ), add_rsv_price AS (
            SELECT *
                , CASE WHEN circ_sigrsv > 0 THEN equity / circ_sigrsv ELSE 0.001 END AS rsv_price
            FROM add_equity
        )
        INSERT INTO ew.sigmausd_history_ratios
        (
            height,
            oracle_price,
            rsv_price,
            liabs,
            equity
        )
        SELECT height
            , oracle_price
            , rsv_price
            , liabs
            , equity
        FROM add_rsv_price
        ORDER BY height;

    $$
    LANGUAGE SQL;

-------------------------------------------------------------------------------
-- Initialize
-------------------------------------------------------------------------------
-- CALL ew.sigmausd_update_bank_boxes();
-- CALL ew.oracle_pools_update_prep_boxes();
-- CALL ew.sigmausd_update_history();

COMMIT;