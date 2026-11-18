-- Source-of-truth wallet table. Debezium reads its changes from the WAL (wal_level=logical).
CREATE TABLE wallets (
  wallet_id     TEXT PRIMARY KEY,
  user_id       TEXT        NOT NULL,
  balance_kurus BIGINT      NOT NULL CHECK (balance_kurus >= 0),  -- kurus: integer money, no float errors
  status        TEXT        NOT NULL DEFAULT 'ACTIVE',
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Full old row in `before` for updates and deletes (default would only carry the primary key)
ALTER TABLE wallets REPLICA IDENTITY FULL;

-- Append-only ledger: every balance change is one row, written in the SAME transaction as the wallet update.
-- Invariant: wallets.balance_kurus == SUM(ledger_entries.delta_kurus) per wallet. Reconciliation checks it downstream.
CREATE TABLE ledger_entries (
  entry_id    BIGSERIAL PRIMARY KEY,
  wallet_id   TEXT        NOT NULL,  -- no FK on purpose: entries outlive a closed wallet
  delta_kurus BIGINT      NOT NULL,
  reason      TEXT        NOT NULL,  -- opening | adjustment | transfer_out | transfer_in | close
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
