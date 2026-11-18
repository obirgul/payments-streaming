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
