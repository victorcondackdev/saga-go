-- Reference schema for a PostgreSQL implementation of store.Store.
--
-- STATUS: reference only. This file documents how the Store contract maps
-- onto PostgreSQL; it is NOT exercised by the test suite and no Postgres
-- store ships in this module (which has no dependencies). Treat it as a
-- starting point to be reviewed and load-tested before production use.
--
-- Mapping:
--   Create     INSERT ... ON CONFLICT (type, idem_key) DO NOTHING, then SELECT
--   Get        SELECT by id
--   Update     UPDATE ... WHERE id = $1 AND version = $2 AND owner IS NOT DISTINCT FROM $3
--              (0 rows => store.ErrConflict); history is appended to saga_events
--              instead of being rewritten with every update
--   Acquire    the CTE below: FOR UPDATE SKIP LOCKED gives each engine a disjoint batch
--   Heartbeat  UPDATE lease_until WHERE id AND owner
--   Release    UPDATE owner = NULL WHERE id AND owner
--   Signal     INSERT INTO saga_signals ... ON CONFLICT DO UPDATE; bump signal_seq and
--              next_run_at = now() in the same transaction
--   List/Stats plain SELECTs on the indexes below

CREATE TABLE saga_instances (
    id            text        PRIMARY KEY,
    type          text        NOT NULL,
    idem_key      text        NOT NULL,
    input_hash    text        NOT NULL,
    def_hash      text        NOT NULL,
    status        text        NOT NULL CHECK (status IN ('PENDING','RUNNING','COMPLETED',
                                   'COMPENSATING','COMPENSATED','FAILED_NEEDS_ATTENTION')),
    state         jsonb       NOT NULL,
    cursor_pos    integer     NOT NULL DEFAULT 0,
    pivot_passed  boolean     NOT NULL DEFAULT false,
    failure       text,
    steps         jsonb       NOT NULL,          -- []store.StepRecord
    seq           integer     NOT NULL DEFAULT 0,
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    deadline      timestamptz,
    next_run_at   timestamptz NOT NULL DEFAULT now(),
    owner         text,
    lease_until   timestamptz,
    version       bigint      NOT NULL DEFAULT 1,  -- fencing token
    signal_seq    bigint      NOT NULL DEFAULT 0,
    UNIQUE (type, idem_key)
);

-- Runnable work, ordered by due time: the only index Acquire needs.
CREATE INDEX saga_instances_due ON saga_instances (next_run_at)
    WHERE status IN ('PENDING', 'RUNNING', 'COMPENSATING');

CREATE INDEX saga_instances_attention ON saga_instances (updated_at)
    WHERE status = 'FAILED_NEEDS_ATTENTION';

-- Append-only execution history (store.Event).
CREATE TABLE saga_events (
    saga_id   text        NOT NULL REFERENCES saga_instances (id) ON DELETE CASCADE,
    seq       integer     NOT NULL,
    at        timestamptz NOT NULL,
    kind      text        NOT NULL,
    step      text,
    attempt   integer,
    duration  interval,
    error     text,
    detail    text,
    owner     text,
    PRIMARY KEY (saga_id, seq)
);

CREATE TABLE saga_signals (
    saga_id   text        NOT NULL REFERENCES saga_instances (id) ON DELETE CASCADE,
    name      text        NOT NULL,
    payload   jsonb       NOT NULL,
    received  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (saga_id, name)
);

-- Acquire($1 owner, $2 now, $3 lease interval, $4 limit).
-- SKIP LOCKED lets N engines poll concurrently without blocking on each
-- other's candidate rows; bumping version fences any previous holder.
WITH due AS (
    SELECT id, owner AS previous_owner
      FROM saga_instances
     WHERE status IN ('PENDING', 'RUNNING', 'COMPENSATING')
       AND next_run_at <= $2
       AND (owner IS NULL OR lease_until < $2)
     ORDER BY next_run_at, id
     LIMIT $4
     FOR UPDATE SKIP LOCKED
)
UPDATE saga_instances s
   SET owner       = $1,
       lease_until = $2 + $3,
       version     = s.version + 1
  FROM due
 WHERE s.id = due.id
RETURNING s.*, due.previous_owner;  -- maps to Instance.PreviousOwner

-- Update($1 id, $2 expected version, $3 caller owner or NULL, ...).
-- UPDATE saga_instances
--    SET status = $4, state = $5, cursor_pos = $6, pivot_passed = $7, failure = $8,
--        steps = $9, seq = $10, updated_at = $11, deadline = $12,
--        next_run_at = CASE WHEN signal_seq <> $13 THEN now() ELSE $14 END,
--        owner = $15, lease_until = $16, version = version + 1
--  WHERE id = $1 AND version = $2 AND owner IS NOT DISTINCT FROM $3
-- RETURNING version;

-- Heartbeat($1 id, $2 owner, $3 until).
-- UPDATE saga_instances SET lease_until = $3 WHERE id = $1 AND owner = $2;
