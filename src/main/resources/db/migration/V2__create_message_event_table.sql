--TODO: Doc that rollback can only be initiated from something that's running, or from the top (in the case of cancellations).
-- This is because, otherwise, we could emit a rollback for some message that was part of some middle step of its parent,
-- without first rolling back the subsequent steps
-- TODO: Rename to "ROLLBACK_SEEN"? Then we have 'seen-type' events as SEEN and ROLLING_BACK, which corresponds to the duality in the place where we use the strategy in the SQL. Probably let's not though, let's just document it.
-- TODO: Actually, rename to "HAPPY_PATH_CONTINUATION_START" and "ROLLBACK_PATH_CONTINUATION_START"?
CREATE TYPE message_event_type AS ENUM (
    'EMITTED',
    'SEEN',
    'SUSPENDED',
    'COMMITTED',
    'ROLLING_BACK',
    'ROLLBACK_EMITTED',
    'ROLLED_BACK',
    'ROLLBACK_FAILED',
    'CANCELLATION_REQUESTED'
);
CREATE TABLE IF NOT EXISTS message_event (
    id UUID PRIMARY KEY DEFAULT gen_uuid_v7(),
    message_id UUID NOT NULL REFERENCES message (id),
    type message_event_type NOT NULL,
    coroutine_name VARCHAR(255), -- identifier of the code being run, should e.g. contain commit version
    coroutine_identifier VARCHAR(255), -- identifier of the instance of the code being run, kept as varchar cause can be e.g. system URL or something
    step VARCHAR(255),
    cooperation_lineage UUID[] NOT NULL DEFAULT '{}'::UUID[],
    -- Using CLOCK_TIMESTAMP() instead of CURRENT_TIMESTAMP is necessary because of a very subtle bug that happens otherwise.
    -- As a refresher, CURRENT_TIMESTAMP returns the time the transaction started (i.e., the BEGIN statement was received), while CLOCK_TIMESTAMP() returns
    -- the actual time at the moment it is run. Since in this implementation, we're running the event loop periodically next to the code that processes the
    -- events, sometimes things line up just right and the following happens:
    --
    -- 10:52:40.331 [70] LOG:  statement: BEGIN
    -- 10:52:40.334 [68] LOG:  statement: BEGIN
    -- 10:52:40.335 [70] LOG:  execute <unnamed>: WITH ... <---- our big query containing SELECT FOR UPDATE
    -- 10:52:40.336 [86] LOG:  execute <unnamed>: INSERT 'ROLLING_BACK'
    -- 10:52:40.337 [70] LOG:  statement: ROLLBACK <--- transaction rollback, lock released
    -- 10:52:40.338 [68] LOG:  execute <unnamed>: WITH ... <---- our big query containing SELECT FOR UPDATE
    -- 10:52:40.339 [68] LOG:  execute <unnamed>: INSERT 'ROLLED_BACK'
    -- 10:52:40.339 [68] LOG:  statement: COMMIT
    --
    -- In the above, you can see the message processing, which will eventually cause a rollback, starting at .331 via connection [70]. You can also see the
    -- event loop triggering the processing of a message in .334 via connection [68]. We can then see us inserting the ROLLING_BACK at .336, in its own
    -- single-statement-transaction (triggered by the code running under [70]), via connection [86]. Since the statement is its own transaction,
    -- created_at of ROLLING_BACK will be .366, and, at .337, the outer transaction rolls back and releases the FOR UPDATE lock. Only at .338 does the
    -- periodically triggered processing get around to running the SELECT FOR UPDATE, at which point the record is already unlocked, everything proceeds
    -- normally, and we insert and commit a ROLLED_BACK at .339. However, if we used CURRENT_TIMESTAMP, the created_at of ROLLED_BACK would be .334 - the
    -- time the transaction started. As a consequence, when ordered by created_at, the ROLLED_BACK would appear to have happened before the ROLLING_BACK.
    --
    -- This behavior of CURRENT_TIMESTAMP is a feature of Postgres, which allows it "to have a consistent notion of the “current” time, so that multiple
    -- modifications within the same transaction bear the same time stamp", to quote the docs. While this is commendable, in the specific case of message_event,
    -- we really do want to use the time of insertion and not the time the transaction started. This is especially true since we're never actually persisting
    -- more than a single record at a time.
    --
    -- Note that this could happen even if the ROLLING_BACK were somehow not done in its own separate transaction, but used the created_at determined by
    -- the outer transaction - the fundamental problem is the periodically triggered transaction that determines the timestamp for ROLLED_BACK, starting
    -- earlier than the one that emits the ROLLING_BACK, but then not doing anything until after the ROLLING_BACK transaction has finished. Due to this
    -- timing, not even SKIP LOCKED helps - we would need to use advisory locks that are acquired by a coroutine at the beginning of each run, before the
    -- transaction is started, and released at the end of each run, after the transaction is finished, to guarantee that only a single transaction only ever
    -- exists at a time. However, just using CLOCK_TIMESTAMP() is a lot simpler.
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CLOCK_TIMESTAMP(),
    exception JSONB,
    context JSONB
);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_message_event_handler_msg_type ON message_event (message_id, type, coroutine_name);
CREATE INDEX IF NOT EXISTS idx_message_event_handler ON message_event (coroutine_name);
CREATE INDEX IF NOT EXISTS idx_message_event_type ON message_event (type);
CREATE INDEX IF NOT EXISTS idx_message_event_created_at ON message_event (created_at);
CREATE INDEX IF NOT EXISTS idx_message_event_cooperation_lineage ON message_event USING GIN (cooperation_lineage);

-- There can only be a single EMITTED event per message
CREATE UNIQUE INDEX IF NOT EXISTS unique_emitted_msg
    ON message_event (message_id, type)
    WHERE type = 'EMITTED';

-- There can only be a single ROLLBACK_EMITTED event per message
CREATE UNIQUE INDEX IF NOT EXISTS unique_rollback_emitted_msg
    ON message_event (message_id, type)
    WHERE type = 'ROLLBACK_EMITTED';

-- There can only be a single SEEN event for a given handler of a message
CREATE UNIQUE INDEX IF NOT EXISTS unique_seen_handler_msg
    ON message_event (coroutine_name, message_id, type)
    WHERE type = 'SEEN';

-- There can only be a single ROLLING_BACK event for a given handler of a message
CREATE UNIQUE INDEX IF NOT EXISTS unique_rolling_back_handler_msg
    ON message_event (coroutine_name, message_id, type)
    WHERE type = 'ROLLING_BACK';

-- There can only be a single successful terminal event for a given handler of a message
CREATE UNIQUE INDEX IF NOT EXISTS one_commit
    ON message_event (coroutine_name, message_id, type)
    WHERE type IN ('COMMITTED');

-- There can only be a single failure terminal event for a given handler of a message
CREATE UNIQUE INDEX IF NOT EXISTS one_rollback_result
    ON message_event (coroutine_name, message_id, type)
    WHERE type IN ('ROLLED_BACK', 'ROLLBACK_FAILED');

-- There can only be a single CANCELLATION_REQUESTED event per cooperation scope
CREATE UNIQUE INDEX IF NOT EXISTS one_cancellation_request
    ON message_event (cooperation_lineage, type)
    WHERE type = 'CANCELLATION_REQUESTED';

-- Index for SleepUntilKey
CREATE INDEX idx_message_event_sleep_until_key ON message_event
    ((context->'SleepUntilKey'->'wakeAfter'))
    WHERE context ? 'SleepUntilKey';

-- Indexes for deadline checks
CREATE INDEX idx_message_event_happy_path_deadline ON message_event
    ((context->'HappyPathDeadlineKey'->'deadline'))
    WHERE context ? 'HappyPathDeadlineKey';

CREATE INDEX idx_message_event_rollback_deadline ON message_event
    ((context->'RollbackDeadlineKey'->'deadline'))
    WHERE context ? 'RollbackDeadlineKey';

CREATE INDEX idx_message_event_absolute_deadline ON message_event
    ((context->'AbsoluteDeadlineKey'->'deadline'))
    WHERE context ? 'AbsoluteDeadlineKey';

-- Index for RunCountKey
CREATE INDEX idx_message_event_run_count ON message_event
    ((context->'RunCountKey'->'value'))
    WHERE context ? 'RunCountKey';

