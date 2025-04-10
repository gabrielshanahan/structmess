-- Create events table for tracking message handling in a distributed system
CREATE TABLE IF NOT EXISTS message_events (
    id UUID PRIMARY KEY DEFAULT gen_uuid_v7(),
    message_id UUID NOT NULL REFERENCES messages (id),
    type VARCHAR(50) NOT NULL CHECK (      --TODO: Change to enum type
        type IN (
                 'EMITTED',
                 'SEEN',                   -- Once per handler x message
                 'SUSPENDED',
                 'COMMITTED',              -- Once per handler x message
                 'ROLLED_BACK'             -- Once per handler x message
            )
        ),
    --TODO: coroutine_name & coroutine_instance
    handler_name VARCHAR(255), -- identifier of the code being run, should e.g. contain commit version
    handler_identifier VARCHAR(255), -- identifier of the instance of the code being run, kept as varchar cause can be e.g. system URL or something
    step VARCHAR(255),
    cooperation_lineage UUID[] NOT NULL DEFAULT '{}'::UUID[],
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for efficient queries
--TODO: Index on lineage
CREATE INDEX IF NOT EXISTS idx_message_events_message_id ON message_events (message_id);
CREATE INDEX IF NOT EXISTS idx_message_events_handler ON message_events (handler_name);
CREATE INDEX IF NOT EXISTS idx_message_events_type ON message_events (type);
CREATE INDEX IF NOT EXISTS idx_message_events_created_at ON message_events (created_at);

-- TODO: Could we reorder and merge the following two, so that (type, message_id) is always a prefix?
CREATE INDEX IF NOT EXISTS idx_message_events_msg_type ON message_events (message_id, type);
CREATE INDEX IF NOT EXISTS idx_message_events_handler_msg_type ON message_events (handler_name, message_id, type);
CREATE INDEX IF NOT EXISTS idx_message_events_cooperation_lineage ON message_events USING GIN (cooperation_lineage);
CREATE INDEX IF NOT EXISTS idx_message_events_created_at ON message_events (created_at);

--TODO: Come back to this and eval which of these we actually need - if we're not querying by it, there's no sense

-- There can only be a single EMITTED event per message
CREATE UNIQUE INDEX IF NOT EXISTS unique_emitted_msg
    ON message_events (message_id, type)
    WHERE type = 'EMITTED';

-- There can only be a single SEEN event for a given handler of a message
CREATE UNIQUE INDEX IF NOT EXISTS unique_seen_handler_msg
    ON message_events (handler_name, message_id, type)
    WHERE type = 'SEEN';

-- There can only be a single COMMITTED/ROLLED_BACK event for a given handler of a message
CREATE UNIQUE INDEX IF NOT EXISTS one_commit_or_rollback
    ON message_events (handler_name, message_id, type)
    WHERE type IN ('COMMITTED', 'ROLLED_BACK');

-- Comment on table
COMMENT ON TABLE message_events IS 'Stores records of message handling events in a distributed system';

-- Comments on columns
COMMENT ON COLUMN message_events.id IS 'Unique identifier for the message_events entry (UUIDv7)';
COMMENT ON COLUMN message_events.message_id IS 'Reference to the message this message_events entry relates to';
COMMENT ON COLUMN message_events.handler_name IS 'Name of the component/service handling the message';
COMMENT ON COLUMN message_events.handler_identifier IS 'Unique instance ID of the handler (e.g., pod/container ID)';
COMMENT ON COLUMN message_events.type IS 'Type of message_events event (EMITTED, PICKED_UP, HANDLED_SUCCESSFULLY, HANDLED_EXCEPTIONALLY, SUSPENDED, RESUMED)';
COMMENT ON COLUMN message_events.created_at IS 'Timestamp when the message_events entry was created';