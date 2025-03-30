-- Create message table for the message queue system
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create a function to generate UUIDv7
CREATE OR REPLACE FUNCTION gen_uuid_v7()
    RETURNS UUID AS
$$
DECLARE
    v_time_ms BIGINT := (EXTRACT(EPOCH FROM CLOCK_TIMESTAMP()) * 1000)::bigint;
    v_bytes bytea := REPEAT(E'\\000', 16);
BEGIN
    -- Set the first 6 bytes to the timestamp (48 bits)
    FOR i IN 0..5
        LOOP
            v_bytes := SET_BYTE(v_bytes, i, ((v_time_ms >> ((5 - i) * 8)) & 255)::int);
        END LOOP;

    -- Set version to 7 in byte 6 (high nibble)
    v_bytes := SET_BYTE(
            v_bytes,
            6,
            ((GET_BYTE(v_bytes, 6) & 0x0F) | 0x70)::int
               );

    -- Set variant to RFC4122 in byte 8
    v_bytes := SET_BYTE(
            v_bytes,
            8,
            ((GET_BYTE(v_bytes, 8) & 0x3F) | 0x80)::int
               );

    -- Fill remaining random bytes (except bytes 6 and 8)
    FOR i IN 6..15
        LOOP
            IF i = 6 OR i = 8 THEN CONTINUE; END IF;
            v_bytes := SET_BYTE(v_bytes, i, FLOOR(RANDOM() * 256)::int);
        END LOOP;

    -- Convert bytea to UUID
    RETURN ENCODE(v_bytes, 'hex')::uuid;
END;
$$ LANGUAGE plpgsql VOLATILE;


CREATE TABLE IF NOT EXISTS messages (
    id UUID PRIMARY KEY DEFAULT gen_uuid_v7(),
    topic VARCHAR(63) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for efficient message processing
CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages (created_at);
CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages (topic);

-- Comment on table
COMMENT ON TABLE messages IS 'Stores messages for the messaging queue system';

-- Comments on columns
COMMENT ON COLUMN messages.id IS 'Unique identifier for the message (UUIDv7)';
COMMENT ON COLUMN messages.topic IS 'The topic to which the message belongs';
COMMENT ON COLUMN messages.payload IS 'The actual message content stored as JSONB';
COMMENT ON COLUMN messages.created_at IS 'Timestamp when the message was created';

-- Create trigger function to notify on message insert
CREATE OR REPLACE FUNCTION notify_message_insert()
    RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(NEW.topic, NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to execute the notification function after insert
CREATE TRIGGER message_insert_trigger
    AFTER INSERT ON messages
    FOR EACH ROW
    EXECUTE FUNCTION notify_message_insert(); 