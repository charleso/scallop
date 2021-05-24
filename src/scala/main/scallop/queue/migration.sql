CREATE TABLE queue (
  id BIGSERIAL PRIMARY KEY
, name TEXT NOT NULL
, key TEXT NOT NULL
, state SMALLINT NOT NULL
, next TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
, retries INTEGER DEFAULT 0
, created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX queue_name_queue_next_idx ON queue(name, next) WHERE state = 0;
CREATE INDEX queue_name_error_next_idx ON queue(name, next) WHERE state = 1;
CREATE INDEX queue_key_idx ON queue(key);
