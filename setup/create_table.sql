-- Drop table if it exists
DROP TABLE IF EXISTS test;


CREATE TABLE test (
    id bigint PRIMARY KEY NOT NULL,
    timestamp timestamp,
    name VARCHAR(100),
    value VARCHAR(100)
);

-- Truncate the table to remove all rows
-- TRUNCATE TABLE test;