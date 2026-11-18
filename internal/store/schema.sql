-- Herald's own state (NOT the application's data). Mirrors paper Table 2.
-- {S} is the cell's schema. Every primary key starts with
-- PartitionKey(id) = hash prefix + id, so each server owns one contiguous
-- key range and recovery is a single range scan. All of it is soft state:
-- TRUNCATE loses nothing clients can't rebuild.

CREATE SCHEMA IF NOT EXISTS {S};

CREATE TABLE IF NOT EXISTS {S}.meta (
    key   text PRIMARY KEY,
    value bigint NOT NULL      -- 'epoch': incarnation of this state
);

CREATE TABLE IF NOT EXISTS {S}.matcher_objects (
    pkey          text PRIMARY KEY,             -- PartitionKey(object_id)
    object_id     text NOT NULL,
    version       bigint NOT NULL,
    source_client text NOT NULL DEFAULT '',
    pending       boolean NOT NULL DEFAULT true -- "propagate to Registrars" marker
);

CREATE TABLE IF NOT EXISTS {S}.matcher_registrations (
    obj_pkey  text NOT NULL,                    -- PartitionKey(object_id)
    object_id text NOT NULL,
    client_id text NOT NULL,
    PRIMARY KEY (obj_pkey, client_id)
);

CREATE TABLE IF NOT EXISTS {S}.registrar_clients (
    pkey      text PRIMARY KEY,                 -- PartitionKey(client_id)
    client_id text NOT NULL,
    seq       bigint NOT NULL,                  -- last unknown-version sequence number
    gateway   text NOT NULL DEFAULT '',         -- presence: which gateway holds the socket
    last_seen timestamptz NOT NULL              -- for garbage collection
);

CREATE TABLE IF NOT EXISTS {S}.registrar_registrations (
    client_pkey text NOT NULL,
    object_id   text NOT NULL,
    active      boolean NOT NULL,               -- false: an unregistration in flight
    pending     boolean NOT NULL DEFAULT true,  -- "propagate to Matcher" marker
    PRIMARY KEY (client_pkey, object_id)
);

CREATE TABLE IF NOT EXISTS {S}.registrar_notifications ( -- unacked, collapsed per object
    client_pkey text NOT NULL,
    object_id   text NOT NULL,
    version     bigint NOT NULL,
    unknown     boolean NOT NULL,
    seq         bigint NOT NULL,
    PRIMARY KEY (client_pkey, object_id)
);
