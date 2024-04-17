-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- In production you would almost certainly limit the replication user must be on the follower (slave) machine,
-- to prevent other clients accessing the log from other machines. For example, 'replicator'@'follower.acme.com'.
-- However, in this database we'll grant the test user 'paimonuser' all privileges:
--

-- ################################################################################
--  PostgresSyncDatabaseActionITCase
-- ################################################################################

CREATE DATABASE paimon_sync_database;
\c paimon_sync_database;

DROP SCHEMA IF EXISTS paimon_sync_schema CASCADE;
CREATE SCHEMA paimon_sync_schema;
SET search_path TO paimon_sync_schema;
CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);

ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);

ALTER TABLE t2
    REPLICA IDENTITY FULL;

-- no primary key, should be ignored
CREATE TABLE t3 (
    v1 INT
);
ALTER TABLE t3
    REPLICA IDENTITY FULL;

-- to make sure we use JDBC Driver correctly
DROP SCHEMA IF EXISTS paimon_sync_schema1 CASCADE;
CREATE schema paimon_sync_schema1;
SET search_path TO paimon_sync_schema1;
CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;


CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;
-- no primary key, should be ignored
CREATE TABLE t3 (
    v1 INT
);
ALTER TABLE t3
    REPLICA IDENTITY FULL;

-- ################################################################################
--  PostgresSyncDatabaseActionITCase#testIgnoreIncompatibleTables
-- ################################################################################

DROP SCHEMA IF EXISTS paimon_sync_schema_ignore_incompatible CASCADE;
CREATE schema paimon_sync_schema_ignore_incompatible;
SET search_path TO paimon_sync_schema_ignore_incompatible;

CREATE TABLE incompatible (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE incompatible
    REPLICA IDENTITY FULL;

CREATE TABLE compatible (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);
ALTER TABLE compatible
    REPLICA IDENTITY FULL;

-- ################################################################################
--  PostgresSyncDatabaseActionITCase#testTableAffix
-- ################################################################################

DROP SCHEMA IF EXISTS paimon_sync_schema_affix CASCADE;
CREATE schema paimon_sync_schema_affix;
SET search_path TO paimon_sync_schema_affix;

CREATE TABLE t1 (
    k1 INT,
    v0 VARCHAR(10),
    PRIMARY KEY (k1)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    k2 INT,
    v0 VARCHAR(10),
    PRIMARY KEY (k2)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;

-- ################################################################################
--  PostgresSyncDatabaseActionITCase#testIncludingTables
-- ################################################################################

DROP SCHEMA IF EXISTS paimon_sync_schema_including CASCADE;
CREATE schema paimon_sync_schema_including;
SET search_path TO paimon_sync_schema_including;

CREATE TABLE paimon_1 (
    k INT,
    PRIMARY KEY (k)
);
ALTER TABLE paimon_1
    REPLICA IDENTITY FULL;

CREATE TABLE paimon_2 (
    k INT,
    PRIMARY KEY (k)
);
ALTER TABLE paimon_2
    REPLICA IDENTITY FULL;

CREATE TABLE flink (
    k INT,
    PRIMARY KEY (k)
);
ALTER TABLE flink
    REPLICA IDENTITY FULL;

CREATE TABLE ignored (
    k INT,
    PRIMARY KEY (k)
);
ALTER TABLE ignored
    REPLICA IDENTITY FULL;


-- ################################################################################
--  PostgresSyncDatabaseActionITCase#testExcludingTables
-- ################################################################################
DROP SCHEMA IF EXISTS paimon_sync_schema_excluding CASCADE;
CREATE schema paimon_sync_schema_excluding;
SET search_path TO paimon_sync_schema_excluding;
CREATE TABLE paimon_1 (
    k INT,
    PRIMARY KEY (k)
);
ALTER TABLE paimon_1
    REPLICA IDENTITY FULL;

CREATE TABLE paimon_2 (
    k INT,
    PRIMARY KEY (k)
);
ALTER TABLE paimon_2
    REPLICA IDENTITY FULL;

CREATE TABLE flink (
    k INT,
    PRIMARY KEY (k)
);
ALTER TABLE flink
    REPLICA IDENTITY FULL;

CREATE TABLE sync (
    k INT,
    PRIMARY KEY (k)
);
ALTER TABLE sync
    REPLICA IDENTITY FULL;


-- ################################################################################
--  PostgresSyncDatabaseActionITCase#testIncludingAndExcludingTables
-- ################################################################################
DROP SCHEMA IF EXISTS paimon_sync_schema_in_excluding CASCADE;
CREATE schema paimon_sync_schema_in_excluding;
SET search_path TO paimon_sync_schema_in_excluding;
CREATE TABLE paimon_1 (
    k INT,
    PRIMARY KEY (k)
);
ALTER TABLE paimon_1
    REPLICA IDENTITY FULL;

CREATE TABLE paimon_2 (
    k INT,
    PRIMARY KEY (k)
);
ALTER TABLE paimon_2
    REPLICA IDENTITY FULL;

CREATE TABLE flink (
    k INT,
    PRIMARY KEY (k)
);
ALTER TABLE flink
    REPLICA IDENTITY FULL;

CREATE TABLE test (
    k INT,
    PRIMARY KEY (k)
);
ALTER TABLE test
    REPLICA IDENTITY FULL;

-- ################################################################################
--  PostgresSyncDatabaseActionITCase#testIgnoreCase
-- ################################################################################
DROP SCHEMA IF EXISTS paimon_ignore_CASE CASCADE;
CREATE schema paimon_ignore_CASE;
SET search_path TO paimon_ignore_CASE;

CREATE TABLE T (
    k INT,
    UPPERCASE_V0 VARCHAR(20),
    PRIMARY KEY (k)
);
ALTER TABLE T
    REPLICA IDENTITY FULL;


-- ################################################################################
--  PostgresSyncDatabaseActionITCase#testNewlyAddedTables
-- ################################################################################
DROP SCHEMA IF EXISTS paimon_sync_schema_newly_added_tables CASCADE;
CREATE schema paimon_sync_schema_newly_added_tables;
SET search_path TO paimon_sync_schema_newly_added_tables;
CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;

DROP SCHEMA IF EXISTS paimon_sync_schema_newly_added_tables_1 CASCADE;
CREATE schema paimon_sync_schema_newly_added_tables_1;
SET search_path TO paimon_sync_schema_newly_added_tables_1;
CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;

DROP SCHEMA IF EXISTS paimon_sync_schema_newly_added_tables_2 CASCADE;
CREATE schema paimon_sync_schema_newly_added_tables_2;
SET search_path TO paimon_sync_schema_newly_added_tables_2;
CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;

DROP SCHEMA IF EXISTS paimon_sync_schema_newly_added_tables_3 CASCADE;
CREATE schema paimon_sync_schema_newly_added_tables_3;
SET search_path TO paimon_sync_schema_newly_added_tables_3;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;

DROP SCHEMA IF EXISTS paimon_sync_schema_newly_added_tables_4 CASCADE;
CREATE schema paimon_sync_schema_newly_added_tables_4;
SET search_path TO paimon_sync_schema_newly_added_tables_4;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    k1 INT,
    k2 VARCHAR(10),
    v1 INT,
    v2 BIGINT,
    PRIMARY KEY (k1, k2)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;

DROP SCHEMA IF EXISTS paimon_sync_schema_add_ignored_table CASCADE;
CREATE schema paimon_sync_schema_add_ignored_table;
SET search_path TO paimon_sync_schema_add_ignored_table;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE a (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE a
    REPLICA IDENTITY FULL;

DROP SCHEMA IF EXISTS many_table_sync_test CASCADE;
CREATE schema many_table_sync_test;
SET search_path TO many_table_sync_test;

CREATE TABLE a (
    k INT,
    v VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE a
    REPLICA IDENTITY FULL;

-- ################################################################################
--  testSyncMultipleShards
-- ################################################################################

DROP SCHEMA IF EXISTS schema_shard_1 CASCADE;
CREATE schema schema_shard_1;
SET search_path TO schema_shard_1;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    k BIGINT,
    v1 DOUBLE PRECISION,
    PRIMARY KEY (k)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;

CREATE TABLE t3 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t3
    REPLICA IDENTITY FULL;

DROP SCHEMA IF EXISTS schema_shard_2 CASCADE;
CREATE schema schema_shard_2;
SET search_path TO schema_shard_2;

-- test schema merging
CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(20),
    v2 BIGINT,
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

-- test schema evolution
CREATE TABLE t2 (
    k BIGINT,
    v1 DOUBLE PRECISION,
    PRIMARY KEY (k)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;

-- test some shard doesn't have primary key
CREATE TABLE t3 (
    k INT,
    v1 VARCHAR(10)
);
ALTER TABLE t3
    REPLICA IDENTITY FULL;

-- ################################################################################
--  testSyncMultipleShardsWithoutMerging
-- ################################################################################
DROP SCHEMA IF EXISTS without_merging_shard_1 CASCADE;
CREATE schema without_merging_shard_1;
SET search_path TO without_merging_shard_1;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;

DROP SCHEMA IF EXISTS without_merging_shard_2 CASCADE;
CREATE schema without_merging_shard_2;
SET search_path TO without_merging_shard_2;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(20),
    v2 BIGINT,
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

-- test some shard doesn't have primary key
CREATE TABLE t2 (
    k INT,
    v1 VARCHAR(10)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;

-- ################################################################################
--  testMonitoredAndExcludedTablesWithMering
-- ################################################################################
DROP SCHEMA IF EXISTS monitored_and_excluded_shard_1 CASCADE;
CREATE schema monitored_and_excluded_shard_1;
SET search_path TO monitored_and_excluded_shard_1;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;

CREATE TABLE t3 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t3
    REPLICA IDENTITY FULL;

DROP SCHEMA IF EXISTS monitored_and_excluded_shard_2 CASCADE;
CREATE schema monitored_and_excluded_shard_2;
SET search_path TO monitored_and_excluded_shard_2;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    k INT,
    v2 DOUBLE PRECISION,
    PRIMARY KEY (k)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;

CREATE TABLE t3 (
    k INT,
    v2 VARCHAR(10)
);
ALTER TABLE t3
    REPLICA IDENTITY FULL;

-- ################################################################################
--  PostgresSyncDatabaseActionITCase#testNewlyAddedTablesOptionsChange
-- ################################################################################
DROP SCHEMA IF EXISTS newly_added_tables_option_schange CASCADE;
CREATE schema newly_added_tables_option_schange;
SET search_path TO newly_added_tables_option_schange;

CREATE TABLE t1 (
   k INT,
   v1 VARCHAR(10),
   PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

-- ################################################################################
--  testMetadataColumns
-- ################################################################################
DROP SCHEMA IF EXISTS metadata CASCADE;
CREATE schema metadata;
SET search_path TO metadata;

CREATE TABLE t1 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t1
    REPLICA IDENTITY FULL;

CREATE TABLE t2 (
    k INT,
    v1 VARCHAR(10),
    PRIMARY KEY (k)
);
ALTER TABLE t2
    REPLICA IDENTITY FULL;
