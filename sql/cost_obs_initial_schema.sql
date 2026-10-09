-- Cost Observability: initial ClickHouse schema.
--
-- Written by:
--   cost_obs_collect_elasticsearch_ta_dag        -> spf.elasticsearch_index_stats_raw
--   cost_obs_sync_tenant_registry_mapping_dag    -> spf.tenant_registry_mapping (via _staging + EXCHANGE TABLES)
--   spf.elasticsearch_index_stats_mv (refresh)   -> spf.elasticsearch_index_stats
--   cost_obs_publish_tenant_resource_usage_dag   -> spf.tenant_resource_usage_daily
-- Read by:
--   cost_obs_publish_tenant_resource_usage_dag   <- spf.elasticsearch_index_stats
--   dashboards                                   <- spf.tenant_resource_usage_daily_latest
--
-- Apply once per ClickHouse cluster, in order:
--   clickhouse-client --multiquery < sql/cost_obs_initial_schema.sql
-- Needs the {cluster}, {shard} and {replica} macros on every server, and ClickHouse >= 24.10 so
-- the refreshable view runs on one replica at a time in the Replicated database.
-- Only CREATE DATABASE uses ON CLUSTER: the Replicated database sends the rest to every replica.

CREATE DATABASE IF NOT EXISTS spf ON CLUSTER '{cluster}'
ENGINE = Replicated('/clickhouse/databases/spf', '{shard}', '{replica}');


-- One row per index per snapshot; ts is when the snapshot was collected (UTC).
CREATE TABLE IF NOT EXISTS spf.elasticsearch_index_stats_raw
(
    `ts` DateTime,
    `dc` LowCardinality(String),
    `namespace` LowCardinality(String),
    `instance_id` UInt64,
    `in_app_id` LowCardinality(String),
    `index_name` String,
    `total_docs_count` UInt64,
    `cpu_ms_cumulative` UInt64,
    `total_mem_bytes` UInt64,
    `total_store_bytes` UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PRIMARY KEY (dc, namespace)
ORDER BY (dc, namespace)
TTL ts + INTERVAL 1 WEEK
SETTINGS index_granularity = 8192;


-- Same snapshots with the tenant attached and CPU as a delta; filled by elasticsearch_index_stats_mv.
CREATE TABLE IF NOT EXISTS spf.elasticsearch_index_stats
(
    `ts` DateTime,
    `dc` LowCardinality(String),
    `namespace` LowCardinality(String),
    `tenant_id` UInt64,
    `instance_id` UInt64,
    `in_app_id` LowCardinality(String),
    `index_name` String,
    `total_docs_count` UInt64,
    `cpu_ms_delta` UInt64,
    `total_mem_bytes` UInt64,
    `total_store_bytes` UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PRIMARY KEY (dc, namespace, tenant_id)
ORDER BY (dc, namespace, tenant_id)
TTL ts + INTERVAL 1 WEEK
SETTINGS index_granularity = 8192;


-- One row per (instance_id, in_app_id); replaced in full by the sync DAG with EXCHANGE TABLES.
CREATE TABLE IF NOT EXISTS spf.tenant_registry_mapping
(
    `updated_at` DateTime DEFAULT now(),
    `dc` LowCardinality(String),
    `instance_id` UInt64,
    `hostname` LowCardinality(String),
    `in_app_id` LowCardinality(String),
    `tenant_id` UInt64
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
ORDER BY (instance_id, in_app_id);

-- Same schema; gets its own {uuid}, so its own replication path.
CREATE TABLE IF NOT EXISTS spf.tenant_registry_mapping_staging AS spf.tenant_registry_mapping;


CREATE MATERIALIZED VIEW IF NOT EXISTS spf.elasticsearch_index_stats_mv
REFRESH EVERY 30 MINUTE APPEND
TO spf.elasticsearch_index_stats
(
    `ts` DateTime,
    `dc` LowCardinality(String),
    `namespace` LowCardinality(String),
    `tenant_id` UInt64,
    `instance_id` UInt64,
    `in_app_id` LowCardinality(String),
    `index_name` String,
    `total_docs_count` UInt64,
    `cpu_ms_delta` UInt64,
    `total_mem_bytes` UInt64,
    `total_store_bytes` UInt64
)
AS SELECT
    s.ts                    AS ts,
    s.dc                    AS dc,
    s.namespace             AS namespace,
    m.tenant_id             AS tenant_id,   -- 0 when not in the mapping
    s.instance_id           AS instance_id,
    s.in_app_id             AS in_app_id,
    s.index_name            AS index_name,
    s.total_docs_count      AS total_docs_count,
    -- 0 for an index's first sample (lag defaults to itself) and uses the new value when the counter went down.
    -- UInt64 - UInt64 is Int64 in ClickHouse; the condition makes it non-negative, so cast it back.
    if(s.cpu_ms_cumulative >= s.p_cpu_ms_cumulative,
       toUInt64(s.cpu_ms_cumulative - s.p_cpu_ms_cumulative),
       s.cpu_ms_cumulative) AS cpu_ms_delta,
    s.total_mem_bytes       AS total_mem_bytes,
    s.total_store_bytes     AS total_store_bytes
FROM
(
    SELECT
        *,
        lagInFrame(cpu_ms_cumulative, 1, cpu_ms_cumulative) OVER w AS p_cpu_ms_cumulative
    FROM spf.elasticsearch_index_stats_raw
    -- lookback gives each index its previous snapshot as a baseline
    WHERE ts >= (SELECT max(ts) FROM spf.elasticsearch_index_stats_raw) - INTERVAL 1 DAY
    WINDOW w AS (PARTITION BY dc, namespace, index_name ORDER BY ts
                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
) AS s
LEFT JOIN spf.tenant_registry_mapping AS m
    ON s.instance_id = m.instance_id AND s.in_app_id = m.in_app_id
-- Last snapshot already in the deltas table, per cluster: each cluster's snapshot has its own ts
-- and is inserted on its own, so a single global max(ts) would skip a cluster whose snapshot
-- lands after another cluster's newer one was emitted. A cluster not there yet gets 1970-01-01.
LEFT JOIN
(
    SELECT dc, namespace, max(ts) AS last_ts
    FROM spf.elasticsearch_index_stats
    GROUP BY dc, namespace
) AS w
    ON s.dc = w.dc AND s.namespace = w.namespace
-- only emit snapshots newer than what's already in the deltas table for that cluster
WHERE s.ts > w.last_ts
    -- throwIf returns 0 when the check passes, so compare with 0 (a bare throwIf would filter out every row)
    AND throwIf((SELECT count() FROM spf.tenant_registry_mapping) = 0, 'tenant_registry_mapping empty') = 0;


-- One row per tenant, resource type and day for every published revision: a rerun adds the next
-- revision and keeps the earlier ones for comparison.
CREATE TABLE IF NOT EXISTS spf.tenant_resource_usage_daily
(
    `date` Date,
    `dc` LowCardinality(String),
    `service` LowCardinality(String),
    `deployment` LowCardinality(String),
    `tenant_id` UInt64,
    `resource_type` LowCardinality(String),
    `value` Float64,
    `unit` LowCardinality(String),
    `weight` Float64,
    `cluster_total` Float64,
    `rung` Enum8('measured' = 1, 'activity' = 2, 'footprint' = 3, 'roster' = 4, 'unattributed' = 5),
    `state` Enum8('provisional' = 1, 'ready' = 2),
    `revision` UInt32,
    `calculated_at` DateTime
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(date)
ORDER BY (dc, service, deployment, tenant_id, resource_type, date, revision)
SETTINGS index_granularity = 8192;


-- Latest revision of each published day (per dc, service and deployment), so a republished day
-- is never counted twice. This is what dashboards read.
CREATE VIEW IF NOT EXISTS spf.tenant_resource_usage_daily_latest AS
SELECT
    date, dc, service, deployment, tenant_id, resource_type,
    value, unit, weight, cluster_total, rung, state, revision, calculated_at
FROM spf.tenant_resource_usage_daily
WHERE (date, dc, service, deployment, revision) IN (
    SELECT date, dc, service, deployment, max(revision)
    FROM spf.tenant_resource_usage_daily
    GROUP BY date, dc, service, deployment
);
