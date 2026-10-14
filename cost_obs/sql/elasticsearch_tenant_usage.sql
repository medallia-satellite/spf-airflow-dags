-- One day of an Elasticsearch cluster's index stats, per tenant. Params: dc, namespace, date.
--
-- The collector stamps each snapshot with the moment it was taken, a few seconds after the hour it
-- closes (the 24:00 one lands at 00:00:02 of the next day), and reruns or manual runs add extra
-- snapshots. So the day's snapshots are those in (00:30, 24:30] UTC. cpu_ms_delta covers the time
-- since the index's previous snapshot, so it adds up across any number of snapshots. Memory and
-- storage are averaged over the day's snapshots, so an index missing from some snapshots counts
-- as 0 for those.
WITH
    toDateTime(%(date)s, 'UTC') AS day_start,
    day_stats AS (
        SELECT *
        FROM spf.elasticsearch_index_stats
        WHERE dc = %(dc)s AND namespace = %(namespace)s
          AND ts > day_start + INTERVAL 30 MINUTE AND ts <= day_start + INTERVAL 1 DAY + INTERVAL 30 MINUTE
    ),
    (SELECT uniqExact(ts) FROM day_stats) AS snapshots
SELECT
    tenant_id,
    sum(cpu_ms_delta) AS cpu_ms,
    sum(total_mem_bytes) / snapshots / 1073741824 AS memory_gib,
    sum(total_store_bytes) / snapshots / 1073741824 AS storage_gib,
    snapshots
FROM day_stats
GROUP BY tenant_id
