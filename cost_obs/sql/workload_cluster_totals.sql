-- A deployment's CPU and memory for one day, added up over every container in its namespace.
-- Params: dc, namespace, date.
--
-- workload_usage_daily samples each pod once a minute (5 samples per v_container_usage_5m window):
-- sample_count is pods x minutes, and the *_avg columns are per-pod averages over those samples.
-- One pod running all day = 1440 samples. avg * sample_count is core-minutes (or byte-minutes):
-- / 60 gives core-hours, / 1440 gives the memory held on average over the day, / 1073741824 is GiB.
SELECT
    count() AS workload_rows,
    sum(cpu_usage_cores_avg * sample_count) / 60 AS cpu_core_hours,
    sum(cpu_request_cores_avg * sample_count) / 60 AS cpu_request_core_hours,
    sum(mem_working_set_bytes_avg * sample_count) / 1440 / 1073741824 AS memory_gib,
    sum(mem_request_bytes_avg * sample_count) / 1440 / 1073741824 AS memory_request_gib
FROM cost.workload_usage_daily
WHERE dc = %(dc)s AND k8s_namespace = %(namespace)s AND window_start = %(date)s
