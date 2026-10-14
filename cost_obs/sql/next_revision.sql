-- Next revision to publish for one day of a deployment (1 if none was published yet).
-- Params: date, dc, service, namespace.
SELECT max(revision) + 1
FROM spf.tenant_resource_usage_daily
WHERE date = %(date)s AND dc = %(dc)s AND service = %(service)s AND deployment = %(namespace)s
