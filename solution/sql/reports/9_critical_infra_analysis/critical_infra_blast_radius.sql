-- Infrastructure incidents and their downstream blast radius
-- Grain: one row per incident + originating device (assumes each incident_id
--   maps to one originating device/site/severity; if an incident spans multiple
--   devices, each device appears as a separate row)
-- Dependency traversal: single hop (upstream → immediate downstream only).
--   Multi-hop transitive closure would require a recursive CTE and is a
--   production improvement.
-- Joins infrastructure events → device_dependencies → downstream sites
-- to measure how many devices and critical sites each incident impacted
WITH incident_summary AS (
    SELECT
        e.incident_id,
        e.device_id AS upstream_device_id,
        e.site_id AS upstream_site_id,
        e.severity,
        MIN(e.event_timestamp) AS incident_start,
        MAX(e.event_timestamp) AS incident_end,
        COUNT(*) AS incident_events
    FROM iceberg.solution.infrastructure_events_gold e
    GROUP BY e.incident_id, e.device_id, e.site_id, e.severity
),
downstream_impact AS (
    SELECT
        i.incident_id,
        i.upstream_device_id,
        i.upstream_site_id,
        i.severity,
        i.incident_start,
        i.incident_end,
        i.incident_events,
        COUNT(DISTINCT dd.downstream_device_id) AS downstream_devices,
        COUNT(DISTINCT ds.site_id) AS downstream_sites,
        COUNT(DISTINCT CASE WHEN s.critical_infrastructure THEN ds.site_id END) AS critical_sites_impacted
    FROM incident_summary i
    LEFT JOIN iceberg.solution.device_dependencies dd
        ON i.upstream_device_id = dd.upstream_device_id
    LEFT JOIN iceberg.solution.devices ds
        ON dd.downstream_device_id = ds.device_id
    LEFT JOIN iceberg.solution.sites s
        ON ds.site_id = s.site_id
    GROUP BY i.incident_id, i.upstream_device_id, i.upstream_site_id,
             i.severity, i.incident_start, i.incident_end, i.incident_events
)
SELECT
    incident_id,
    upstream_device_id,
    upstream_site_id,
    severity,
    incident_start,
    incident_end,
    incident_events,
    downstream_devices,
    downstream_sites,
    critical_sites_impacted
FROM downstream_impact
ORDER BY critical_sites_impacted DESC, downstream_devices DESC
