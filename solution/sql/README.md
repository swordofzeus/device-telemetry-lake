# Analytical SQL Report Inventory

## Report Map

| Perspective | File | Grain | Purpose |
|---|---|---|---|
| 4 — Technology Statistics | `4_technology_statistics/technology_throughput.sql` | One row per technology | Min, max, mean, median, P5, P95 for download and upload speeds by access technology. Ordered by median download DESC. |
| 4 — Technology Statistics | `4_technology_statistics/technology_connectivity.sql` | One row per technology | Min, max, mean, median, P95 for latency and jitter, mean and P95 for packet loss by access technology. Ordered by median latency ASC. |
| 5 — Underperforming Devices | `5_underperforming_devices/underperforming_vs_peers.sql` | One row per underperforming device | Devices with median download below 50% of their peer group (same provider + technology) median. Peer groups < 5 devices excluded. |
| 6 — Provider Comparison | `6_provider_comparison/advertised_vs_actual.sql` | One row per provider + technology | Per-device delivery ratio (actual/advertised) averaged by provider+technology. Each device weighted equally. Ordered by worst delivery ratio. |
| 7 — Geographic Patterns | `7_geographic_performance_patterns/urban_vs_rural_performance.sql` | One row per area type (Urban/Rural) | Throughput and connectivity comparison using `inside_census_urban_area` flag. Joins `census_block_groups` for population density. |
| 7 — Geographic Patterns | `7_geographic_performance_patterns/distance_vs_performance.sql` | One row per distance bucket (0-5, 5-15, 15-30, 30-50, 50+ km) | Throughput and connectivity by distance from nearest urban area. Tests whether degradation is gradual or has a cliff. |
| 9 — Critical Infrastructure | `9_critical_infra_analysis/critical_infra_connectivity.sql` | One row per critical_infrastructure flag | Avg and P95 latency, jitter, packet loss at critical vs non-critical sites. |
| 9 — Critical Infrastructure | `9_critical_infra_analysis/critical_infra_throughput.sql` | One row per critical_infrastructure flag | Avg, median, P5 download and upload at critical vs non-critical sites. |
| 9 — Critical Infrastructure | `9_critical_infra_analysis/critical_infra_device_health.sql` | One row per critical_infrastructure flag | Avg and P95 CPU, memory, temperature at critical vs non-critical sites. |
| 9 — Critical Infrastructure | `9_critical_infra_analysis/critical_infra_blast_radius.sql` | One row per incident | Infrastructure incident blast radius: walks dependency graph from upstream device to downstream sites, counts critical infrastructure sites impacted. |

---

## Perspective 9 — Shared Incidents and Downstream Impact (Critical Infrastructure)

**Why critical infrastructure dominates the reports:** The challenge asks us to understand how shared network dependencies affect multiple sites. In a real ISP network, not all outages are equal — a router going down at a hospital, fire station, or school has a fundamentally different blast radius than the same failure at a residential site. Critical infrastructure sites are flagged in the `sites` reference table via `critical_infrastructure = true`. I sliced this dimension across connectivity, throughput, and device health to answer a single question from three angles: *are critical infrastructure sites getting worse service than the rest of the network?*

1. **`critical_infra_connectivity.sql`** — Connectivity quality at critical vs non-critical sites. Grain: one row per `critical_infrastructure` flag. Outputs `avg_latency_ms`, `avg_jitter_ms`, `avg_packet_loss_pct` and their P95 counterparts. P95 captures the worst 5% of experiences without being dominated by one-off outliers the way P99 can be at lower event volumes.

2. **`critical_infra_throughput.sql`** — Throughput performance at critical vs non-critical sites. Grain: one row per `critical_infrastructure` flag. Outputs `avg_download_mbps`, `avg_upload_mbps`, median (P50), and P5 (floor of performance). P5 is used instead of P95 because for throughput the concern is the *bottom* of the distribution — the worst speeds users actually experience.

3. **`critical_infra_device_health.sql`** — Device resource utilization at critical vs non-critical sites. Grain: one row per `critical_infrastructure` flag. Outputs `avg_cpu_pct`, `avg_memory_pct`, `avg_temperature_c` and their P95 counterparts. P95 flags devices approaching resource limits — a device consistently hitting P95 CPU > 90% is a candidate for proactive replacement before it degrades service.

4. **`critical_infra_blast_radius.sql`** — Infrastructure incident blast radius. Grain: one row per `incident_id` + originating device (assumes each `incident_id` maps to one originating device/site/severity; if an incident spans multiple devices, each appears as a separate row). Starts from `infrastructure_events_gold`, groups by incident to get the upstream device, severity, and time window. Then walks the `device_dependencies` graph one hop (upstream → immediate downstream) to find downstream devices and their sites, counting how many are critical infrastructure. Outputs `incident_id`, `upstream_device_id`, `severity`, `incident_start`/`incident_end`, `downstream_devices`, `downstream_sites`, and `critical_sites_impacted`. Ordered by `critical_sites_impacted` DESC. Multi-hop transitive traversal (full dependency chain) would require a recursive CTE and is a production improvement.

**Assumptions:** All four reports join gold event tables to `devices` and `sites` reference tables. They cover the full event history (no time window filter) to establish baselines. No hard-coded device IDs or site IDs — results generalize across simulator seeds.

## Perspective 6 — Provider Comparison (Advertised vs Actual)

Do providers deliver what they advertise? This report compares advertised download/upload speeds from the `devices` reference table against actual measured throughput from `throughput_events_gold`. Ordered by worst delivery ratio first — the providers least delivering on their promise float to the top.

1. **`advertised_vs_actual.sql`** — Grain: one row per `provider_name` + `technology` combination. Pre-aggregates to one row per device first (device's actual avg / device's advertised = that device's delivery ratio), then averages those per-device ratios by provider + technology. This ensures each device gets equal weight regardless of event count. Outputs `avg_advertised_download_mbps`, `avg_advertised_upload_mbps`, `avg_actual_download_mbps`, `avg_actual_upload_mbps`, `avg_download_delivery_pct` / `avg_upload_delivery_pct` (delivery ratio averaged across devices), and `p5_download_delivery_pct` (the worst 5% of devices in that cohort). Devices with NULL advertised speeds are excluded. Delivery percentage below 100% means underdelivering. Ordered by `avg_download_delivery_pct` ASC so the worst provider+technology combinations appear first.

## Perspective 7 — Geographic Performance Patterns

Does distance from urban areas predict network performance? These reports use location and Census context from the `sites` and `census_block_groups` tables to surface geographic disparities in service quality.

1. **`urban_vs_rural_performance.sql`** — Grain: one row per area type (`Urban` / `Rural`), classified by the `inside_census_urban_area` flag on `sites`. Joins `census_block_groups` for `avg_population_density`. Outputs throughput metrics (`avg_download_mbps`, `median_download_mbps`, `p5_download_mbps`) and connectivity metrics (`avg_latency_ms`, `avg_jitter_ms`, `avg_packet_loss_pct`, `p95_latency_ms`). P5 download captures the floor of throughput in each area type; P95 latency captures the tail. This is the broadest geographic cut — a single row per classification to establish whether the urban/rural divide exists in this network.

2. **`distance_vs_performance.sql`** — Grain: one row per distance bucket from the nearest urban area (`0-5 km`, `5-15 km`, `15-30 km`, `30-50 km`, `50+ km`). Uses `distance_to_nearest_urban_area_km` from `sites`. Outputs `device_count`, `site_count`, throughput (`avg_download_mbps`, `avg_upload_mbps`, `median_download_mbps`), and connectivity (`avg_latency_ms`, `avg_jitter_ms`, `avg_packet_loss_pct`, `p95_latency_ms`). The buckets are chosen to show whether degradation is gradual or has a cliff — if performance drops sharply at 30+ km, that's a different infrastructure problem than a smooth linear decline. Ordered by distance ascending so the pattern reads left to right.

## Perspective 4 — Equivalent Statistics by Access Technology

How does network performance differ across access technologies (fiber, DSL, cable, fixed wireless, etc.)? These reports produce the same throughput and connectivity statistics grouped by the `technology` column on `devices`, making it easy to compare the performance profile of each technology side by side.

1. **`technology_throughput.sql`** — Grain: one row per technology. Outputs min, max, mean, median (P50), P5 (floor), and P95 (ceiling) for both download and upload speeds. Ordered by `median_download_mbps` DESC so the fastest technologies appear first. The full min/max/P5/P95 spread shows not just which technology is fastest on average, but which has the tightest or widest variance — a technology with a high median but a very low P5 may have reliability issues.

2. **`technology_connectivity.sql`** — Grain: one row per technology. Outputs min, max, mean, median, and P95 for latency and jitter, plus mean and P95 for packet loss. Ordered by `median_latency_ms` ASC so the lowest-latency technologies appear first. The combination of throughput and connectivity reports reveals whether a technology that delivers high speeds also delivers stable connections, or whether there's a speed-vs-stability trade-off.

## Perspective 5 — Devices Underperforming an Appropriate Peer Group

Identifies individual devices performing significantly below their peers. The peer group is defined as all devices sharing the same `provider_name` and `technology` — this controls for expected performance differences between fiber and DSL, or between providers with different infrastructure quality.

1. **`underperforming_vs_peers.sql`** — Grain: one row per underperforming device. Computes each device's median download speed, then computes the peer group's median download. A device is flagged as underperforming if its median download is below **50% of its peer group median** — meaning it's getting less than half the throughput of a typical device on the same provider and technology. Peer groups with fewer than 5 devices are excluded to avoid flagging devices in groups too small for a meaningful comparison. Outputs `device_median_download_mbps`, `peer_median_download_mbps`, `threshold_mbps` (the 50% cutoff), and `pct_of_peer_median` (how far below the peer the device actually is). Ordered by `pct_of_peer_median` ASC so the worst performers appear first.

## Key Thresholds and Definitions

- **Underperforming threshold (Perspective 5):** Device median download < 50% of peer group median. Peer group = same `provider_name` + `technology`. Minimum 5 devices per group.
- **P95 vs P5:** P95 is used for latency/jitter/health metrics (worst tail). P5 is used for throughput (floor of performance). Both avoid the P99 noise problem at moderate event volumes.
- **Distance buckets (Perspective 7):** 0-5 km, 5-15 km, 15-30 km, 30-50 km, 50+ km from nearest urban area. Based on `distance_to_nearest_urban_area_km` from the `sites` table.
- **Delivery percentage (Perspective 6):** Per-device `(device_avg_actual / device_advertised) * 100`, then averaged across devices in each provider+technology group. Below 100% = underdelivering. Above 100% = conservative tier.

## Assumptions

- All event reports query gold (deduplicated) tables, not bronze.
- Full event history is used (no time window filter) to establish baselines.
- No hard-coded device IDs, site IDs, or expected results — reports generalize across simulator seeds.
- Reports are read-only and deterministically ordered.
