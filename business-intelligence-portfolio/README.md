# Business Intelligence Portfolio

> **Hypothetical Showcase:** Demonstrates BI and data visualisation skills across Tableau and Looker. The LookML examples are based on real dashboard patterns I've built for engineering velocity and DORA metrics reporting. All company names, org identifiers, and credentials are fully anonymised. No proprietary data is included.

---

## Tableau Public

Interactive dashboards built in Tableau — publicly available, no login required.

**[View full portfolio story →](https://public.tableau.com/app/profile/derek.o.halloran/viz/Portfolio_54/Story1)**
**[Browse all vizzes →](https://public.tableau.com/app/profile/derek.o.halloran/vizzes)**

---

### Featured Vizzes

#### WorldWealthSankey ⭐ Featured
*"If there was only $100 in the world — how would it be distributed?"*

A Sankey flow diagram mapping global wealth distribution across regions. Highlights a striking insight: 12 individual nations each hold more wealth than the entire continent of Africa combined. Built using Tableau's flow/path chart type with custom colour encoding per region.

**Skills demonstrated:** Sankey diagram construction, custom calculated fields for flow weighting, annotated insights, story-driven layout.

---

#### Food Delivery KPI Dashboard
*Operational performance dashboard for a food delivery business*

Multi-section KPI dashboard covering average market size, total sales, delivery time performance, food item metrics, and a sales vs back-order rate heat map calendar. Designed for operations managers who need a single-screen view of daily performance.

**Skills demonstrated:** KPI scorecards, heat map calendar, dual-axis charts, cross-filter actions, parameter-driven date selection.

---

#### Messi vs Ronaldo
*"Who is the greatest of all time?"*

Head-to-head statistical comparison of Messi and Ronaldo's careers — goals, assists, and trophies across club and international competitions. Uses a mirrored bar chart layout to make direct comparison intuitive at a glance.

**Skills demonstrated:** Custom layout design, mirrored bar charts, image integration, calculated career totals, tooltip customisation.

---

#### GDP & Happiness
*"Are wealthier nations happier?"*

Scatter plot exploring the relationship between GDP per capita and life satisfaction scores across countries. Applies k-means clustering to identify three distinct groups — revealing that beyond a wealth threshold, additional income has diminishing returns on happiness.

**Skills demonstrated:** Scatter plot with cluster analysis, logarithmic axis scaling, reference band annotations, cluster labelling, statistical narrative.

---

#### Bridges to Prosperity
*Bridging the gap across 22 nations*

Impact dashboard for a humanitarian infrastructure programme — tracking 313 bridges built, 1.14M individuals served, and 19.57 km of total span across 22 nations. Combines a world map with country-level bar charts and KPI tiles.

**Skills demonstrated:** Filled map + bar chart combination, KPI summary tiles, custom tooltips with impact metrics, map-to-chart filter actions.

---

#### Gender Pay Inequality
*Tracking the gender pay gap over time*

Area/line chart showing the evolution of the gender pay gap across multiple countries or sectors over time. Uses a diverging colour scheme (pink/blue) to make the gap immediately visible and highlights where progress has stalled.

**Skills demonstrated:** Dual-series area charts, diverging colour palettes, trend annotations, time-series filtering, comparative storytelling.

---

#### Peaches and Nectarines Production
*Global agricultural production analysis*

World map and bar chart combination showing peach and nectarine production by country. Demonstrates geographic data visualisation alongside ranked bar charts to surface both spatial patterns and country-level rankings in a single view.

**Skills demonstrated:** Filled map with custom colour scales, synchronized bar chart, parameter-driven measure switching, map zoom and filter interactions.

---

## LookML Examples ([`lookml/`](./lookml))

LookML is Looker's modelling language — a YAML-like syntax that defines how Looker generates SQL, structures explores (the self-serve query interface), and builds reusable dimension/measure libraries. The examples here cover the **Engineering Velocity** subject area, powered by the GitHub Insights data model.

### Why LookML matters

Raw SQL dashboards break when your schema changes and only one person can maintain them. LookML solves this by centralising business logic in a version-controlled layer:

- Dimensions and measures are defined once and reused across every dashboard
- Business rules (e.g. "exclude bots", "only SHA-matched deployments") are enforced at the model level — analysts can't accidentally violate them
- Explores control what joins are available, preventing accidental fan-out on many-to-many relationships
- `sql_always_where` filters apply globally to an explore, so defaults like "current employees only" don't need to be added to every tile

---

### Project Structure

```
lookml/
├── engineering_velocity.model.lkml     Model file — defines database connection
│                                       and the two explores (pr_velocity, dora_lead_time)
│
├── views/
│   ├── pr_facts.view.lkml              PR velocity metrics
│   │     Dimensions: org, repo, author LDAP, author type, state, branch, timestamps
│   │     Measures: PR count, merged count, merge rate,
│   │               avg/median/P75 time to merge
│   │
│   ├── lead_time_to_deploy.view.lkml   DORA lead time metrics
│   │     Dimensions: service, org, DORA bucket, match scenario, staging flags, timestamps
│   │     Measures: count, median lead time, P95 lead time,
│   │               avg merge→prod, avg time on staging, SHA match rate, elite %
│   │
│   └── dim_users.view.lkml             SCD Type 2 engineer dimension
│         Dimensions: LDAP, GitHub login, team, function, org hierarchy,
│                     employment type, SCD validity dates, is_current flag
│         Measures: engineer count (distinct LDAP)
│
└── dashboards/
    └── dora_metrics.dashboard.lkml     DORA Metrics dashboard definition
          Tiles: KPI row (median / P95 / elite % / SHA match rate)
                 Weekly lead time trend with DORA benchmark lines
                 DORA bucket distribution (donut)
                 Lead time by service (top 20 bar chart)
                 Lead time by team (bar chart)
                 Staging vs production time breakdown (stacked bar)
```

---

### [`engineering_velocity.model.lkml`](./lookml/engineering_velocity.model.lkml)

The model file is the entry point for Looker. It declares the database connection and defines two explores:

**`pr_velocity` explore** — answers questions about PR cycle time and review coverage:
- How long does it take PRs to get reviewed and merged, by team or repo?
- What % of PRs are merged without any external review (self-merges)?
- How has cycle time changed week-over-week?

```lookml
explore: pr_velocity {
  sql_always_where: ${pr_facts.author_type} = 'human' ;;  -- bots excluded by default
  join: dim_users { ... }                                  -- team attribution
  join: pr_review_summary { ... }                          -- review stats
}
```

**`dora_lead_time` explore** — answers DORA lead time questions:
- What's the median/P95 lead time by service or team?
- What % of deployments are in the "elite" DORA tier (<1 hour)?
- How much time do services spend in staging vs production promotion?

```lookml
explore: dora_lead_time {
  sql_always_where: ${lead_time_to_deploy.prod_match_scenario} = 'sha_match' ;;  -- high-confidence only
  join: pr_facts { ... }
  join: dim_users { ... }
}
```

---

### [`views/pr_facts.view.lkml`](./lookml/views/pr_facts.view.lkml)

Exposes `FACT_PULL_REQUESTS` to Looker. Key design decisions:

- **`drill_fields`** on the count measure — clicking a bar chart drops into a list of individual PRs with a direct GitHub link
- **`link`** on `pr_number` — generates a clickable "Open in GitHub" URL using the stored URL field
- **Three cycle time measures** (avg, median, P75) — median is most useful for skewed distributions; P75 surfaces the long tail
- `author_type` dimension uses a `case` block to show friendly labels ("Human", "Bot") in Looker UI while keeping raw values for filtering

---

### [`views/lead_time_to_deploy.view.lkml`](./lookml/views/lead_time_to_deploy.view.lkml)

Exposes `LEAD_TIME_TO_DEPLOY` to Looker. Key design decisions:

- **`is_sha_match` yesno dimension** — used as a default filter in measures (`filters: [is_sha_match: "Yes"]`) so every metric is automatically high-confidence unless the analyst explicitly overrides it
- **`dora_bucket_sort` hidden dimension** — LookML doesn't have a native "sort by another field" for string dimensions; this hidden numeric field controls ordering so buckets always display elite → high → medium → low, not alphabetically
- **`pct_sha_matched` measure** — a data quality KPI surfaced directly in the BI layer; if this drops below 80%, the deployment tooling needs attention
- **Separate measures for staging vs production timing** — `avg_time_on_staging_hours` is only meaningful when `has_staging_deployment = Yes`, so the filter is baked into the measure definition

---

### [`views/dim_users.view.lkml`](./lookml/views/dim_users.view.lkml)

SCD Type 2 engineer dimension. Key design decisions:

- **`is_current` filter pattern** — the model-level explore joins with `AND ${dim_users.is_current}` for all current-state dashboards. Point-in-time reports (e.g. "what team was this engineer on when the PR merged?") override this with a date-range join instead
- **`engineer_count` uses `count_distinct`** — prevents inflation when the dimension is joined to a many-to-one fact table
- Org hierarchy exposed at three levels (`team_name`, `function_l1_name`, `org_name`) so dashboards can be sliced at any level without schema changes

---

### [`dashboards/dora_metrics.dashboard.lkml`](./lookml/dashboards/dora_metrics.dashboard.lkml)

A complete DORA Metrics dashboard defined in LookML. Benefits of dashboard-as-code:
- Version controlled — dashboard changes go through code review
- Reproducible — deploy the same dashboard to dev/staging/prod environments
- Parameterised — date range, org, and service filters are pre-wired and reusable

The dashboard has six sections:
1. **KPI row** — four single-value tiles: median lead time, P95, elite %, SHA match rate
2. **Weekly trend** — line chart with reference lines at the DORA "high" (24h) and "elite" (1h) thresholds
3. **Bucket distribution** — donut chart showing the proportion of deployments in each DORA tier
4. **By service** — horizontal bar chart (top 20 services by median lead time)
5. **By team** — same view sliced by engineering team using the SCD2 user dimension
6. **Staging breakdown** — stacked bar showing how much time is spent in staging vs the merge→prod journey

---

## Tech Stack

`Looker` · `LookML` · `Snowflake` · `Tableau`
