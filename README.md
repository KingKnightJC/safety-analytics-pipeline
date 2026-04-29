# Safety Analytics Pipeline

A end-to-end data engineering project simulating the kind of trust & safety 
data infrastructure a Safeguards team would need — from raw event ingestion 
through to curated analytics and dashboarding.

## Overview

This project demonstrates a production-style data pipeline built with dbt and 
DuckDB, surfaced through a Metabase dashboard. It models a platform where users 
interact with an AI system, and the data team's job is to detect misuse patterns, 
score user risk, and provide self-service analytics to safety analysts.

## Architecture
Raw Seeds (CSV)
↓
Staging Layer — cleaned, typed, standardised
├── stg_events
├── stg_user_reports
└── stg_classifiers
↓
Marts Layer — business-ready analytics
├── fct_user_safety_signals  (one row per user, all signals aggregated)
├── dim_users                (risk tier + risk score per user)
└── mart_abuse_patterns      (daily abuse pattern trends)
↓
Metabase Dashboard — self-service analytics for safety analysts

## Dashboard

![Safety Analytics Dashboard](screenshots/dashboard.png)
![Lineage Graph](screenshots/lineage.png)

## Data Sources

| Table | Description |
|---|---|
| `raw_events` | Every user interaction with the AI platform, including content flags |
| `raw_user_reports` | Manual reports submitted by users against other users |
| `raw_classifiers` | Automated ML classifier scores for harmful behaviour detection |

## Models

### Staging (Silver Layer)
Cleans and standardises raw data — enforces types, normalises text, 
derives boolean flags and severity categories.

### Marts (Gold Layer)
- **fct_user_safety_signals** — aggregates all safety signals per user 
  across events, reports and classifier scores
- **dim_users** — assigns each user a risk tier (low/medium/high) and 
  numeric risk score based on their signals
- **mart_abuse_patterns** — daily summary of harmful content patterns 
  for trend detection

## Data Quality

30 dbt tests across all models including:
- Uniqueness and not-null checks on all primary keys
- Accepted values validation on all categorical fields
- Source freshness monitoring

## Tech Stack

| Tool | Purpose |
|---|---|
| dbt Core | Data transformation and modelling |
| DuckDB | Local analytical database |
| Metabase | Self-service dashboarding |
| Python | Seed data generation |
| GitHub | Version control |

## How to Run

```bash
# Install dependencies
pip install dbt-core dbt-duckdb

# Load seed data
dbt seed

# Run all models
dbt run

# Run data quality tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Key Findings from Sample Data

- **2 high-risk users** identified (user_002, user_004) with classifier 
  scores above 0.88 and multiple active reports
- **Hate speech** is the most frequent abuse pattern across the platform
- **Flagged events trending down** from Jan 1 to Jan 2 — demonstrating 
  trend detection capability

## About

Built as a portfolio project to demonstrate modern data engineering 
practices applied to trust & safety use cases — including Medallion 
architecture, data quality testing, and self-service analytics.
