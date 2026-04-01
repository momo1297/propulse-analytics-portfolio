# Propulse Analytics — Subscription Intelligence Pipeline

> **"Which subscribers are about to churn — and what can we do about it before they leave?"**

A production-grade analytics pipeline built for subscription businesses managing multiple partners. Turns raw CSV data into actionable revenue and retention insights using a Bronze → Silver → Gold medallion architecture.

---

## Business Context

**The problem:** A SaaS subscription platform hosts 3 media partners (HealthMedia, WellnessPress, NutriDigital). Each partner needs to answer the same critical questions every month:

- Are my subscribers engaged or silently drifting toward churn?
- Which acquisition channel actually delivers loyal, high-value customers?
- Is my email program reducing churn — or just adding noise?
- How much revenue did I actually collect this month (vs. failed payments)?

Without a unified data layer, these questions require manual spreadsheet work across 3 separate exports. This pipeline automates the entire flow — from raw ingestion to executive-ready KPI tables — in a single `python pipeline.py` run.

**Dataset:** 10,000 subscribers · 423,000 email events · 21,900 transactions · 3 partners · 2 years of history

---

## Technical Stack

| Layer | Tool | Why |
|---|---|---|
| Query engine | **DuckDB** | Analytical SQL on local Parquet at scale — no server needed |
| Language | **Python 3** | Orchestration, path handling, summary reporting |
| Storage format | **Parquet** | Columnar, compressed, Athena/Spark-ready from day one |
| Architecture | **Medallion (Bronze/Silver/Gold)** | Separates raw ingestion, cleaning, and business logic into auditable layers |
| Modeling | **Star Schema** (Gold layer) | Fact tables (transactions, events) + dimension (subscribers) — standard for BI tools |

---

## Pipeline Architecture

```
data/bronze/          ← Raw CSVs + _ingested_at timestamp (audit trail)
     ↓
data/silver/          ← Typed, cleaned, business columns added
     ↓
data/gold/            ← 4 analytical tables, KPI-ready
```

### Bronze — Immutable raw ingestion
Every CSV is loaded as-is with a `_ingested_at` timestamp. Nothing is modified. This layer is the auditable source of truth: if a data quality issue surfaces weeks later, you can always trace it back to the exact ingestion batch.

### Silver — Business-grade cleaning

| Table | Key transformations |
|---|---|
| `subscribers` | Typed dates · `segment` (premium / standard / basic by MRR tier) · `is_converted` flag to exclude leads from churn metrics |
| `email_events` | Boolean casting from Python strings · `engagement_score` (clicked=2, opened=1, ignored=0) |
| `transactions` | Typed amounts · `is_failed` flag · `revenue` = 0 on failed payments to prevent MRR inflation |

### Gold — Analytical tables (one business question per table)

| Table | Business question | Key metric |
|---|---|---|
| `gold_mrr_monthly` | **Is our recurring revenue growing?** MRR by month and partner | 371,178 € total collected |
| `gold_churn_by_segment` | **Which customers are we losing — and which ones matter most?** | 81.4% churn rate on converted base |
| `gold_ltv_by_channel` | **Which acquisition channel actually pays off?** Avg LTV by channel | Referral = 66.69 € avg LTV |
| `gold_email_churn_correlation` | **Does email engagement predict churn?** Churn rate by open-rate quartile | 62-point gap Q1 vs Q4 |

---

## 3 Key Insights

### 1. Email engagement is a 62-point churn predictor
Subscribers in the bottom open-rate quartile (Q1) churn at **99.8%**. Subscribers in the top quartile (Q4) churn at **37.6%** — a **+62 point difference**. This confirms that email re-engagement campaigns targeting Q1 and Q2 subscribers are a direct retention lever, not just a marketing vanity metric. Partners should trigger automated re-engagement sequences when a subscriber's engagement_score drops below threshold.

### 2. Referral is the highest-LTV acquisition channel
With an average LTV of **€66.69** across 951 converted subscribers, referral outperforms all other channels (organic search, paid social, content, direct). Referral subscribers likely arrive with higher product intent and social proof, leading to lower early churn. Budget reallocation toward referral programs (affiliate, word-of-mouth incentives) would improve blended LTV across the platform.

### 3. 4.76% payment failure rate is a silent revenue leak
Nearly **1 in 20 transactions fails**. These failures are silent: the subscriber doesn't always churn immediately, but the revenue is lost and the relationship is stressed. A smart retry logic (exponential backoff) combined with pre-expiry card update prompts could recover an estimated 2–3% of those failures — directly improving net MRR without acquiring a single new subscriber.

---

## Results Summary

```
Revenue collected (all time)    371,178 €
MRR — latest month              503 €
Converted subscribers           6,161
Global churn rate               81.45 %
Payment failure rate            4.76 %

Top partners by MRR:
  1. HealthMedia      165,718 €
  2. WellnessPress    126,975 €
  3. NutriDigital      78,484 €

Best LTV channel:   referral  →  66.69 € avg LTV
Email ↔ Churn gap:  Q1 = 99.8% churn  /  Q4 = 37.6% churn  (+62 pts)
```

---

---

## Run Locally

```bash
# Install dependencies
pip install duckdb

# Generate synthetic data (optional — data already included)
python generate_data.py

# Run the full pipeline
python pipeline.py
```

**Output:** Parquet files in `data/bronze/`, `data/silver/`, `data/gold/` + KPI summary in terminal.

---

## Repository Structure

```
propulse-analytics-portfolio/
├── pipeline.py          # Full Bronze → Silver → Gold pipeline
├── generate_data.py     # Synthetic dataset generator
├── data/
│   ├── bronze/          # Raw CSVs + Parquet snapshots
│   ├── silver/          # Cleaned, typed, enriched tables
│   └── gold/            # Analytical KPI tables
└── README.md
```
