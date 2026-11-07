# Books Lakehouse — Architecture & Design

An end-to-end lakehouse for the Book-Crossing dataset, built on Databricks: medallion architecture (bronze → silver → gold), orchestrated as a **Declarative Automation Bundle (DAB)**, with an **AI/BI dashboard** as the serving layer.

---

## 1. High-level architecture

```mermaid
flowchart LR
    subgraph SRC["Book-Crossing CSV exports"]
        A1["BX-Books"]
        A2["BX-Book-Ratings"]
        A3["BX-Users"]
    end

    subgraph BR["BRONZE — raw, as ingested"]
        B1["books_raw"]
        B2["ratings_raw"]
        B3["users_raw"]
    end

    subgraph SI["SILVER — validated + deduplicated"]
        C1["books"]
        C2["ratings"]
        C3["users"]
        C4[("quarantine: *_invalid")]
    end

    subgraph GO["GOLD — serving"]
        D1{{"books_rating_stats<br/>materialized view"}}
    end

    DASH["AI/BI Dashboard<br/>Books Lakehouse Overview"]

    A1 -->|"Auto Loader"| B1
    A2 -->|"Auto Loader"| B2
    A3 -->|"Auto Loader"| B3
    B1 -->|"silver pipeline +<br/>validity checks"| C1
    B2 -->|"silver pipeline +<br/>validity checks"| C2
    B3 -->|"silver pipeline +<br/>Auto CDC (SCD1)"| C3
    B1 --> C4
    B2 --> C4
    B3 --> C4
    C1 --> D1
    C2 --> D1
    C1 --> DASH
    C2 --> DASH
    C3 --> DASH
    D1 --> DASH
    BR -.->|"row counts"| DASH
```

**One rule drives the whole design:** bad rows are **quarantined, never dropped**. Every stream keeps a parallel `*_invalid` table so data-quality issues stay visible and debuggable.

---

## 2. Repository layout (DAB)

The bundle name is `books_dataset` with two targets — **dev** (`books_dataset_dev`, default) and **prod** (`books_dataset_prod`). A single `catalog` variable switches environments.

```text
books_dataset/
├── databricks.yml                        # bundle + targets, ${var.catalog}, ${var.warehouse_id}
├── variables.yml                         # variable defaults
├── assets/
│   ├── ingestion_pipeline.yml            # bronze Auto Loader pipeline
│   ├── silver_pipelines.yml              # 3 pipelines: books / ratings / users
│   ├── checks.yml                        # job that deploys the UC SQL check functions
│   ├── gold_pipeline.yml                 # gold materialized view pipeline
│   └── dashboards.yml                    # dashboard resource (dataset_catalog per target)
├── dashboards/
│   └── Books_Lakehouse_Overview.lvdash.json
├── src/books_dataset/
│   ├── bronze/
│   │   └── autoloader.py                 # CSV ingestion from landing volume
│   ├── silver/
│   │   ├── tables/
│   │   │   ├── books.sql
│   │   │   ├── ratings.sql
│   │   │   └── users.sql                 # Auto CDC → SCD1 streaming table + quarantine
│   │   └── checks/
│   │       └── validity_checks.sql       # UC functions: valid_book_data / valid_ratings_data / valid_users_data
│   └── gold/
│       └── views/
│           └── books_rating_stats.sql    # book-grain rating stats materialized view
└── infra/terraform/                      # workspace-level infra (catalogs, warehouses)
```

---

## 3. Layer by layer

### Bronze — raw as ingested

* One managed table per source file: `bronze.books_raw`, `bronze.ratings_raw`, `bronze.users_raw`.
* **Original column names preserved** (e.g. `Book-Author`, `Year-Of-Publication`, `ISBN`) plus ingestion metadata (`_ingestion_time`).
* Ingested with **Auto Loader** from a landing volume; checkpoints and inferred schemas live in dedicated volumes (`bronze.checkpoints`, `bronze.schemas`).

### Silver — validated, deduplicated, normalized

Each stream is a Lakeflow Spark Declarative Pipeline producing two streaming tables:

| Stream | Passed (`silver.<name>`) | Quarantined (`silver.<name>_invalid`) | Quality gate |
| --- | --- | --- | --- |
| Books | one row per ISBN, latest wins | failed `valid_book_data` | `Year-Of-Publication` → `TRY_CAST` to INT, non-null author/publisher, … |
| Ratings | one row per user-book pair, latest wins | failed `valid_ratings_data` | rating in 0–10, known ISBN/user shape, … |
| Users | one row per user via **Auto CDC (SCD1)**, keyed on `user_id` | failed `valid_users_data` | age sanity, location split into country/city/state, lower/trim/`NULLIF` normalization |

Design details worth calling out:

* **Check functions are Unity Catalog SQL UDFs** (`valid_book_data`, `valid_ratings_data`, `valid_users_data`), deployed by a dedicated job as a parameterized SQL file (with a `{{catalog}}` placeholder), so the same logic is reusable and testable outside the pipelines.
* **Latest-version-wins dedup** on natural keys (ISBN / user-book pair / user_id) driven by `_ingestion_time`.
* ANSI mode is on, so casts use `TRY_CAST` — malformed strings are quarantined rather than crashing the pipeline.
* Re-ingesting an already-known key **updates in place** instead of appending a duplicate.

### Gold — one additive materialized view

Deliberately minimal: a single metric view at **book grain** that pre-aggregates ratings so the dashboard can re-aggregate by any dimension (title, author, publisher, year) without precomputed averages.

`gold.books_rating_stats` (materialized view, one row per ISBN):

| Column | Meaning |
| --- | --- |
| `isbn`, `book_title`, `book_author`, `publisher`, `year_of_publication` | book attributes (from silver books) |
| `total_ratings` | all ratings received (explicit + implicit) |
| `explicit_ratings` | ratings with value > 0 (the 1–10 scale) |
| `implicit_ratings` | ratings with value = 0 |
| `sum_ratings` | sum of explicit rating values |

The core principle — **store additive quantities, compute averages at query time**:

```sql
Avg Explicit Rating = ROUND(SUM(sum_ratings) / NULLIF(SUM(explicit_ratings), 0), 2)
```

Because both operands are additive, this recomputes correctly under *any* grouping (by author, publisher, period, …) and under any filter combination. Precomputed per-book averages would silently produce wrong averages when re-averaged across books. (Distinct-rater counts are intentionally **not** stored — they are non-additive and would be misleading when summed.)

---

## 4. Data model

```mermaid
erDiagram
    BOOKS ||--o{ RATINGS : "receives"
    USERS ||--o{ RATINGS : "gives"

    BOOKS {
        string isbn PK
        string book_title
        string book_author
        string publisher
        int year_of_publication
    }
    RATINGS {
        string user_id FK
        string isbn FK
        int book_rating "0 = implicit, 1-10 = explicit"
    }
    USERS {
        string user_id PK
        int age
        string country
    }
    BOOKS_RATING_STATS {
        string isbn PK
        bigint total_ratings
        bigint explicit_ratings
        bigint implicit_ratings
        bigint sum_ratings
    }
```

---

## 5. Dashboard design — Books Lakehouse Overview

A single-page AI/BI dashboard built on **four deliberately simple SQL datasets**, each mapped to a specific widget group:

| # | Dataset | Source | Purpose |
| --- | --- | --- | --- |
| 1 | **Gold Books (MV)** | `gold.books_rating_stats` + publication-period parameter | KPI, top-10 rankings, author/publisher filters |
| 2 | **Layer Counts** | one `UNION ALL` of row counts per stream x layer (bronze / silver / quarantined / gold), period-guarded | Ratings KPI, three rows-per-layer bar charts |
| 3 | **Silver Users** | plain `SELECT` from `silver.users` (+ `age_bin`, normalized country names) | Users KPI, age histogram, country map, country filter |
| 4 | **Top Users by Activity** | `silver.ratings` grouped by user (explicit count, total count), top 10 each | two user tables, **unfiltered** |

### Widget and filter wiring

```mermaid
flowchart TB
    subgraph F["Global filters"]
        F1["Publication Period<br/>(date range -> pub_year_range)"]
        F2["Author"]
        F3["Publisher"]
        F4["Country"]
    end

    subgraph D["Datasets"]
        DS1["Gold Books (MV)"]
        DS2["Layer Counts"]
        DS3["Silver Users"]
        DS4["Top Users by Activity<br/>(unfiltered)"]
    end

    subgraph W["Widgets"]
        K1["KPI: Total Books"]
        K2["KPI: Total Ratings"]
        K3["KPI: Total Users"]
        R1["Top 10 Books - avg explicit rating"]
        R2["Top 10 Authors - avg explicit rating"]
        R3["Top 10 Publishers - avg explicit rating"]
        T1["Most Active Users (unfiltered)"]
        T2["Highest Buyers (unfiltered)"]
        H["User Age Distribution<br/>(10-year bins, 0-120)"]
        M["Users by Country (map)"]
        L["3 x rows-per-layer bar charts<br/>(Books / Ratings / Users)"]
    end

    DS1 --> K1
    DS1 --> R1
    DS1 --> R2
    DS1 --> R3
    DS2 --> K2
    DS2 --> L
    DS3 --> K3
    DS3 --> H
    DS3 --> M
    DS4 --> T1
    DS4 --> T2

    F1 -.-> DS1
    F1 -.-> DS2
    F2 -.-> DS1
    F3 -.-> DS1
    F4 -.-> DS3
```

Key behaviour:

* **Rankings** group the gold MV by book / author / publisher, filter to books with **>= 3 explicit ratings**, and use the shared `Avg Explicit Rating` measure — the same additive SUM/COUNT formula, so rankings stay correct under every filter.
* **The two user tables are intentionally exempt from all filters** (widget-level ignore-filters): they always show the overall top-10 raters and top-10 buyers, and their titles say so with an `(unfiltered)` suffix.
* **Country names are normalized in SQL** for geocoding: `INITCAP(...)` plus a `usa -> 'United States of America'` fix-up, so the choropleth map resolves properly.
* **The age histogram** bins ages into 10-year buckets via a computed `age_bin` column in the dataset (null ages filtered out), plotted on a fixed 0-120 axis.

### Known data caveats

* Book-Crossing has **no rating timestamps** — the only time dimension is `year_of_publication`. The Publication Period filter therefore slices by publication year, never by rating date.
* The `>= 3 explicit ratings` threshold is tuned for the (small) dev dataset; with the full dataset it filters out noise books with a single rating.
* The dev environment runs on a sample of the data, so counts shown are dev-scale.

---

## 6. Deployment and environments

* `databricks bundle deploy -t dev` targets catalog `books_dataset_dev`; `-t prod` targets `books_dataset_prod`.
* The silver check functions are deployed by a dedicated job that rewrites `{{catalog}}` in the SQL file, so each environment gets its own copies of the UDFs.
* The dashboard is checked into the bundle as a resource with `dataset_catalog: ${var.catalog}` and `warehouse_id: ${var.warehouse_id}` — dataset queries reference **unqualified** table names, so the same dashboard JSON resolves against whichever catalog the target deploys to.
* The dashboard queries tables live on open; no refresh schedule is required at this dataset size.

---

## 7. Design decisions and lessons learned

1. **Quarantine, don't drop.** Every silver stream has an `*_invalid` twin; the dashboard surfaces quarantine counts as a first-class funnel stage, making data-quality regressions visible at a glance.
2. **Additive gold measures.** Store sums and counts, derive averages at query time. This is what lets three different rankings (book / author / publisher) share one materialized view and one measure.
3. **Minimal gold.** The gold layer resisted growing a family of per-question aggregates; one book-grain MV covers all of them by re-grouping.
4. **Unqualified UC function calls don't resolve the way you'd expect.** Inside a pipeline, `valid_users_data(...)` resolves against the session's current schema, not the pipeline's catalog/schema — check functions must be fully qualified (and pipelines configured with the catalog).
5. **Batch views must be read with `read` in streaming pipelines.** Defining quality-check views with `FROM STREAM(bronze.*)` inside the view fixes the otherwise-confusing "view is not a streaming view" error.
6. **ANSI mode changes error behaviour.** `CAST('x' AS INT)` throws; `TRY_CAST` routes bad values to quarantine instead.
7. **Widgets accept plain columns; datasets accept expressions.** Precomputed bins (like `age_bin`) belong in the dataset SQL — the widget layer strips computed field expressions.
8. **Dashboard tags can't be set from the bundle (yet).** They live on the deployed dashboard object and are managed in the UI; `bundle deploy` preserves them.

