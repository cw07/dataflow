**Filename:** `dataflow_project_requirements.md`

---

# DataFlow Framework – Project Requirements Document

## 1. Overview

The **DataFlow** framework is a Python-based, vendor-agnostic, ORM-agnostic, and output-agnostic data service platform designed to extract, transform, and route both **real-time** and **historical** financial time series data. The framework must support flexible configuration and modular architecture to allow easy swapping of data sources, transformation logic, and output destinations with minimal code changes.

Key design principles:
- **Vendor-free**: No hard dependencies on specific data vendors.
- **ORM-free**: Support multiple ORMs (e.g., SQLAlchemy, Peewee) via configuration.
- **Output-free**: Support multiple output destinations (database, Redis, file) with pluggable interfaces.
- **Configurable & Scalable**: All behavior driven by configuration files and environment variables.

---

## 2. Core Functional Requirements

### 2.1 Real-Time Data Extractors
- All real-time extractors must inherit from a common base class (`BaseRealtimeExtractor`).
- Shared functionality includes:
  - Session initialization (e.g., WebSocket connection, authentication).
  - Message handler interface for incoming real-time messages.
- Examples: `DatabentoRealtimeExtractor`, `FluxRealtimeExtractor`.
- Each extractor maps to a specific vendor but conforms to the same interface.

### 2.2 Historical Data Extractors
- All historical extractors must inherit from a common base class (`BaseHistoricalExtractor`).
- Shared functionality includes:
  - Date-range handling.
  - Reference data fetching logic.
- Example: `BloombergHistoricalExtractor`.

### 2.3 Output Destinations
Support three primary output types:
- **Database**: Configurable ORM (SQLAlchemy or Peewee). Must auto-generate ORM models from time series definitions.
- **Redis**: Optimized for real-time data; must follow structured key/value patterns for downstream consumption.
- **File**: Write to structured formats (e.g., Parquet, JSON, CSV) with configurable paths and rotation.

### 2.4 Time Series Data Model
- Each time series is represented by a dedicated data class.
- When outputting to a database:
  - The framework must dynamically generate an ORM model based on the time series schema.
  - Use the configured ORM to persist data.

### 2.5 Database Configuration
- Supported databases: SQLite, PostgreSQL, Microsoft SQL Server.
- Connection details and ORM choice defined in `.env` via Pydantic settings.

### 2.6 Redis & File Output Patterns
- Must use standardized naming/structuring conventions (e.g., `data_source:asset_type:symbol:data_schema`) to enable downstream processing.
- File outputs should include metadata (e.g., schema, date range).

### 2.7 Transformers
- Support data transformation and indicator calculation (e.g., moving averages, volatility).
- Transformers may consume **multiple input time series**.
- Must define an abstract base class (`BaseTransformer`) with a standard `transform()` interface.
- Transformations can be chained or composed.

### 2.8 Service Architecture
- A `services/` directory contains all entry points.
- orchestrator.py is to organize and dispatch services based on time series configuration 
- Each service (e.g., `databento_realtime_service.py`, `bloomberg_historical_service.py`) runs independently—critical for cloud deployment (e.g., Kubernetes jobs, serverless functions).


### 2.9 Flexibility & Swappability
- The framework must allow:
  - Changing data vendors with minimal code changes.
  - Redirecting output from database → Redis or file → database via config.
  - Reusing extractors/transformers across different pipelines.


### 2.10 Service Launch Configuration
- All app configuration resides in a `.env` file at the project root.
- The `.env` file defines which type of database, and it's configuration, ORM type
- For each service, specify:
  - Data source (vendor).
  - Tickers/assets (fetch from Time Series Configuration).
  - Schema type.


### 2.12 Time Series Configuration
- A `time_series.csv` file defines all time series metadata:
  - `series_id`, `data_source`, `extractor_type` (realtime/historical), `schema`, `assets`, `output_destinations`, etc.
- At startup:
  1. Load `time_series.csv`.
  2. Group entries by `(extractor_type, data_source)`.
  3. Instantiate one extractor per unique `(extractor_type, data_source)` pair.
  4. Pass all relevant time series to that extractor.

---

## 3. Technical Architecture

### 3.1 Directory Structure (Finalized)

```
dataflow/
├── .env                                  # Environment configuration
├── time_series.csv                       # Master time series definition
├── src/
│   └── dataflow/
│       ├── config/
│       │   ├── __init__.py
│       │   └── settings.py               # Pydantic Settings loader
│       │
│       ├── extractors/
│       │   ├── __init__.py
│       │   ├── base.py                   # BaseExtractor (shared)
│       │   ├── historical/
│       │   │   ├── __init__.py
│       │   │   └── base_historical.py    # BaseHistoricalExtractor
│       │   │   └── bloomberg.py          # Example implementation
│       │   └── realtime/
│       │       ├── __init__.py
│       │       ├── base_realtime.py      # BaseRealtimeExtractor
│       │       ├── databento.py
│       │       └── flux.py
│       │
│       ├── orm/
│       │   ├── __init__.py
│       │   ├── base_orm.py               # Abstract ORM interface
│       │   ├── peewee.py
│       │   └── sqlalchemy.py
│       │
│       ├── outputs/
│       │   ├── __init__.py
│       │   ├── base.py                   # BaseOutput
│       │   ├── allocator.py              # Route data to correct output
│       │   ├── database/
│       │   │   ├── __init__.py
│       │   │   ├── db_manager.py
│       │   │   └── model_factory.py       # Dynamically create ORM models
│       │   ├── file/
│       │   │   ├── __init__.py
│       │   │   └── file_manager.py
│       │   └── redis/
│       │       ├── __init__.py
│       │       └── redis_manager.py
│       │
│       ├── services/
│       │   ├── __init__.py
│       │   └── orchestrator.py           # Main entry: reads config, launches services
│       │
│       ├── transformers/
│       │   ├── __init__.py
│       │   ├── base.py                   # BaseTransformer
│       │   └── indicators.py             # Example: moving average, RSI, etc.
│       │
│       └── utils/
│           └── __init__.py
```

---

## 4. Non-Functional Requirements

- **Modularity**: Each component (extractor, output, transformer) is replaceable.
- **Testability**: All base classes support mocking; services are unit-testable.
- **Performance**: Real-time extractors must handle high-throughput streams efficiently.
- **Maintainability**: Clear separation of concerns; minimal cross-component coupling.
- **Deployment**: Designed for daily batch runs (historical) and long-running processes (real-time) in cloud environments.

---

## 5. Out of Scope

- UI or dashboard components.
- Authentication/authorization beyond vendor API keys.
- Data validation beyond schema enforcement.
- Built-in scheduling (assumes external scheduler like Airflow or cron).

---

*Document Version: 1.0*  
*Last Updated: 2025-10-11*