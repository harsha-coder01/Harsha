# UMC Backend ETL Developer Workflow

This guide is for developers working inside `UMC_Backend_ETL`. The repository is a unified Flask API and PySpark runtime.

## Repository Layout

```text
UMC_Backend_ETL/
  app/
    routes/
      health_routes.py     # GET /etl/health
      etl_routes.py        # ETL orchestration under /etl
      connection_routes.py # Connection catalog under /etl/connections
    services/
      etl/
        connectors/        # File/table connection helpers
        docs/
          block_json_templates.md # Control-table config examples for each block
          developer_workflow.md   # This guide
          api_contracts.md        # API specification
        jars/              # JDBC/Spark connector jars loaded into Spark
        runtime/
          cli.py           # umc-etl CLI entrypoint
          spark_session.py # SparkSession creation and jar loading
          flask_spark.py   # Global SparkSession initialization for Flask
          flow_runtime.py  # Pipeline orchestration
          core_db.py       # MySQL control-table access
          planner.py       # DAG validation and execution ordering
          blocks/          # SOURCE, TARGET, and transform block implementations
        tests/             # Pytest coverage
  Dockerfile
  pyproject.toml
  run.py                   # Flask server entrypoint
```

## Setup

Run all commands from the repository root:

```powershell
uv sync
```

[project.scripts]
umc-etl = "app.services.etl.runtime.cli:main"
```

## Runtime Inputs

The CLI does not take a full pipeline definition JSON. It takes IDs and runtime
options, then `runtime/flow_runtime.py` reads the actual flow, step, edge, and
connection metadata from the MySQL control tables.

Use `docs/block_json_templates.md` when preparing `DATA_FLOW_STEP.config` values
for the control tables or frontend payload generation.

## MySQL Configuration

Set the environment variables for your local database. Do not commit secrets.

```bash
export DB_HOST=localhost
export DB_PORT=3306
export DB_DATABASE=umc_etl_control
export DB_USER=root
export DB_PASSWORD=your_password
```

## API Access

You can trigger jobs via the Flask API, which abstracts the CLI calls. All HTTP endpoints share the `/etl` prefix (see `API_PREFIX` in `app/config.py`). Route groups:

| Group | Base path | Module |
|-------|-----------|--------|
| Health | `/etl` | `app/routes/health_routes.py` |
| ETL orchestration | `/etl` | `app/routes/etl_routes.py` |
| Connection catalog | `/etl/connections` | `app/routes/connection_routes.py` |

**Start the Flask Server:**
```powershell
uv run run.py
```

See [api_contracts.md](./api_contracts.md) for the full endpoint index and curl/payload examples for `POST /etl/preview` and `POST /etl/run`.

## CLI Debugging (Direct Execution)

If you need to bypass the API for local debugging, you can use the CLI directly. 
The CLI reads the database credentials from environment variables automatically.

Run preview:
```powershell
uv run umc-etl --master "local[*]" --jars-dir app/services/etl/jars --stop-spark preview --request '{"data_flow_id": 1, "step_id": 3, "write_schema_contract": true}'
```

Run the full flow:
```powershell
uv run umc-etl --master "local[*]" --jars-dir app/services/etl/jars --stop-spark run --run-id 210 --request '{"data_flow_id": 1, "trigger_type": "MANUAL", "submitted_by": "developer", "runtime_options": {"collect_counts": true, "fail_fast": true}}'
```

## Jars

Place JDBC and Spark connector jars in:

```text
app/services/etl/jars
```

At runtime, `runtime/spark_session.py` loads every `*.jar` from that folder into:

- `spark.jars`
- `spark.driver.extraClassPath`
- `spark.executor.extraClassPath`

For MySQL JDBC source or target work, place MySQL Connector/J in `jars/`.

## Block Development

Block implementations live in `runtime/blocks`.

Current block responsibilities:

| Block | File |
|---|---|
| SOURCE | `runtime/blocks/source_block.py` |
| SELECT | `runtime/blocks/select_block.py` |
| FILTER | `runtime/blocks/filter_block.py` |
| TRANSFORM | `runtime/blocks/transform_block.py` |
| JOIN | `runtime/blocks/join_block.py` |
| UNION | `runtime/blocks/union_block.py` |
| AGGREGATE | `runtime/blocks/aggregate_block.py` |
| VALIDATION | `runtime/blocks/validation_block.py` |
| CROSS_VALIDATION | `runtime/blocks/cross_validation_block.py` |
| PROTECTION | `runtime/blocks/protection_block.py` |
| EXPERT_SQL | `runtime/blocks/expert_sql_block.py` |
| RECONCILIATION | `runtime/blocks/reconciliation_block.py` |
| TARGET | `runtime/blocks/sink_block.py` |

When adding or changing block behavior:

- Keep the external config shape aligned with `docs/block_json_templates.md`.
- Return `StepResult` from the block execute function.
- Put output DataFrames in named output ports such as `main`, `valid`, or
  `invalid`.
- Keep user-facing error messages safe and avoid leaking secrets.
- Use PySpark `Observation` to track row counts (e.g., records_in, records_out) when applicable, passing them as `step_observations` so `flow_runtime.py` can collect them.
- Add or update pytest coverage under `tests/`.

## Filter Grouping

The FILTER block supports simple filters and nested grouped filters. Use
`condition_group` for grouped filters:

```json
{
  "condition_group": {
    "operator": "AND",
    "conditions": [
      {"column": "country", "operator": "=", "value": "US"},
      {
        "operator": "OR",
        "conditions": [
          {"column": "amount", "operator": ">", "value": 100},
          {"column": "priority", "operator": "=", "value": "HIGH"}
        ]
      }
    ]
  }
}
```

## Protection Secrets

PROTECTION supports local literal values for development and Azure Key Vault
secret references for deployed environments. Prefer Key Vault references when a
real key or salt is needed.

Example local fallback:

```json
{
  "operations": [
    {
      "column": "email",
      "operation": "hash",
      "secret": "local-dev-salt"
    }
  ]
}
```

Example Key Vault reference:

```json
{
  "operations": [
    {
      "column": "email",
      "operation": "hash",
      "secret_ref": {
        "provider": "azure_key_vault",
        "vault_url": "https://example.vault.azure.net/",
        "secret_name": "email-hash-salt"
      }
    }
  ]
}
```

## Verification

Run these checks before handing work back:

```powershell
uv run ruff check runtime connectors tests
uv run pytest -q
uv run umc-etl --help
uv build
```

The Spark-backed pytest suite can take a few minutes on Windows.

- Do not create or migrate MySQL control tables from the runtime package.
- Keep MySQL control-table reads and writes centralized in
  `app/services/etl/runtime/core_db.py`.
- Keep jars in `app/services/etl/jars/`; do not vendor jar binaries into Python packages.
- Do not change source or target connector behavior unless the task explicitly
  asks for source or target work.
- Do not commit local request files, DB config files, secrets, `.venv`, build
  output, or caches.
