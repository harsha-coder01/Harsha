# UMC ETL API Contracts

This document is the canonical reference for all HTTP endpoints exposed by the UMC Flask + PySpark ETL backend. The Flask API serves as a control plane for launching and previewing Spark ETL jobs within the same container.

## Base URL and routing

All endpoints share a single application prefix configured in `app/config.py`:

| Setting | Default | Env override |
|---------|---------|--------------|
| `API_PREFIX` | `/etl` | `API_PREFIX` |

Local default base URL: `http://localhost:5000/etl`

### Route groups

| Group | Prefix | Blueprint |
|-------|--------|-----------|
| Health | `/etl` | `health_bp` |
| ETL orchestration | `/etl` | `etl_bp` |
| Connection catalog | `/etl/connections` | `connection_bp` |

Prefixes are applied centrally in `app/__init__.py`. Individual blueprint files define route suffixes only.

### Endpoint index

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/etl/health` | Service health check |
| `POST` | `/etl/preview` | Preview a single step (sync, Spark) |
| `POST` | `/etl/run` | Submit a full flow run (async, Spark) |
| `POST` | `/etl/run/<run_id>/cancel` | Cancel a queued or running flow |

| `POST` | `/etl/connections/<conn_id>/test` | Test connectivity |
| `POST` | `/etl/connections/<conn_id>/schemas` | List schemas/databases |
| `POST` | `/etl/connections/<conn_id>/tables` | List tables in a schema |
| `POST` | `/etl/connections/<conn_id>/discover` | Column metadata, primary keys, and schema contract (read-only, no DB write) |
| `POST` | `/etl/connections/<conn_id>/objects` | Discover schema + INSERT new row into OBJECT_REGISTRY (no Spark) |
| `POST` | `/etl/connections/<conn_id>/refresh-schema` | Full schema refresh via Spark (UPDATE existing OBJECT_REGISTRY row) |

---

## Health

### GET `/etl/health`

Liveness check. No request body.

**Response (200 OK)**

```json
{
  "status": "success",
  "status_code": 200,
  "message": "API is running and in good condition",
  "data": {
    "service": "UMC_Backend_ETL",
    "status": "healthy"
  }
}
```

---

## ETL orchestration

### POST `/etl/preview`

Execute the upstream path of a specific step and return sample rows synchronously.

**Request body**

```json
{
  "data_flow_id": 1,
  "step_id": 3,
  "write_schema_contract": true
}
```

**Response (200 OK)**

Returns the schema inferred from Spark and the sample rows from the DataFrame.

```json
{
  "status": "success",
  "status_code": 200,
  "message": "Preview successful",
  "data": {
    "status": "SUCCESS",
    "data_flow_id": 1,
    "step_id": 3,
    "schema_contract": {
      "type": "struct",
      "fields": [
        {"name": "order_id", "type": "long", "nullable": true, "metadata": {}}
      ]
    },
    "runtime": {
      "steps_executed": ["read_orders", "filter_orders"]
    },
    "warnings": []
  }
}
```

### POST `/etl/run`

Submit a full data flow execution DAG to run asynchronously in the background.

**Request body**

```json
{
  "data_flow_id": 1,
  "trigger_type": "MANUAL",
  "submitted_by": "developer",
  "runtime_options": {
    "collect_counts": true,
    "fail_fast": true
  }
}
```

**Response (202 Accepted)**

Returns immediately with the `run_id`. Poll `GET /etl/run/<run_id>/status` (or the MySQL `DATA_FLOW_RUN` table) to track progress and completion status.

```json
{
  "status": "success",
  "status_code": 202,
  "message": "Flow submitted",
  "data": {
    "status": "QUEUED",
    "run_id": 42
  }
}
```

### POST `/etl/run/<run_id>/cancel`

Forcefully abort a currently queued or running ETL data flow.

**Request body**

*None*

**Response (200 OK)**

Updates the control database status to `CANCELLED` and safely terminates the PySpark background process.

```json
{
  "status": "success",
  "status_code": 200,
  "message": "Run cancelled",
  "data": {
    "status": "CANCELLED",
    "run_id": 42
  }
}
```


---

## Connection catalog

Connection endpoints use direct JDBC/mysql.connector calls for fast metadata responses, except `refresh-schema` which spawns a Spark subprocess.

### POST `/etl/connections/<conn_id>/test`

Test connectivity. No request body.

**Response (200 OK)**

```json
{
  "status": "success",
  "status_code": 200,
  "message": "Connection successful",
  "data": {
    "connection_id": 1,
    "latency_ms": 42
  }
}
```

### POST `/etl/connections/<conn_id>/schemas`

List schemas/databases. No request body.

**Response (200 OK)**

```json
{
  "status": "success",
  "status_code": 200,
  "message": "Schemas listed",
  "data": {
    "schemas": [
      {"schema_name": "sales", "catalog": "def"}
    ]
  }
}
```

### POST `/etl/connections/<conn_id>/tables`

List tables in a schema.

**Request body**

```json
{
  "schema_name": "sales"
}
```

**Response (200 OK)**

```json
{
  "status": "success",
  "status_code": 200,
  "message": "Tables listed",
  "data": {
    "schema_name": "sales",
    "tables": [
      {
        "table_name": "orders",
        "table_type": "BASE TABLE",
        "schema_name": "sales"
      }
    ]
  }
}
```

### POST `/etl/connections/<conn_id>/discover`

Discover column metadata, primary keys, and Spark-compatible schema contract for a
table. **Read-only — does NOT write to any database.**

Returns `physically_exists: false` with empty columns and schema if the table does not
exist yet (instead of an error). This allows the UI to gracefully handle future target
tables that will be created on first pipeline run.

**Request body**

```json
{
  "schema_name": "sales",
  "table_name": "orders"
}
```

**Response (200 OK)**

```json
{
  "status": "success",
  "status_code": 200,
  "message": "Schema discovered",
  "data": {
    "table_name": "orders",
    "schema_name": "sales",
    "physically_exists": true,
    "primary_keys": ["order_id"],
    "columns": [
      {
        "name": "order_id",
        "data_type": "int(11)",
        "nullable": false,
        "ordinal_position": 1
      }
    ],
    "schema_contract": {
      "type": "struct",
      "fields": [
        {"name": "order_id", "type": "integer", "nullable": false}
      ]
    }
  }
}
```

**Response when table does not exist (200 OK — not a 404):**

```json
{
  "status": "success",
  "status_code": 200,
  "message": "Schema discovered",
  "data": {
    "table_name": "future_target_table",
    "schema_name": "sales",
    "physically_exists": false,
    "primary_keys": [],
    "columns": [],
    "schema_contract": {"type": "struct", "fields": []}
  }
}
```

### POST `/etl/connections/<conn_id>/objects`

Discover a table's schema and **register it as a new row in `OBJECT_REGISTRY`**.
This is the onboarding endpoint. Uses direct JDBC (no Spark). Response time target: <500ms.

Returns `409 Conflict` if the same `schema_name.table_name` is already registered on
this connection. Call `refresh-schema` to update an existing entry instead.

Can register tables that do not physically exist yet (`physically_exists: false`).
This supports pre-declaring TARGET tables that will be created on first pipeline run.

**Request body**

```json
{
  "schema_name": "HR",
  "table_name": "Employees",
  "object_name": "HR Employees Data",
  "created_by": "sandeep"
}
```

| Field | Required | Description |
|---|---|---|
| `schema_name` | Yes | Database/schema name. |
| `table_name` | Yes | Table name to register. |
| `object_name` | No | Human-readable display name. Defaults to `schema_name.table_name`. |
| `created_by` | No | Username string for audit trail. Defaults to `"system"`. |

**Response (201 Created)**

```json
{
  "status": "success",
  "status_code": 201,
  "message": "Object registered successfully",
  "data": {
    "object_id": 101,
    "object_identifier": "HR.Employees",
    "object_name": "HR Employees Data",
    "object_type": "TABLE",
    "connection_id": 5,
    "object_metadata": {
      "columns": [...],
      "schema_contract": {...},
      "primary_keys": ["emp_id"],
      "physically_exists": true
    }
  }
}
```

**Response (409 Conflict — already registered):**

```json
{
  "status": "failure",
  "status_code": 409,
  "message": "Object 'HR.Employees' already registered on connection 5 (object_id=101). Use refresh-schema to update its metadata."
}
```

### POST `/etl/connections/<conn_id>/refresh-schema`

Full schema refresh for a registered `OBJECT_REGISTRY` entry via Spark. This call may take 20–60 seconds because it boots a Spark subprocess.

**Request body**

```json
{
  "object_id": 42,
  "write_back": true
}
```

**Response (200 OK)**

```json
{
  "status": "success",
  "status_code": 200,
  "message": "Schema refreshed successfully",
  "data": {
    "status": "SUCCESS",
    "object_id": 42,
    "columns": [],
    "schema_contract": {
      "type": "struct",
      "fields": []
    },
    "sample_records": []
  }
}
```
