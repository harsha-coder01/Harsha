# UMC ETL Block JSON Templates

These are the standard `DATA_FLOW_STEP.config` JSON shapes for the backend runtime.
Each block has one canonical template that includes the supported runtime keys for
that block. Optional keys can be omitted when the runtime default is acceptable.

`SOURCE` and `TARGET` configs reference `OBJECT_REGISTRY.object_id`. The runtime
loads the matching `OBJECT_REGISTRY` and `CONNECTION_REGISTRY` rows from MySQL
before executing those blocks.

## Shared Rules

- **Single Responsibility Configuration Ownership:** A configuration property must have exactly one owner. Do not duplicate properties across object metadata and block configs (e.g. `load_type` and `watermark_column` are owned exclusively by SOURCE; `write_mode` is owned exclusively by TARGET).
- Store configs as JSON objects in `DATA_FLOW_STEP.config`.
- Prefer the canonical key names shown in the templates.
- For operation collections, use arrays even when there is only one item.
- Column references are DataFrame column names unless the block explicitly says
  the value is an input alias, object id, or Spark SQL expression.
- `input_map` is not part of `config`; it belongs on `DATA_FLOW_STEP.input_map`
  and is used by multi-input blocks such as `JOIN`, `UNION`, and `EXPERT_SQL`.

## SOURCE

Reads one registered source object into a Spark DataFrame.
The Source block is exclusively responsible for determining the load type, watermark logic, and packetization logic. 

```json
{
  "source_object_id": 101,
  "load_type": "DELTA",
  "watermark_override": "updated_at",
  "initial_watermark_value": "2025-01-01 00:00:00",
  "packetization": {
    "strategy": "column",
    "column": "order_id",
    "lower_bound": 1,         
    "upper_bound": 1000000,   
    "num_partitions": 8
  },
  "read_options_override": {
    "fetchsize": "10000"
  }
}
```

#### Schema Contract

| Key | Type | Required | Description |
|---|---|---|---|
| `source_object_id` | int | Yes | Existing `OBJECT_REGISTRY.object_id`. Used to resolve connection metadata. |
| `load_type` | string | Yes | `FULL`, `DELTA`. Exclusively owned by block config. Must be explicitly provided by the UI. |
| `watermark_override` | string | No | Column name. Required if `load_type` is `DELTA`. |
| `initial_watermark_value` | string | No | Timestamp/Date string. Overrides prior checkpoint to force a specific start date. |
| `packetization` | object | No | Controls parallel JDBC reads. Strict separation from `read_options_override`. |
| `read_options_override` | object | No | Connector/Spark read option object. Merged into connector read call. Does not include partitioning. |

For `DELTA`, the runtime reads the prior completed `STEP_RUN.checkpoint` for the same step and passes its `max_watermark_value` to the connector. If `initial_watermark_value` is provided, it overrides the checkpoint.

#### Supported Packetization Strategies

**1. `column` (Numeric Range)**
Splits JDBC reads using a numeric column.
| Key | Type | Required | Description |
|---|---|---|---|
| `strategy` | string | Yes | `"column"` |
| `column` | string | Yes | The numeric column used for partitioning. |
| `lower_bound` | number | No | Minimum expected value. If omitted, the backend will dynamically calculate bounds. |
| `upper_bound` | number | No | Maximum expected value. If omitted, the backend will dynamically calculate bounds. |
| `num_partitions` | int | No | Number of parallel Spark tasks to spawn. |

**Example JSON & Behavior:**
```json
{
  "strategy": "column",
  "column": "order_id",
  "lower_bound": 1,
  "upper_bound": 1000000,
  "num_partitions": 4
}
```
*What it does:* Spark will issue 4 simultaneous background database queries, automatically dividing the bounds.
- Query 1: `WHERE order_id >= 1 AND order_id < 250000`
- Query 2: `WHERE order_id >= 250000 AND order_id < 500000`
- Query 3: `WHERE order_id >= 500000 AND order_id < 750000`
- Query 4: `WHERE order_id >= 750000 AND order_id <= 1000000`

**2. `date` (Date Range)**
Splits JDBC reads using a Date/Timestamp column.
| Key | Type | Required | Description |
|---|---|---|---|
| `strategy` | string | Yes | `"date"` |
| `column` | string | Yes | The date or timestamp column used for partitioning. |
| `lower_bound` | string | No | Minimum expected date. If omitted, the backend will dynamically calculate bounds. |
| `upper_bound` | string | No | Maximum expected date. If omitted, the backend will dynamically calculate bounds. |
| `num_partitions` | int | No | Number of parallel Spark tasks to spawn. |

**Example JSON & Behavior:**
```json
{
  "strategy": "date",
  "column": "order_date",
  "lower_bound": "2025-01-01",
  "upper_bound": "2025-01-31",
  "num_partitions": 2
}
```
*What it does:* Spark divides the 30-day range into 2 equal queries.
- Query 1: `WHERE order_date >= '2025-01-01' AND order_date < '2025-01-16'`
- Query 2: `WHERE order_date >= '2025-01-16' AND order_date <= '2025-01-31'`

**3. `categorical` (Specific Values)**
Splits JDBC reads by issuing distinct queries for specific categorical values.
| Key | Type | Required | Description |
|---|---|---|---|
| `strategy` | string | Yes | `"categorical"` |
| `column` | string | Yes | The categorical column used for partitioning. |
| `categories` | array | Yes | Array of exact string or numeric values to match. |

**Example JSON & Behavior:**
```json
{
  "strategy": "categorical",
  "column": "region",
  "categories": ["APAC", "EMEA", "NA"]
}
```
*What it does:* Spark explicitly spins up exactly 3 parallel tasks.
- Task 1: `WHERE region = 'APAC'`
- Task 2: `WHERE region = 'EMEA'`
- Task 3: `WHERE region = 'NA'`

*Note on Unlisted Categories:* If the database has `LATAM` but it is not in the JSON array, those records are completely ignored (dropped). Categorical packetization acts as both a partitioner and a strict whitelist filter.

**4. `row_count` (Sequential Batches)**
Splits API/JDBC reads into sequential batches based purely on limits.
| Key | Type | Required | Description |
|---|---|---|---|
| `strategy` | string | Yes | `"row_count"` |
| `fetch_size` | int | Yes | The number of rows to retrieve per batch. |

**Example JSON & Behavior:**
```json
{
  "strategy": "row_count",
  "fetch_size": 50000
}
```
*What it does:* Bypasses column partitioning and instead sequentially reads batches of 50,000 records. Useful for APIs or DBs where column partitioning is impossible, but strictly single-threaded (no parallel speedup).

## SELECT

Purpose: picks columns, renames them, and drops unwanted ones.

### UI Column Mapping & Deselection
The frontend should display an interactive Data Grid when upstream schema is available:

| Select | Input Column (Read Only) | Output Column (Editable) |
| :--- | :--- | :--- |
| ☑️ | `A` | `A` |
| ☑️ | `B` | `New_B` |
| 🔲 | `C` | `C` |
| ☑️ | `D` | `D` |

*   **Deselection:** If the user unchecks the box (e.g., `C`), the column is completely omitted from the payload (deselected).
*   **Renaming:** If the user changes the Output Column text (e.g., `B` to `New_B`), it is added to the `rename` dictionary.
*   **Passthrough:** If the name is unchanged (e.g., `A` and `D`), it is only added to the `columns` array.
*   `allow_missing_columns` should NOT be exposed in the UI (the backend safely defaults it to `false`).

```json
{
  "columns": ["A", "B", "D"],
  "rename": {
    "B": "New_B"
  }
}
```

#### Schema Contract

| Key | Type | Required | Description |
|---|---|---|---|
| `columns` | array[string] | Yes | An array of strictly selected columns. Any upstream column NOT present in this array is automatically dropped by the backend. |
| `rename` | object (dict) | No | A dictionary mapping `old_name` to `new_name`. Only include columns where the name has actually changed. |

## FILTER

Purpose: Filters rows with a nested condition group.

```json
{
  "condition_group": {
    "combine_with": "AND",
    "conditions": [
      {
        "column": "amount",
        "operator": ">=",
        "value": 40
      },
      {
        "combine_with": "OR",
        "conditions": [
          {
            "column": "region",
            "operator": "=",
            "value": "APAC"
          },
          {
            "column": "status",
            "operator": "IN",
            "value": ["COMPLETE", "CANCELLED"]
          }
        ]
      },
      {
        "column": "email",
        "operator": "IS_NOT_NULL"
      },
      {
        "column": "order_date",
        "operator": "BETWEEN",
        "value": ["2026-01-01", "2026-12-31"]
      }
    ]
  }
}
```

#### Condition Group (Root or Nested)

| Key | Type | Required | Description |
|---|---|---|---|
| `condition_group` | object | Recommended | Root nested group object. |
| `combine_with` | string | No | `AND`, `OR` (Defaults to `AND`). |
| `conditions` | array[object] | Yes | Array of Leaf Conditions or nested Condition Groups. |
| `filter` | object | Alternative | Runtime alias for `condition_group`. |

#### Leaf Condition

| Key | Type | Required | Description |
|---|---|---|---|
| `column` | string | Yes | Column to filter on. |
| `operator` | string | Yes | `=`, `==`, `!=`, `<>`, `>`, `>=`, `<`, `<=`, `LIKE`, `IN`, `NOT_IN`, `IS_NULL`, `IS_NOT_NULL`, `BETWEEN`. |
| `value` | any | Conditional | The value to compare against. Not required for `IS_NULL`/`IS_NOT_NULL`. Must be an array for `IN`, `NOT_IN`, and `BETWEEN`. |

## TRANSFORM

Performs Prepare-style column transformations in a strict sequential order.
At least one operation must be present in the `operations` array.

```json
{
  "operations": [
    {
      "operation": "expression",
      "output_column": "gross_amount",
      "expression": "amount * qty"
    },
    {
      "operation": "change_type",
      "column": "qty",
      "new_type": "int",
      "on_parse_error": "set_to_null"
    },
    {
      "operation": "replace_value",
      "column": "status",
      "match_mode": "regex",
      "match": "complete",
      "replace_with": "DONE",
      "case_sensitive": false
    },
    {
      "operation": "fill_null",
      "column": "email",
      "fill_with": "missing@example.com"
    },
    {
      "operation": "text_case",
      "columns": ["status", "category"],
      "case": "lower"
    },
    {
      "operation": "trim",
      "columns": ["email", "status"],
      "side": "both"
    },
    {
      "operation": "regex_replace",
      "column": "phone",
      "pattern": "[^0-9]",
      "replacement": "",
      "case_sensitive": true
    },
    {
      "operation": "extract",
      "source_column": "ticket",
      "target_column": "ticket_prefix",
      "pattern": "^([A-Z])",
      "capture_group": 1
    },
    {
      "operation": "parse_date",
      "column": "event_date_text",
      "target_column": "event_date",
      "to": "date",
      "format": "yyyy-MM-dd",
      "on_parse_error": "set_to_null"
    },
    {
      "operation": "padding",
      "columns": ["ticket"],
      "pad_type": "lpad",
      "length": 10,
      "pad_string": "0"
    },
    {
      "operation": "numeric_scaling",
      "column": "gross_amount",
      "method": "min-max",
      "range": [0, 100]
    }
  ]
}
```

Operation order is strictly sequential: each item in the `operations` array is executed in the exact order specified.
Existing columns are overwritten when an operation writes to the same name.
Most operations (except `expression`, `extract`, `parse_date`, `numeric_scaling`) support applying the exact same transformation to multiple columns at once by using a `columns` array of strings instead of a single `column` string.

#### Supported Operations

**1. `expression`**
Evaluates a Spark SQL expression to create or overwrite a column.
| Key | Type | Required | Description |
|---|---|---|---|
| `operation` | string | Yes | `"expression"` |
| `output_column` | string | Yes | New or overwritten output column. Aliases: `target_column`, `column`, `col_name`, `colname`. |
| `expression` | string | Yes | Spark SQL expression text. Aliases: `expression_sql`, `expr`. |

**2. `change_type`**
Casts columns to a new data type.
| Key | Type | Required | Description |
|---|---|---|---|
| `operation` | string | Yes | `"change_type"` |
| `column` or `columns` | string/array | Yes | Column(s) to cast. |
| `new_type` | string | Yes | `string`, `int`, `bigint`, `double`, `float`, `boolean`, `date`, `timestamp`. Aliases: `type`, `data_type`, `to`. |
| `on_parse_error` | string | No | `set_to_null` (default), `raise_error`. |

**3. `replace_value`**
Replaces matched values in column(s) with a new value.
| Key | Type | Required | Description |
|---|---|---|---|
| `operation` | string | Yes | `"replace_value"` |
| `column` or `columns` | string/array | Yes | Column(s) to transform. |
| `match` | string/number/bool/null | Yes | Value or pattern to match. `null` is allowed only for `exact` mode. Aliases: `match_value`, `given_value`, `value`. |
| `replace_with` | string/number/bool/null | Yes | Value written when a match is found. Aliases: `replacement`, `replace_value`, `new_value`. |
| `match_mode` | string | No | `exact` (default), `contains`, `starts_with`, `ends_with`, `regex`. |
| `case_sensitive` | bool | No | `true` (default), `false`. |

**4. `fill_null`**
Replaces null values with a specified literal value.
| Key | Type | Required | Description |
|---|---|---|---|
| `operation` | string | Yes | `"fill_null"` |
| `column` or `columns` | string/array | Yes | Column(s) to transform. |
| `fill_with` | string/number/bool | Yes | Value used when the column is null. Aliases: `fill_value`, `value`. |

**5. `text_case`**
Converts the casing of text column(s).
| Key | Type | Required | Description |
|---|---|---|---|
| `operation` | string | Yes | `"text_case"` |
| `column` or `columns` | string/array | Yes | Column(s) to transform. |
| `case` | string | Yes | `lower`, `upper`, `title`. Aliases: `case_type`, `to`. |

**6. `trim`**
Trims whitespace from text column(s).
| Key | Type | Required | Description |
|---|---|---|---|
| `operation` | string | Yes | `"trim"` |
| `column` or `columns` | string/array | Yes | Column(s) to transform. |
| `side` | string | No | `both` (default), `left`, `right`. |

**7. `regex_replace`**
Replaces text matching a regex pattern.
| Key | Type | Required | Description |
|---|---|---|---|
| `operation` | string | Yes | `"regex_replace"` |
| `column` or `columns` | string/array | Yes | Column(s) to transform. |
| `pattern` | string | Yes | Valid regex pattern. Aliases: `regex`. |
| `replacement` | string | No | Replacement text (defaults to `""`). `null` is treated as empty string. |
| `case_sensitive` | bool | No | `true` (default), `false`. |

**8. `extract`**
Extracts text from a column using a regex capture group.
| Key | Type | Required | Description |
|---|---|---|---|
| `operation` | string | Yes | `"extract"` |
| `source_column` | string | Yes | Source column to read. Aliases: `source_col`. |
| `pattern` | string | Yes | Valid regex pattern. Aliases: `regex`. |
| `target_column` | string | No | Output column to write (defaults to source column). Aliases: `target_col`, `output_column`. |
| `capture_group` | int | No | Regex capture group index to return (defaults to `1`). |

**9. `parse_date`**
Parses a text column into a date or timestamp.
| Key | Type | Required | Description |
|---|---|---|---|
| `operation` | string | Yes | `"parse_date"` |
| `column` | string | Yes | Source column to parse. |
| `to` | string | No | `date` (default), `timestamp`. |
| `target_column` | string | No | Output column to write (defaults to source column). |
| `format` | string | No | Spark datetime pattern (e.g. `yyyy-MM-dd`). If omitted, Spark defaults are used. |
| `on_parse_error` | string | No | `set_to_null` (default), `raise_error`. |

**10. `padding`**
Pads text column(s) to a specific length.
| Key | Type | Required | Description |
|---|---|---|---|
| `operation` | string | Yes | `"padding"` |
| `column` or `columns` | string/array | Yes | Column(s) to transform. |
| `length` | int | Yes | Total length of the output string. Aliases: `pad_length`. |
| `pad_type` | string | No | `lpad` (default), `rpad`. Aliases: `direction`. |
| `pad_string` | string | No | String to pad with (defaults to `" "`). Aliases: `pad_with`. |

**11. `numeric_scaling`**
Applies numerical scaling to a column.
| Key | Type | Required | Description |
|---|---|---|---|
| `operation` | string | Yes | `"numeric_scaling"` |
| `column` | string | Yes | Column to scale. |
| `method` | string | Yes | `min-max`, `z-score`, `robust`. Aliases: `scale_method`. |
| `target_column` | string | No | Output column (defaults to source column). |
| `range` | array | No | `[min, max]` target range for `min-max` scaling (defaults to `[0, 1]`). |

Supported target types for `change_type`: `string`, `str`, `int`, `integer`,
`bigint`, `long`, `double`, `float`, `boolean`, `bool`, `date`, `timestamp`.

Expression text is evaluated with Spark SQL expression semantics and must pass
runtime safety checks. Forbidden expression tokens include statement separators,
SQL comments, and mutation/DDL keywords.

## JOIN

Purpose: Joins two input streams (left and right) based on specific conditions.

```json
{
  "join_type": "left",
  "left_alias": "orders",
  "right_alias": "customers",
  "conditions": [
    {
      "left_column": "customer_id",
      "operator": "=",
      "right_column": "customer_id"
    },
    {
      "left_column": "region",
      "operator": "=",
      "right_column": "region"
    }
  ],
  "deduplicate_right_columns": true
}
```

#### Schema Contract

| Key | Type | Required | Description |
|---|---|---|---|
| `join_type` | string | Yes | `inner`, `left`, `right`, `full`, `left_semi`, `left_anti`, `cross` (Defaults to `inner`). |
| `left_alias` | string | Yes | The alias of the left input stream (defined in `input_map`). |
| `right_alias` | string | Yes | The alias of the right input stream (defined in `input_map`). |
| `conditions` | array[object] | Conditional | Array of join condition objects. Required for all joins except `cross`. |
| `deduplicate_right_columns` | bool | No | Drops duplicate right-side join key columns. (Defaults to `true`). |

#### Join Condition

| Key | Type | Required | Description |
|---|---|---|---|
| `left_column` | string | Yes | Column name from the left alias. |
| `operator` | string | Yes | `=`, `==`, `!=`, `<>`, `>`, `>=`, `<`, `<=`. |
| `right_column` | string | Yes | Column name from the right alias. |

## UNION

Purpose: Unites multiple input streams vertically.

```json
{
  "mode": "unionByName",
  "allow_missing_columns": true,
  "deduplicate": false
}
```

#### Schema Contract

| Key | Type | Required | Description |
|---|---|---|---|
| `mode` | string | No | `unionByName` or `by_name`. (Defaults to `unionByName`). |
| `allow_missing_columns` | bool | No | If `true`, missing columns across inputs are filled with nulls. (Defaults to `false`). |
| `deduplicate` | bool | No | If `true`, applies `distinct()` after the union. (Defaults to `false`). |

Inputs are resolved from `input_0`, `input_1`, ... aliases first; otherwise the runtime uses sorted input aliases from `DATA_FLOW_STEP.input_map`.

## AGGREGATE

Purpose: Groups data and computes aggregate measures.

```json
{
  "group_by": ["region", "customer_segment"],
  "aggregations": [
    {
      "output_column": "orders",
      "function": "count",
      "column": "*"
    },
    {
      "output_column": "customers",
      "function": "countDistinct",
      "column": "customer_id"
    },
    {
      "output_column": "total_amount",
      "function": "sum",
      "column": "amount"
    },
    {
      "output_column": "avg_qty",
      "function": "avg",
      "column": "qty"
    },
    {
      "output_column": "max_amount",
      "function": "max",
      "column": "amount"
    }
  ]
}
```

#### Schema Contract

| Key | Type | Required | Description |
|---|---|---|---|
| `group_by` | array[string] | No | Columns to group by. If omitted, aggregates over the entire dataset. |
| `aggregations` | array[object] | Yes | Array of aggregation definition objects. |

#### Aggregation

| Key | Type | Required | Description |
|---|---|---|---|
| `output_column` | string | Yes | The name of the new column containing the aggregate result. |
| `function` | string | Yes | `sum`, `count`, `countDistinct`, `avg`, `min`, `max`, `first`, `last`. |
| `column` | string | Conditional | The input column to aggregate. Can be `*` for `count`. |

## VALIDATION

Splits records into `valid` and `rejected` output ports based on configurable rules.
`REJECT` rules split rows. `WARN` rules emit warnings without splitting rows.
`FAIL` stops the pipeline immediately — used only for `SCHEMA_CHECK` and aggregate rules.

Rules are processed in list order. For `REJECT` rules, the first failing rule wins
the `_rejection_reason` column on the rejected row. For `WARN`/`FAIL` rules, warnings
are collected in list order before any `FAIL` raises.

```json
{
  "rules": [
    {
      "rule_type": "RANGE_CHECK",
      "rule_name": "amount_non_negative",
      "column": "amount",
      "min": 0,
      "max": 999999,
      "on_fail": "REJECT"
    },
    {
      "rule_type": "NULL_CHECK",
      "rule_name": "customer_required",
      "column": "customer_id",
      "on_fail": "REJECT"
    },
    {
      "rule_type": "REGEX_CHECK",
      "rule_name": "email_format",
      "column": "email",
      "pattern": "EMAIL",
      "on_fail": "REJECT"
    },
    {
      "rule_type": "EXPRESSION",
      "rule_name": "email_present",
      "expression": "email IS NOT NULL",
      "on_fail": "WARN"
    },
    {
      "rule_type": "SCHEMA_CHECK",
      "rule_name": "schema_check",
      "required_columns": ["order_id", "customer_id", "amount"],
      "expected_schema": {
        "fields": [
          {"name": "order_id",    "type": "integer"},
          {"name": "customer_id", "type": "string"},
          {"name": "amount",      "type": "double"}
        ]
      },
      "on_fail": "WARN"
    },
    {
      "rule_type": "ROW_COUNT_CHECK",
      "rule_name": "min_row_count",
      "on_fail": "FAIL",
      "min_count": 1
    }
  ]
}
```

| Rule Type | Level | Required Keys | Optional Keys / Values |
|---|---|---|---|
| `EXPRESSION` | Row | `expression` | `on_fail`: `REJECT`, `WARN`. Must include `IS NOT NULL` guards for nullable columns. |
| `NULL_CHECK` | Row | `column` | `on_fail`: `REJECT`, `WARN`. |
| `RANGE_CHECK` | Row | `column`, `min`, `max` | `on_fail`: `REJECT`, `WARN`. Bounds are inclusive. Both `min` and `max` are required — no single-sided form. |
| `REGEX_CHECK` | Row | `column`, `pattern` | `on_fail`: `REJECT`, `WARN`. `pattern` accepts raw regex or preset name: `EMAIL`, `PHONE`, `PAN`, `AADHAAR`, `IFSC`, `ZIP`, `URL`. |
| `LENGTH_CHECK` | Row | `column` | `min_length` defaults to `0`; `max_length` unbounded when omitted. `on_fail`: `REJECT`, `WARN`. |
| `DATA_TYPE_CHECK` | Row | `column`, `expected_type` | `expected_type`: `integer`, `long`, `double`, `decimal`, `date`, `timestamp`, `boolean`. `on_fail`: `REJECT`, `WARN`. |
| `DATE_FORMAT_CHECK` | Row | `column`, `date_format` | Spark date format string e.g. `yyyy-MM-dd`. `on_fail`: `REJECT`, `WARN`. |
| `DUPLICATE_CHECK` | Row | `column` or `columns` | Keeps first occurrence (natural order); rejects the rest. `on_fail`: `REJECT`, `WARN`. |
| `UNIQUE_CHECK` | Row | `column` or `columns` | Rejects all occurrences of a duplicate value. `on_fail`: `REJECT`, `WARN`. |
| `PRIMARY_KEY_CHECK` | Row | `column` or `columns` | All columns must be NOT NULL and unique in combination. `on_fail`: `REJECT`, `WARN`. |
| `SCHEMA_CHECK` | Dataset | — (at least one of the optional keys should be set) | `required_columns`: array of column names. `expected_schema`: `{"fields": [{"name": "...", "type": "..."}]}`. `on_fail`: `WARN`, `FAIL`. Cannot use `REJECT`. |
| `NULL_PCT_CHECK` | Dataset | `column`, `max_pct` | `max_pct`: float `0`–`100`. `on_fail`: `WARN`, `FAIL`. Cannot use `REJECT`. |
| `ROW_COUNT_CHECK` | Dataset | — (at least one of `min_count`/`max_count` should be set) | `min_count`, `max_count`: integer. `on_fail`: `WARN`, `FAIL`. Cannot use `REJECT`. |
| `SUM_CHECK` | Dataset | `column` | `min_sum`, `max_sum`: numeric. `on_fail`: `WARN`, `FAIL`. Cannot use `REJECT`. |
| `DISTINCT_COUNT_CHECK` | Dataset | `column` | `min_count`, `max_count`: integer. `on_fail`: `WARN`, `FAIL`. Cannot use `REJECT`. |

Rule ordering convention: `REJECT` rules first, `WARN` rules next, `FAIL` rules last.
`rule_type` defaults to `EXPRESSION` when omitted. `on_fail` defaults to `REJECT` when omitted.
`rule_name` falls back to `rule_type` when omitted — always set it for clear warning messages.

The current runtime always returns `valid` and `rejected` output ports. Do not put
output-port routing in this config; use downstream `DATA_FLOW_STEP.input_map`.

## CROSS_VALIDATION

Validates the primary input against one or more reference DataFrames supplied via
`input_map` (see `refs: dict[str, DataFrame]`). `REFERENCE_CHECK` is a row-level
left-join check against a reference input. 


```json
{
  "main_alias": "orders",
  "rules": [
    {
      "rule_type":       "REFERENCE_CHECK",
      "rule_name":       "customer_exists",
      "reference_alias": "customer_master",
      "column":          "customer_id",
      "ref_column":      "customer_id",
      "on_fail":         "REJECT"
    }
  ]
}

{
  "main_alias": "orders",
  "rules": [
    {
      "rule_type":       "REFERENCE_CHECK",
      "rule_name":       "valid_bank_ifsc_combination",
      "reference_alias": "rbi_ifsc_master",
      "columns":         ["bank_code", "ifsc_code"],
      "ref_columns":     ["bank_code", "ifsc_code"],
      "on_fail":         "REJECT"
    }
  ]
}
```

| Key | Required | Values | Default / behaviour |
|---|---:|---|---|
| `main_alias` | No | Input alias string | `"default"`. The `input_map` alias of the primary DataFrame being validated. |
| `rules` | Yes | Non-empty array of rule objects | Required. |
| `rules[].rule_type` | Yes | `REFERENCE_CHECK` | Required — no safe default. |
| `rules[].rule_name` | Recommended | Rule name string | Falls back to `rule_type`. Used in `_rejection_reason` and warning messages. |
| `rules[].reference_alias` | Yes | input_map alias string | None. Must match a key in `input_map` pointing to the reference DataFrame. |
| `rules[].column` | Single only | Column name | None. Source column for single-column check. |
| `rules[].ref_column` | Single only | Column name | None. Reference column for single-column check. |
| `rules[].columns` | Composite only | Array of column names | None. Source columns for composite key check. |
| `rules[].ref_columns` | Composite only | Array of column names | None. Reference columns for composite key check. Must have same length as `columns`. |
| `rules[].on_fail` | No | `REJECT`, `WARN` | `REJECT`. `REJECT` routes non-matching rows to the `rejected` port. `WARN` emits a warning but keeps all rows in `valid`. |


`rule_results` audit entries are recorded in `StepResult.metrics` for
`REFERENCE_CHECK` WARN rules only. `REFERENCE_CHECK`
REJECT rules are excluded from `rule_results` — per-rule counts would require an
extra Spark action per rule.

## PROTECTION

Applies data protection operations and optionally drops original columns.

```json
{
  "operations": [
    {
      "column": "customer_id",
      "action": "HASH",
      "output_column": "customer_hash",
      "secret_ref": {
        "vault_url": "https://kv-etl.vault.azure.net/",
        "secret_name": "customer-hash-key",
        "version": null,
        "local_key": "customer-hash-key"
      },
      "require_secret": true
    },
    {
      "column": "email",
      "action": "MASK",
      "output_column": "email_masked",
      "keep_last": 4
    },
    {
      "column": "ticket",
      "action": "NULLIFY",
      "output_column": "ticket_null"
    },
    {
      "column": "region",
      "action": "REDACT",
      "output_column": "region_redacted"
    }
  ],
  "local_secrets": {
    "customer-hash-key": "local-development-only"
  },
  "drop_original_columns": ["email"]
}
```

#### Schema Contract

| Key | Type | Required | Description |
|---|---|---|---|
| `operations` | array[object] | Yes | Non-empty array of protection operation objects. |
| `local_secrets` | object (dict) | No | A dictionary of `{secret_key: value}` for local development bypass. |
| `drop_original_columns` | array[string] | No | Array of column names to drop after all operations are applied. |

#### Protection Operation

| Key | Type | Required | Description |
|---|---|---|---|
| `column` | string | Yes | The source column to protect. |
| `action` | string | Yes | `HASH`, `MASK`, `NULLIFY`, `REDACT`. |
| `output_column` | string | No | The output column name (defaults to `column`, which overwrites the original). |
| `secret_ref` | object | Conditional | Secret reference object. Used by `HASH`. Aliases: `key_ref`, `salt_ref`, `pepper_ref`. |
| `require_secret` | bool | No | When `true`, unresolved HASH secret fails the step. (Defaults to `false`). |
| `keep_last` | int | No | Integer `>= 0`. Used by `MASK`. (Defaults to `4`). |

#### Secret Reference

| Key | Type | Required | Description |
|---|---|---|---|
| `vault_url` | string | No | Full Azure Key Vault URL. |
| `key_vault_name` | string | No | Builds `https://<key_vault_name>.vault.azure.net/` when `vault_url` is absent. |
| `secret_name` | string | Yes | Secret name to resolve. Alias: `name`. |
| `version` | string | No | Optional Azure Key Vault secret version. |
| `local_key` | string | No | Local fallback key (checks `local_secrets` map or environment variables). Alias: `env_var`. |

## EXPERT_SQL

Runs one guarded Spark SQL `SELECT` or `WITH ... SELECT` statement over registered
input DataFrames.

```json
{
  "sql": "WITH complete_orders AS (SELECT customer_id, amount FROM orders WHERE status = 'COMPLETE') SELECT customer_id, SUM(amount) AS total_amount FROM complete_orders GROUP BY customer_id",
  "registered_inputs": [
    {
      "alias": "orders",
      "from_step_id": 101,
      "output_port": "default"
    }
  ]
}
```

#### Schema Contract

| Key | Type | Required | Description |
|---|---|---|---|
| `sql` | string | Yes | Single `SELECT` or `WITH ... SELECT` statement. Trailing semicolon is stripped; embedded semicolons are rejected. |
| `registered_inputs` | array[object] | Yes | Non-empty array of input registration objects. |

#### Input Registration

| Key | Type | Required | Description |
|---|---|---|---|
| `alias` | string | Yes | Valid Spark temp view name: `[A-Za-z_][A-Za-z0-9_]*`. Must match an input alias resolved from `DATA_FLOW_STEP.input_map`. |
| `from_step_id` | int | No | The upstream step ID. |
| `output_port` | string | Recommended | The upstream output port (e.g., `default`, `valid`, `rejected`). |

Forbidden SQL keywords include DML, DDL, mutation, catalog, cache, load/copy,
procedure, and analyze commands. Multiple statements are not allowed.

## TARGET

Writes the incoming DataFrame to one registered target object. The TARGET block is
exclusively responsible for determining write behavior. All write-related configuration
(`write_mode`, `merge_keys`, `partition_columns`) is owned by the TARGET block config
or its `OBJECT_REGISTRY.object_metadata` — never by the source.

```json
{
  "target_object_id": 201,
  "write_mode": "append",
  "partition_columns": ["region"],
  "write_options_override": {
    "batchsize": "10000"
  }
}
```

**Merge example (DATABRICKS Delta Lake only):**
```json
{
  "target_object_id": 202,
  "write_mode": "merge",
  "merge_keys": ["customer_id"],
  "write_options_override": {}
}
```

#### Schema Contract

| Key | Type | Required | Description |
|---|---|---|---|
| `target_object_id` | int | Yes | Existing `OBJECT_REGISTRY.object_id`. Used to resolve connection and metadata. |
| `write_mode` | string | No | `append`, `overwrite`, `merge`. Defaults to `OBJECT_REGISTRY.object_metadata.write_mode`, then `append`. |
| `merge_keys` | array[string] | Conditional | Required when `write_mode=merge`. If omitted, falls back to `OBJECT_REGISTRY.object_metadata.primary_keys`. If neither is set the runtime raises a hard error. |
| `partition_columns` | array[string] | No | Physical partitions on the target (FILE/DATABRICKS only). Falls back to `OBJECT_REGISTRY.object_metadata.partition_columns`. |
| `write_options_override` | object | No | Connector/Spark write option object merged into the write call. |

#### Write Mode Compatibility Matrix

| `write_mode` | JDBC (MySQL, PostgreSQL, SQL Server) | DATABRICKS (Delta Lake) | File (S3, ADLS, GCS) |
|---|:---:|:---:|:---:|
| `append` | ✅ | ✅ | ✅ |
| `overwrite` | ✅ | ✅ | ✅ |
| `merge` | ❌ Hard error | ✅ (requires `merge_keys`) | ❌ Hard error |

> **CRITICAL:** `merge` is a Delta Lake–exclusive operation. The runtime raises a `BLOCK_CONFIG_INVALID` error immediately if `write_mode=merge` is attempted against a JDBC target. Do not surface `merge` as an option in the UI for JDBC connections.

#### merge_keys Resolution Order

The backend resolves `merge_keys` in this priority order:
1. `merge_keys` key in the `DATA_FLOW_STEP.config` JSON (highest priority).
2. `primary_keys` field inside `OBJECT_REGISTRY.object_metadata` for the target object.
3. If neither is found → runtime raises a hard `BLOCK_CONFIG_INVALID` error before Spark starts.

#### Schema Compatibility

Before writing, the runtime checks incoming DataFrame columns against
`OBJECT_REGISTRY.object_metadata.schema_contract`. If the target schema contract is
empty (e.g. `physically_exists: false` — a future/new table), the check is **skipped**
and the table will be created on first write. If the contract has fields and the
incoming DataFrame is missing required columns, a `TARGET_SCHEMA_MISMATCH` error is raised.

#### Preview Mode Behavior

In preview mode, the TARGET block **never writes any data**. It returns the schema of
the incoming DataFrame that would have been written. This is safe for UI canvas preview
and does not require a live target connection.

#### write_options_override Common Keys

| Key | Target Type | Description |
|---|---|---|
| `batchsize` | JDBC | Controls INSERT batch size per Spark task. Default varies by driver. |
| `truncate` | JDBC | `"true"` truncates before write on `overwrite` instead of DROP+CREATE. |
| `maxRecordsPerFile` | File (S3/ADLS/GCS) | Caps individual output file size. |

---

## RECONCILIATION

Compares two datasets row-by-row based on primary keys and routes data to four
output ports. This block is used for Change Data Capture (CDC), data quality
validation, and audit pipelines.

> **Note:** The RECONCILIATION block does NOT perform aggregate or statistical checks
> (SUM, AVG, NULL %, etc.). Those belong in the VALIDATION and CROSS_VALIDATION blocks.

### Inputs and Outputs

**Inputs (exactly 2 required):**
- Name the inputs `"source"` and `"target"` in `DATA_FLOW_STEP.input_map`. If not
  explicitly named, the engine uses the first input as source and second as target.

**Outputs (exactly 4 ports):**

| Output Port | Description | Typical Downstream Action |
|---|---|---|
| `match` | Rows where PK exists in both and all compare columns are identical. | Log / discard |
| `mismatch` | Rows where PK exists in both but at least one compare column differs. | UPDATE target |
| `missing_in_target` | Rows in source that have no matching PK in target. | INSERT into target |
| `missing_in_source` | Rows in target that have no matching PK in source. | DELETE from target |

### Configuration JSON

```json
{
  "primary_keys": ["employee_id"],
  "compare_columns": ["salary", "department_name", "status"]
}
```

**Minimal config (compare ALL shared columns automatically):**
```json
{
  "primary_keys": ["order_id"]
}
```

#### Schema Contract

| Key | Type | Required | Description |
|---|---|---|---|
| `primary_keys` | array[string] | Yes | Non-empty list of column names used as the JOIN key between the two datasets. Must exist in both input DataFrames. |
| `compare_columns` | array[string] | No | Specific columns to compare for differences. If omitted, the engine automatically compares ALL columns that exist in both inputs (excluding the primary keys). |

#### The `mismatch` Output — `_diff_details` Column

The `mismatch` output port includes a special auto-generated column `_diff_details`
containing a JSON string that maps each differing column to its source and target values.

**Example `_diff_details` value:**
```json
{
  "salary": {
    "source": "80000",
    "target": "75000"
  },
  "department_name": {
    "source": "Engineering",
    "target": "R&D"
  }
}
```

The mismatch output column layout:
- All `primary_keys` columns.
- For each `compare_columns`: `source_<col>` and `target_<col>` side-by-side.
- `_diff_details` (JSON string of all columns that differ).

#### Result Persistence

The RECONCILIATION block automatically persists all row-level results to the
`RECON_RESULT` control table in MySQL with the following columns:
`run_id`, `step_id`, `step_key`, `recon_status` (`MATCHED`, `MISMATCHED`,
`MISSING_IN_TARGET`, `MISSING_IN_SOURCE`), `pk_values` (JSON), `diff_type`, `diff_details` (JSON).

This table is the audit trail for every reconciliation run and can be queried
by the frontend for reporting and dashboards.

#### Example `input_map` for RECONCILIATION

The `DATA_FLOW_STEP.input_map` for a RECONCILIATION step must wire two
upstream steps to the named ports `source` and `target`:

```json
{
  "source": "read_source_employees",
  "target": "read_target_employees"
}
```

#### UI Canvas Guidance

The UI canvas should render the RECONCILIATION block with:
- **2 input arrows** (left side): labeled `source` and `target`.
- **4 output arrows** (right side): labeled `match`, `mismatch`, `missing_in_target`, `missing_in_source`.
- A typical CDC pipeline connects `missing_in_target` → TARGET (append) and `mismatch` → TARGET (merge/overwrite).
