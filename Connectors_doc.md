# Table Connector

> Enterprise-grade JDBC Connector for PySpark with Azure Key Vault
> integration.

## Table of Contents

-   Overview
-   Repository Layout
-   Connector Architecture
-   Prerequisites
-   Installation
-   Python Dependencies
-   External Dependencies
-   Azure Requirements
-   Environment Configuration
-   Configuration
-   Authentication
-   SSL Configuration
-   Package Components
-   Connector Workflow
-   Example Usage
-   Logging
-   Error Handling
-   Verification
-   Best Practices
-   Future Enhancements

------------------------------------------------------------------------

# Overview

The **Table Connector** is responsible for building secure JDBC
connection properties that can be consumed directly by Spark JDBC
readers and writers.

It provides a modular architecture supporting multiple relational
databases while securely retrieving credentials from Azure Key Vault.

Supported databases include:

-   MySQL
-   PostgreSQL
-   SQL Server
-   Oracle
-   Snowflake

------------------------------------------------------------------------

# Repository Layout

table/
│
├── secret_manager.py
│     Azure Key Vault integration
│
├── auth_manager.py
│     Authentication handling
│
├── Connector_manager.py
│     Main connector entry point
│
├── jdbc_connection_manager.py
│     Builds JDBC URL and connection properties
│
├── ssl_manager.py
│     SSL configuration
│
└── __init__.py
      Package exports

------------------------------------------------------------------------

# Connector Architecture

               UI / Control Table
                        │
                        ▼
                Connection Config
                        │
                        ▼
                 JDBCConnector
                        │
                        ▼
            JDBCConnectionManager
          ┌─────────────┴─────────────┐
          ▼                           ▼
     AuthManager                 SSLManager
          │                           │
          ▼                           │
    SecretManager                     │
          │                           │
          ▼                           ▼
 Azure Key Vault              SSL Properties
          └─────────────┬─────────────┘
                        ▼
         JDBC Connection Properties
                        │
                        ▼
        Spark JDBC Reader / Writer
                        │
                        ▼
           Source / Target Database

------------------------------------------------------------------------

# Prerequisites

  Software       Recommended Version     Purpose
  -------------- ----------------------- ------------------
  Python         3.10+                   Runtime
  Apache Spark   3.5.x                   Data processing
  PySpark        Compatible with Spark   JDBC Integration
  Java JDK       11+                     Spark & JDBC
  pip            Latest                  Package Manager

------------------------------------------------------------------------

# Installation

pip install pyspark
pip install azure-identity
pip install azure-keyvault-secrets


Or

pip install pyspark azure-identity azure-keyvault-secrets

------------------------------------------------------------------------

# Python Dependencies

  Package                  Purpose
  ------------------------ ----------------------------------
  pyspark                  Spark DataFrame and JDBC support
  azure-identity           Azure authentication
  azure-keyvault-secrets   Azure Key Vault secret retrieval

pyspark>=3.5.0
azure-identity>=1.25.0
azure-keyvault-secrets>=4.8.0
------------------------------------------------------------------------

# External Dependencies

Database-specific JDBC driver JARs are required.

  Database     Driver Class
  ------------ ----------------------------------------------
  MySQL        com.mysql.cj.jdbc.Driver
  PostgreSQL   org.postgresql.Driver
  SQL Server   com.microsoft.sqlserver.jdbc.SQLServerDriver
  Oracle       oracle.jdbc.OracleDriver
  Snowflake    net.snowflake.client.jdbc.SnowflakeDriver

Place the required JDBC driver JAR files in the Spark classpath before
executing the connector.

------------------------------------------------------------------------

# Azure Requirements

-   Azure Subscription
-   Azure Key Vault
-   Azure Identity Authentication
-   Database credentials stored as Key Vault secrets

The connector retrieves secrets securely during runtime using the Azure
SDK.

------------------------------------------------------------------------

# Environment Configuration
Key Vault URI will be passed from the control tables. If no Key Vault URI is provided in the configuration, the default URI is
used from OS Env

DEFAULT_KEYVAULT_URI=https://<your-keyvault>.vault.azure.net/


------------------------------------------------------------------------


## TABLES
## CONNECTION_REGISTRY:

Column	                  Description
connection_id	      Unique identifier for the connection configuration.
connection_name	      User-friendly name of the connection.
connection_category	Category of the connection (for example: TABLE, FILE, API, DATABRICKS).
system_name	            Name of the source or target system (for example: MySQL, Oracle, PostgreSQL, SQL Server, Snowflake).
connection_type	      Type of connection to establish (for example: JDBC).
auth_type	            Authentication mechanism used by the connector (BASIC, TOKEN, OAuth, etc.).
connection_config	       JSON configuration containing all connection-specific properties such as host, port, database   name, driver, SSL settings, Key Vault information, and authentication parameters.

Detail connection_config Typical configuration:

  Parameter        Description
  ---------------- ---------------------
  system_name      Database type
  host             Database host
  port             Database port
  database_name    Database name
  auth_type        Authentication type
  driver           JDBC driver class
  KEY_VAULT_NAME   Azure Key Vault
  service_name     Oracle service name

Authentication-specific parameters are required depending on the
authentication mechanism.

------------------------------------------------------------------------

# Authentication

## BASIC

Required:

-   username_key
-   password_key

## TOKEN

Required:

-   token_key

## OAuth

Required:

-   client_id_key
-   client_secret_key

All credentials are securely retrieved from Azure Key Vault.

# Azure Key Vault Authentication Setup (Local Windows)

## Objective

Configure a local Windows environment to authenticate with Azure Key Vault using Azure CLI and Python, then retrieve secrets securely.

# Prerequisites

* Windows (64-bit)
* Python 3.x
* Azure subscription
* Azure Key Vault
* Permission to access the Key Vault

---

# Step 1: Install Azure SDK Packages

Install the required Python libraries.

```bash
pip install azure-identity azure-keyvault-secrets
```

Verify installation:

```bash
pip show azure-identity
pip show azure-keyvault-secrets
```

---

# Step 2: Install Azure CLI

Download and install the **Azure CLI (64-bit MSI)** for Windows.

After installation, verify:

Open powershell 
az --version
```

Expected output:

```
azure-cli 2.xx.x
```

---

**## Step 3: Login to Azure **

Authenticate using Azure CLI.

```powershell
az login
```

If prompted to select a subscription, press **Enter** to use the default subscription.

Verify the logged-in account:

```powershell
az account show
```

Example output:

```json
{
  "name": "umc-etl-1",
  "tenantDisplayName": "Unique IT Solutions inc",
  "user": {
      "name": "venkat.annapureddy@unikitsolutions.com"
  }
}
```

---

# Step 4: Create Python Code

```python
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

KEY_VAULT_NAME = "<key-vault-name>"
SECRET_NAME = "<secret-name>"

vault_url = f"https://{KEY_VAULT_NAME}.vault.azure.net"

credential = DefaultAzureCredential()

client = SecretClient(
    vault_url=vault_url,
    credential=credential
)

secret = client.get_secret(SECRET_NAME)

print(secret.value)
```

---

# Step 5: Initial Authentication Error

Initially the following error occurred:

```
DefaultAzureCredential failed to retrieve a token.
AzureCliCredential: Azure CLI not found on path.
```

### Root Cause

Azure CLI was not installed on the local machine.

### Resolution

Installed Azure CLI and authenticated using:

```powershell
az login
```

---

# Step 6: Verify Azure CLI Installation

Verify Azure CLI:

```powershell
az --version
```

Locate the executable:

```powershell
where.exe az
```

Example:

```
C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az
C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd
```

---

# Step 7: Authentication Successful

After Azure CLI installation and login:

* Azure CLI authentication succeeded.
* Azure Identity successfully obtained an access token.
* The application connected to Azure Key Vault.

---

# Step 8: Authorization Error

The application then returned:

```
HttpResponseError: (Forbidden)

The user does not have secrets get permission on key vault 'DEKeyvault121'
```

### Root Cause

Authentication was successful, but the Azure account did not have permission to read secrets from the Key Vault.

---

# Step 9: Grant Key Vault Permission

Depending on the Key Vault configuration:

## Option A: Azure RBAC

1. Azure Portal
2. Open Key Vault
3. Access Control (IAM)
4. Add Role Assignment
5. Select **Key Vault Secrets User**
6. Select the required Azure user
7. Assign the role

---

## Option B: Access Policies

1. Azure Portal
2. Open Key Vault
3. Access Policies
4. Add Access Policy
5. Secret Permissions:

   * Get
   * List
6. Select the Azure user
7. Save

---

# Step 10: Retry

After permissions are granted:

```python
secret = client.get_secret(SECRET_NAME)
print(secret.value)
```

The secret value should be returned successfully.

---

# Troubleshooting

## Verify Azure CLI

```powershell
az --version
```

## Verify Login

```powershell
az account show
```

## Verify Azure CLI Location

```powershell
where.exe az
```

## Verify Installed Python Packages

```bash
pip show azure-identity
pip show azure-keyvault-secrets
```

------------------------------------------------------------------------

# SSL Configuration

Supported properties:

  Property        Description
  --------------- --------------------
  ssl_enabled     Enable SSL
  ssl_cert_path   Client certificate
  ssl_key_path    Client private key

The SSL Manager automatically constructs JDBC SSL properties.

------------------------------------------------------------------------

# Package Components

## connector_manager.py

-   Connector entry point
-   Validates configuration
-   Builds reusable connection information

## jdbc_connection_manager.py

-   Builds JDBC URLs
-   Creates connection properties
-   Loads authentication
-   Loads SSL configuration

## auth_manager.py


-   Determines authentication type
-   Retrieves credentials
-   Builds authentication properties

## secret_manager.py

-   Connects to Azure Key Vault
-   Retrieves secrets securely

## ssl_manager.py

-   Reads SSL configuration
-   Validates SSL parameters
-   Creates SSL properties

------------------------------------------------------------------------

# Connector Workflow

Application
      │
      ▼
jdbc_connector.py
      │
      ▼
jdbc_connection_manager.py
      │
      ├────► auth_manager.py
      │            │
      │            ▼
      │     secret_manager.py
      │
      ▼
ssl_manager.py
      │
      ▼
JDBC Database


------------------------------------------------------------------------

# Example Usage

from connectors.table.jdbc_connector import JDBCConnector

connector = JDBCConnector(config)

connection_properties = connector.get_connection()
```

The returned connection properties can be passed directly to Spark JDBC
Reader or Writer function modules


------------------------------------------------------------------------

# Logging

Logging includes:

-   Connector initialization
-   Authentication
-   Secret retrieval
-   JDBC URL generation
-   SSL configuration
-   Error reporting

Sensitive information such as passwords and tokens is never logged.

Example:

``` text
INFO  Initializing JDBC Connector
INFO  Loading authentication properties
INFO  Retrieving secrets from Azure Key Vault
INFO  Building JDBC connection URL
INFO  Applying SSL configuration
INFO  JDBC connection properties created successfully
INFO  Connection established successfully
```

------------------------------------------------------------------------

# Error Handling

Exceptions are raised and logged for:

-   Invalid configuration
-   Authentication failures
-   SSL failures
-   Database connection failures

------------------------------------------------------------------------

# Verification Checklist

-   [ ] Required Python packages installed
-   [ ] Java configured
-   [ ] Spark configured
-   [ ] JDBC Driver JAR available
-   [ ] Azure Key Vault accessible
-   [ ] Secrets exist
-   [ ] Database reachable
-   [ ] SSL configuration verified

------------------------------------------------------------------------

# Best Practices

-   Store all credentials in Azure Key Vault.
-   Never hardcode usernames or passwords.
-   Enable SSL for production.
-   Keep JDBC drivers compatible with Spark.
-   Validate configuration before execution.
-   Enable logging.
-   Follow the principle of least privilege.

------------------------------------------------------------------------

# Future Enhancements

## 1. Support Additional Database Connections

Extend support for additional JDBC-compatible databases such as:

-   IBM DB2
-   SAP HANA
-   Teradata
-   Amazon Redshift
-   Vertica
-   SQLite
-   MariaDB
-   Informix

using the existing pluggable architecture.

## 2. Simplify JDBC URL Generation

Create a centralized JDBC URL Builder/Factory to eliminate
database-specific conditional logic, making URL generation easier to
maintain and extend.

Benefits:

-   Cleaner implementation
-   Easier onboarding of new databases
-   Reduced maintenance effort

## 3. Reduce Code Redundancy

Refactor common functionality into reusable utility modules.

Examples:

-   Shared validation utilities
-   Common property builders
-   Generic authentication helpers
-   Common exception handling
-   Reusable logging utilities

Benefits:

-   Fewer lines of code
-   Improved readability
-   Easier testing
-   Better maintainability

------------------------------------------------------------------------

# License

Internal project documentation.
