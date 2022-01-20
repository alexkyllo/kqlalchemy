# KQLAlchemy

SQLAlchemy dialect for Azure Data Explorer (Kusto).

Kusto supports SQL over ODBC and emulates SQL Server. You can use the MSSQL
dialect of SQLAlchemy directly to work with a Kusto cluster, but SQLAlchemy's
reflection methods run several queries on `dbo.INFORMATION_SCHEMA` that are slow
and unnecessary on Kusto. This package provides a `KQLDialect`, a thin wrapper
over the MSSQL dialect, to improve performance when working with Kusto.

Also included are some helper functions for connecting to Kusto with Azure
Active Directory credentials.

## Prerequisites

The KQL Dialect uses ODBC and requires the `pyodbc` package so you must first
install SQL Server ODBC drivers. `pymssql` (which uses FreeTDS) is not
supported.

On Ubuntu/Debian:

```
sudo apt install unixodbc-dev
```

On Mac with Homebrew:

```
brew install unixodbc
```

Then, follow the instructions from [Microsoft Docs](https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-1-configure-development-environment-for-pyodbc-python-development?view=sql-server-ver17) to install
ODBC Driver 17 for SQL Server.

For more details see the [PyODBC wiki](https://github.com/mkleehammer/pyodbc/wiki/Install)

## Installation

```
pip install kqlalchemy
```

## Usage

To get a SQLAlchemy `Engine` instance, use the scheme `"mskql+pyodbc://"`:

```python
from azure.identity import AzureCliCredential
from sqlalchemy import create_engine
from sqlalchemy.dialects import registry
from sqlalchemy.engine import URL


# Dynamically register the KQLDialect plugin
registry.register("mskql.pyodbc", "kqlalchemy.pyodbc", "KQLDialect_pyodbc")

server = "help"
database = "Samples"
conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={server}.kusto.windows.net;Database={database}"
connection_url = URL.create("mskql+pyodbc", query={"odbc_connect": conn_str, "autocommit": "True"})
engine = create_engine(connection_url, azure_credential=AzureCliCredential())
```

Or, use the helper function `kusto_engine` to get an `Engine` instance:

```python
from azure.identity import AzureCliCredential
# Importing kqlalchemy loads the dialect plugin.
from kqlalchemy import kusto_engine, kusto_table
import pandas as pd
from sqlalchemy.orm import Session

engine = kusto_engine("help", "Samples", AzureCliCredential())
storm_events = kusto_table("StormEvents", engine) # performs reflection
query = storm_events.select().limit(10)

with Session(engine, autocommit=True) as session:
    df = pd.read_sql(
        query.compile(engine, compile_kwargs={"literal_binds": True}),
        session.bind,
    )
    print(df[["StartTime", "EventType", "State", "DamageProperty"]].head(5))
```
```
           StartTime          EventType           State  DamageProperty
0 2007-09-29 08:11:00         Waterspout  ATLANTIC SOUTH               0
1 2007-09-18 20:00:00         Heavy Rain         FLORIDA               0
2 2007-09-20 21:57:00            Tornado         FLORIDA         6200000
3 2007-12-30 16:00:00  Thunderstorm Wind         GEORGIA            2000
4 2007-12-20 07:50:00  Thunderstorm Wind     MISSISSIPPI           20000
```
