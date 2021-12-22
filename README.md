# KQLAlchemy

SQLAlchemy dialect for Azure Data Explorer (Kusto).

Kusto supports SQL over ODBC and emulates SQL Server. You can use the MSSQL
dialect of SQLAlchemy directly, but SQLAlchemy's reflection methods run several
queries on `dbo.INFORMATION_SCHEMA` that are slow and unnecessary on Kusto. This
package is a thin wrapper over the MSSQL dialect, to improve performance.

Also included are some helper functions for connecting to Kusto with Azure
Active Directory credentials.

## Prerequisites

This requires PyODBC so you need ODBC drivers.

On Ubuntu/Debian:

```
sudo apt install unixodbc-dev
```

On Mac with Homebrew:

```
brew install unixodbc
```

Then, follow the instructions from [Microsoft Docs](https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/python-sql-driver-pyodbc?view=sql-server-ver15) to install
ODBC Driver 17 for SQL Server.

For more details see the [PyODBC wiki](https://github.com/mkleehammer/pyodbc/wiki/Install)

## Installation

```
pip install kqlalchemy
```

## Usage

```python
from kqlalchemy import get_engine, kusto_table, to_pandas
from azure.identity import AzureCliCredential

engine = get_engine("help", "Samples", AzureCliCredential())
storm_events = kusto_table("StormEvents", engine) # performs reflection
query = storm_events.select().limit(10)
df = to_pandas(query, engine)
```

## TODO

- [ ] tests
- [ ] docs
- [ ] publish to PyPI