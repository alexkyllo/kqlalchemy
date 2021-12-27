"""KQLAlchemy
A SQLAlchemy dialect for Azure Data Explorer (Kusto).
"""
__version__ = "0.1.0"
from sqlalchemy.dialects import registry

from .kql_dialect import KQLDialect, kusto_engine, kusto_table

registry.register("mskql.pyodbc", "kqlalchemy.pyodbc", "KQLDialect_pyodbc")
