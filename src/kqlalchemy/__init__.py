"""KQLAlchemy
A SQLAlchemy dialect for Azure Data Explorer (Kusto).
"""
from .kql_dialect import *
from sqlalchemy.dialects import registry
registry.register("mskql.pyodbc", "kqlalchemy.pyodbc", "KQLDialect_pyodbc")