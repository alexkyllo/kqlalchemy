"""KQL Dialect base module."""
import struct
from urllib.parse import unquote

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from sqlalchemy import create_engine, event, util
from sqlalchemy.dialects.mssql.base import (
    MSBinary,
    MSChar,
    MSDialect,
    MSNChar,
    MSNText,
    MSNVarchar,
    MSString,
    MSText,
    MSVarBinary,
    _db_plus_owner,
    _db_plus_owner_listing,
)
from sqlalchemy.engine import URL, Engine
from sqlalchemy.engine.reflection import cache
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql import sqltypes


def _get_token(azure_credentials):
    """Use an Azure credential to get a token from the Kusto service."""
    TOKEN_URL = "https://kusto.kusto.windows.net/"
    token = azure_credentials.get_token(TOKEN_URL).token
    return token


def _encode_token(token):
    raw_token = token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(raw_token)}s", len(raw_token), raw_token)
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}


def kusto_engine(server: str, database: str, azure_credential, *args, **kwargs):
    """Get a SQLAlchemy Engine for Kusto over ODBC."""
    conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={server}.kusto.windows.net;Database={database}"
    connection_url = URL.create(
        "mskql+pyodbc", query={"odbc_connect": conn_str, "autocommit": "True"}
    )
    engine = create_engine(connection_url, azure_credential=azure_credential, *args, **kwargs)

    return engine


def kusto_table(table_name, engine):
    """Get a SQLAlchemy Table instance corresponding to an existing Kusto table.
    Performs reflection to get the table schema as SQLAlchemy MetaData.

    Parameters
    ----------
    table_name: str
        The name of the table.
    engine: str
        A SQLAlchemy Engine instance.

    Returns
    -------
    sqlalchemy.Table
        A sqlalchemy Table instance with metadata reflected from the database.
    """
    metadata = MetaData()
    metadata.reflect(only=[table_name], bind=engine)
    tbl = Table(table_name, metadata)
    return tbl


def _parse_connection_str(conn_str):
    """Parse the server and database names out of the connection string."""
    url = unquote(str(conn_str))
    url_split_db = url.split(";Database=")
    database = url_split_db[1]
    server = url_split_db[0].split(";Server=")[1]
    server = f"https://{server}"
    return server, database


class KQLDialect(MSDialect):
    """"""

    parent = MSDialect
    name = "mskql"
    supports_statement_cache = True
    supports_default_values = True
    supports_empty_insert = False
    execution_ctx_cls = parent.execution_ctx_cls
    use_scope_identity = True
    max_identifier_length = 128
    schema_name = "dbo"
    implicit_returning = True
    full_returning = True

    colspecs = parent.colspecs

    engine_config_types = parent.engine_config_types

    ischema_names = parent.ischema_names

    supports_sequences = True
    sequences_optional = True
    default_sequence_base = 1
    supports_native_boolean = False
    non_native_boolean_check_constraint = False
    supports_unicode_binds = True
    postfetch_lastrowid = True
    _supports_offset_fetch = False
    _supports_nvarchar_max = False
    legacy_schema_aliasing = False
    server_version_info = ()
    statement_compiler = parent.statement_compiler
    ddl_compiler = parent.ddl_compiler
    type_compiler = parent.type_compiler
    preparer = parent.preparer

    construct_arguments = parent.construct_arguments

    def __init__(self, azure_credential, *args, **kwargs) -> None:
        self._kusto_client = None
        self._server = None
        self._database = None

        @event.listens_for(Engine, "do_connect")
        def provide_token(dialect, conn_rec, cargs, cparams):
            """Get a token from the Azure credential to build the connection parameters."""
            # remove the "Trusted_Connection" parameter that SQLAlchemy adds
            cargs[0] = cargs[0].replace(";Trusted_Connection=Yes", "")
            token = _get_token(azure_credential)
            self._server, self._database = _parse_connection_str(cargs[0])
            url = self._server
            kcsb = KustoConnectionStringBuilder.with_token_provider(url, lambda: token)
            self._kusto_client = KustoClient(kcsb)
            cparams["attrs_before"] = _encode_token(token)

        super().__init__(*args, **kwargs)

    def get_isolation_level(self, dbapi_connection):
        return "READ COMMITTED"

    @cache
    @_db_plus_owner
    def get_pk_constraint(self, connection, tablename, dbname, owner, schema, **kw):
        """Kusto does not have primary key constraints. Returns empty set."""
        return {"constrained_columns": [], "name": None}

    @cache
    @_db_plus_owner
    def get_foreign_keys(self, connection, tablename, dbname, owner, schema, **kw):
        """Kusto does not have foreign keys. Returns empty set."""
        return set()

    @cache
    @_db_plus_owner
    def get_indexes(self, connection, tablename, dbname, owner, schema, **kw):
        """Kusto indexes all columns and does not expose indexes to the user. Returns empty set."""
        return set()

    @cache
    @_db_plus_owner
    def get_columns(self, connection, tablename, dbname, owner, schema, **kw):
        """Get a list of all columns and their datatypes in the given table."""
        # Map all Kusto types to SQL Server types
        type_map = {
            "I8": "bit",
            "DateTime": "datetime2",
            "Dynamic": "nvarchar",
            "UniqueId": "uniqueidentifier",
            "I32": "int",
            "I64": "bigint",
            "R64": "float",
            "StringBuffer": "nvarchar",
            "TimeSpan": "bigint",
            "Decimal": "decimal",
        }
        type_precision = {
            "int": {"precision": 10, "scale": 0},
            "bigint": {"precision": 19, "scale": 0},
            "float": {"precision": 53, "scale": None},
            "decimal": {"precision": 38, "scale": 17},
        }
        res = self._kusto_client.execute_mgmt(
            self._database, f".show table {tablename}"
        ).primary_results[0]
        cols = []
        for row in res.rows:
            name = row["AttributeName"]
            type_ = type_map.get(row["AttributeType"])
            coltype = self.ischema_names.get(type_, None)
            collation = "SQL_Latin1_General_CP1_CS_AS" if type_ == "nvarchar" else None
            kwargs = {}
            if coltype in (
                MSString,
                MSChar,
                MSNVarchar,
                MSNChar,
                MSText,
                MSNText,
                MSBinary,
                MSVarBinary,
                sqltypes.LargeBinary,
            ):
                kwargs["length"] = None
                if collation:
                    kwargs["collation"] = collation

            if coltype is None:
                util.warn("Did not recognize type '%s' of column '%s'" % (type_, name))
                coltype = sqltypes.NULLTYPE
            else:
                if issubclass(coltype, sqltypes.Numeric):
                    kwargs["precision"] = type_precision.get(type_)
                    if not issubclass(coltype, sqltypes.Float):
                        kwargs["scale"] = type_precision.get(type_)

                coltype = coltype(**kwargs)
            cdict = {
                "name": name,
                "type": coltype,
                "nullable": True,
                "default": None,
                "autoincrement": False,
            }

            cols.append(cdict)

        return cols

    @cache
    @_db_plus_owner_listing
    def get_table_names(self, connection, dbname, owner, schema, **kwargs):
        """Get a list of all the table names in the database."""
        res = self._kusto_client.execute_mgmt(
            self._database, f".show tables | project TableName"
        ).primary_results[0]
        return [n[0] for n in res.rows]

    @_db_plus_owner
    def has_table(self, connection, tablename, dbname, owner, schema):
        """Check if the given table exists in the database."""
        res = self._kusto_client.execute_mgmt(
            self._database, f".show tables | where TableName == '{tablename}'"
        )
        tbls = res.primary_results[0]
        return len(tbls) > 0
