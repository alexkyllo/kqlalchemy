"""KQL Dialect base module."""
import struct
from urllib.parse import unquote

import pandas as pd
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from sqlalchemy import create_engine, event, sql, util
from sqlalchemy.dialects.mssql import information_schema as ischema
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
)
from sqlalchemy.engine import URL
from sqlalchemy.engine.reflection import cache
from sqlalchemy.orm import Session
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql import sqltypes


def get_token(azure_credentials):
    """Use a credential to get a"""
    TOKEN_URL = "https://kusto.kusto.windows.net/"
    raw_token = azure_credentials.get_token(TOKEN_URL).token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(raw_token)}s", len(raw_token), raw_token)
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}


def get_engine(server: str, database: str, azure_credentials, *args, **kwargs):
    """Get a SQLAlchemy Engine for Kusto over ODBC."""
    conn_str = f"Driver={{ODBC Driver 17 for SQL Server}};Server={server}.kusto.windows.net;Database={database}"
    connection_url = URL.create(
        "mskql+pyodbc", query={"odbc_connect": conn_str, "autocommit": "True"}
    )
    engine = create_engine(connection_url, *args, **kwargs)

    @event.listens_for(engine, "do_connect")
    def provide_token(dialect, conn_rec, cargs, cparams):
        # remove the "Trusted_Connection" parameter that SQLAlchemy adds
        cargs[0] = cargs[0].replace(";Trusted_Connection=Yes", "")
        # apply it to keyword arguments
        cparams["attrs_before"] = get_token(azure_credentials)

    return engine


def kusto_table(table_name, engine):
    """"""
    metadata = MetaData()
    metadata.reflect(only=[table_name], bind=engine)
    tbl = Table(table_name, metadata)
    return tbl


def to_pandas(query, engine):
    """"""
    with Session(engine, autocommit=True) as session:
        df = pd.read_sql(
            query.compile(engine, compile_kwargs={"literal_binds": True}),
            session.bind,
        )
        return df


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

    def get_isolation_level(self, dbapi_connection):
        return "READ COMMITTED"

    @cache
    @_db_plus_owner
    def get_pk_constraint(self, connection, tablename, dbname, owner, schema, **kw):
        return {"constrained_columns": [], "name": None}

    @cache
    @_db_plus_owner
    def get_foreign_keys(self, connection, tablename, dbname, owner, schema, **kw):
        return set()

    @cache
    @_db_plus_owner
    def get_indexes(self, connection, tablename, dbname, owner, schema, **kw):
        return set()

    @cache
    @_db_plus_owner
    def get_columns(self, connection, tablename, dbname, owner, schema, **kw):
        """"""
        print(dir(connection.engine))
        url = connection.engine.url.query["odbc_connect"]
        url = unquote(str(url))
        url_split_db = url.split(";Database=")
        database = url_split_db[1]
        server = url_split_db[0].split(";Server=")[1]
        print(database)
        print(server)

        columns = ischema.columns
        whereclause = columns.c.table_name == tablename
        s = (
            sql.select(
                columns.c.column_name,
                columns.c.data_type,
                columns.c.ordinal_position,
                columns.c.numeric_precision,
                columns.c.numeric_scale,
            )
            .where(whereclause)
            .order_by(columns.c.ordinal_position)
        )
        c = connection.execution_options(future_result=True).execute(s)
        cols = []
        for row in c.mappings():
            name = row[columns.c.column_name]
            type_ = row[columns.c.data_type]
            numericprec = row[columns.c.numeric_precision]
            numericscale = row[columns.c.numeric_scale]
            collation = "SQL_Latin1_General_CP1_CS_AS" if type_ == MSNVarchar else None

            coltype = self.ischema_names.get(type_, None)

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
                    kwargs["precision"] = numericprec

                    if not issubclass(coltype, sqltypes.Float):
                        kwargs["scale"] = numericscale

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

    @_db_plus_owner
    def has_table(self, connection, tablename, dbname, owner, schema):
        self._ensure_has_table_connection(connection)
        tables = ischema.tables
        s = sql.select(tables.c.table_name).where(
            sql.and_(
                sql.or_(
                    tables.c.table_type == "BASE TABLE",
                    tables.c.table_type == "VIEW",
                ),
                tables.c.table_name == tablename,
            )
        )

        if owner:
            s = s.where(tables.c.table_schema == owner)

        c = connection.execute(s)

        return c.first() is not None
