from sqlalchemy import sql, util
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
from sqlalchemy.engine.reflection import cache
from sqlalchemy.sql import sqltypes


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
