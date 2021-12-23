"""KQL Dialect ODBC driver module."""
import datetime
import decimal
import re
import struct

from sqlalchemy import exc
from sqlalchemy import types as sqltypes
from sqlalchemy import util
from sqlalchemy.connectors.pyodbc import PyODBCConnector
from sqlalchemy.dialects.mssql.base import BINARY, DATETIMEOFFSET, VARBINARY
from sqlalchemy.dialects.mssql.pyodbc import (
    _ODBCDATETIMEOFFSET,
    MSExecutionContext_pyodbc,
    _BINARY_pyodbc,
    _MSFloat_pyodbc,
    _MSNumeric_pyodbc,
    _ODBCDateTime,
    _VARBINARY_pyodbc,
)

from kqlalchemy.base import KQLDialect


class KQLDialect_pyodbc(PyODBCConnector, KQLDialect):
    supports_statement_cache = True

    # mssql still has problems with this on Linux
    supports_sane_rowcount_returning = False

    execution_ctx_cls = MSExecutionContext_pyodbc

    colspecs = util.update_copy(
        KQLDialect.colspecs,
        {
            sqltypes.Numeric: _MSNumeric_pyodbc,
            sqltypes.Float: _MSFloat_pyodbc,
            BINARY: _BINARY_pyodbc,
            # support DateTime(timezone=True)
            sqltypes.DateTime: _ODBCDateTime,
            DATETIMEOFFSET: _ODBCDATETIMEOFFSET,
            # SQL Server dialect has a VARBINARY that is just to support
            # "deprecate_large_types" w/ VARBINARY(max), but also we must
            # handle the usual SQL standard VARBINARY
            VARBINARY: _VARBINARY_pyodbc,
            sqltypes.VARBINARY: _VARBINARY_pyodbc,
            sqltypes.LargeBinary: _VARBINARY_pyodbc,
        },
    )

    def __init__(self, description_encoding=None, fast_executemany=False, **params):
        if "description_encoding" in params:
            self.description_encoding = params.pop("description_encoding")
        super(KQLDialect_pyodbc, self).__init__(**params)
        self.use_scope_identity = (
            self.use_scope_identity and self.dbapi and hasattr(self.dbapi.Cursor, "nextset")
        )
        self._need_decimal_fix = self.dbapi and self._dbapi_version() < (
            2,
            1,
            8,
        )
        self.fast_executemany = fast_executemany

    def _get_server_version_info(self, connection):
        try:
            # "Version of the instance of SQL Server, in the form
            # of 'major.minor.build.revision'"
            raw = connection.exec_driver_sql(
                "SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR)"
            ).scalar()
        except exc.DBAPIError:
            # SQL Server docs indicate this function isn't present prior to
            # 2008.  Before we had the VARCHAR cast above, pyodbc would also
            # fail on this query.
            return super(KQLDialect_pyodbc, self)._get_server_version_info(
                connection, allow_chars=False
            )
        else:
            version = []
            r = re.compile(r"[.\-]")
            for n in r.split(raw):
                try:
                    version.append(int(n))
                except ValueError:
                    pass
            return tuple(version)

    def on_connect(self):
        super_ = super(KQLDialect_pyodbc, self).on_connect()

        def on_connect(conn):
            if super_ is not None:
                super_(conn)

            self._setup_timestampoffset_type(conn)

        return on_connect

    def _setup_timestampoffset_type(self, connection):
        # output converter function for datetimeoffset
        def _handle_datetimeoffset(dto_value):
            tup = struct.unpack("<6hI2h", dto_value)
            return datetime.datetime(
                tup[0],
                tup[1],
                tup[2],
                tup[3],
                tup[4],
                tup[5],
                tup[6] // 1000,
                datetime.timezone(datetime.timedelta(hours=tup[7], minutes=tup[8])),
            )

        odbc_SQL_SS_TIMESTAMPOFFSET = -155  # as defined in SQLNCLI.h
        connection.add_output_converter(odbc_SQL_SS_TIMESTAMPOFFSET, _handle_datetimeoffset)

    def do_executemany(self, cursor, statement, parameters, context=None):
        if self.fast_executemany:
            cursor.fast_executemany = True
        super(KQLDialect_pyodbc, self).do_executemany(
            cursor, statement, parameters, context=context
        )

    def is_disconnect(self, e, connection, cursor):
        if isinstance(e, self.dbapi.Error):
            code = e.args[0]
            if code in {
                "08S01",
                "01000",
                "01002",
                "08003",
                "08007",
                "08S02",
                "08001",
                "HYT00",
                "HY010",
                "10054",
            }:
                return True
        return super(KQLDialect_pyodbc, self).is_disconnect(e, connection, cursor)


dialect = KQLDialect_pyodbc
