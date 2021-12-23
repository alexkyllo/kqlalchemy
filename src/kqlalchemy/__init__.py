"""KQLAlchemy
A SQLAlchemy dialect for Azure Data Explorer (Kusto).
"""
import struct

import pandas as pd
from sqlalchemy import create_engine, event
from sqlalchemy.dialects import registry
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session
from sqlalchemy.schema import MetaData, Table

registry.register("mskql.pyodbc", "kqlalchemy.pyodbc", "KQLDialect_pyodbc")

def get_token(azure_credentials):
    """Use a credential to get a """
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
