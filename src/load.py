"""
    File name: load.py
    Author: Daniel Liden
    Date created: 2021-08-19
    Date last modified: 2021-08-25
    Python Version: 3.9
"""

import bitdotio
import csv
import pandas
import sqlalchemy

from io import StringIO


def create_new_schema(
    schema_name: str, pg_string: str, bitio_key: str, bitio_username: str
) -> None:
    """Creates a new schema if it does not already exist

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
    schema_name : str
        The schema to check for and/or create.

    Return
    ----------
    None
    """
    schema = f"{bitio_username}/{schema_name}"
    engine = sqlalchemy.create_engine(
        url=pg_string, connect_args={"options": "-c statement_timeout=120s"}
    )
    if not engine.dialect.has_schema(engine.connect(), schema):
        b = bitdotio.bitdotio(bitio_key)
        repo = bitdotio.model.repo.Repo(name=schema_name, is_private=True)
        with b.api_client as api_client:
            # Create an instance of the API class
            api_instance = bitdotio.ApiBitdotio(api_client)
            api_instance.create_repo(repo=repo)


def _psql_insert_copy(table: pandas.io.sql.SQLTable, conn, keys, data_iter):
    """DataFrame.to_sql Insertion method using PostgreSQL COPY FROM.
    Adapted for bit.io from pandas docs: https://pandas.pydata.org/docs/
    Source: https://github.com/bitdotioinc/simple-pipeline/blob/main/simple_pipeline/load.py
    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ", ".join(f'"{k}"' for k in keys)
        table_name = f'"{table.schema}"."{table.name}"'
        sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"
        cur.copy_expert(sql=sql, file=s_buf)


def _truncate_table(engine, schema, table):
    """Truncates (deletes all data from) a table.

    Source: https://github.com/bitdotioinc/simple-pipeline/blob/main/simple_pipeline/load.py
    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
    schema : str
        The destination schema.
    table : str
        The destination table.
    """
    with engine.connect() as conn:
        sql = f"""
            TRUNCATE TABLE "{schema}"."{table}";
        """
        conn.execute(sql)


def to_table(df, destination, pg_string):
    """
    Loads a pandas DataFrame to a bit.io database.
    Source: https://github.com/bitdotioinc/simple-pipeline/blob/main/simple_pipeline/load.py
    Parameters
    ----------
    df : pandas.DataFrame
    destination : str
        Fully qualified bit.io table name.
    pg_string : str
        A bit.io PostgreSQL connection string including credentials.
    """
    # Validation and setup
    if pg_string is None:
        raise ValueError("You must specify a PG connection string.")
    schema, table = destination.split(".")
    engine = sqlalchemy.create_engine(pg_string)

    # Check if table exists and set load type accordingly
    if engine.dialect.has_table(
        connection=engine.connect(), table_name=table, schema=schema
    ):
        _truncate_table(engine, schema, table)
        if_exists = "append"
    else:
        if_exists = "fail"

    with engine.connect() as conn:
        # 10 minute upload limit
        conn.execute("SET statement_timeout = 600000;")
        df.to_sql(
            table,
            conn,
            schema,
            if_exists=if_exists,
            index=False,
            method=_psql_insert_copy,
        )
