from os import getenv
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.schema import DropTable


class Connection:
    def __init__(
        self,
        dialect=None,
        user=None,
        pwd=None,
        server=None,
        port=None,
        db=None,
        schema=None,
        creds=None,
    ):
        self.dialect = dialect
        self.schema = schema
        self.db = db
        self.options = {
            "username": user,
            "password": pwd,
            "host": server,
            "port": port,
            "database": db,
        }
        self.creds = creds
        self.helpers = {}
        self.engine = self._get_engine()

    def _cstr(self):
        """
        Constructs SQLAlchemy connection string.
        """
        return URL(self.dialect, **self.options)

    def _add_bigquery_options(self):
        """
        Adds BigQuery specific connection options for creating engine.
        """
        if self.dialect == "bigquery":
            self._get_bigquery_creds()
            self.helpers["credentials_path"] = self.creds

    def _get_bigquery_creds(self):
        """
        Sets bigquery specific authentication variables.
        """
        credentials = service_account.Credentials.from_service_account_file(self.creds)
        pandas_gbq.context.credentials = credentials
        pandas_gbq.context.project = self.db

    def _get_engine(self):
        """
        Sets database engine for connections.
        """
        self._add_bigquery_options()
        return create_engine(self._cstr(), **self.helpers)

    def query(self, sql_query, params=None):
        """
        Returns results of SQL query as pandas dataframe.
        """
        if self.dialect == "bigquery":
            return pandas_gbq.read_gbq(sql_query, progress_bar_type=None)
        else:
            return pd.read_sql_query(sql_query, con=self.engine, params=params)

    def insert_into(self, table, df, if_exists="append", chunksize=None, dtype=None):
        """
        Inserts pandas dataframe into database table.
        """
        if self.dialect == "bigquery":
            pandas_gbq.to_gbq(
                dataframe=df,
                destination_table=f"{self.schema}.{table}",
                if_exists=if_exists,
                chunksize=chunksize,
                table_schema=dtype,
            )
        else:
            df.to_sql(
                name=table,
                con=self.engine,
                schema=self.schema,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize,
                dtype=dtype,
            )

    def table(self, tablename):
        """
        Returns SQLAlchemy Table object.
        """
        return Table(
            tablename,
            MetaData(),
            autoload=True,
            autoload_with=self.engine,
            schema=self.schema,
        )

    def drop_table(self, tablename):
        """
        Drops database table if it exists.
        """
        try:
            table = self.table(tablename)
            self.engine.execute(DropTable(table))
        except NoSuchTableError:
            pass

    def read_table(self, tablename):
        """
        Returns all data from a specified database table. 
        """
        if self.dialect == "bigquery":
            return self.query(f"SELECT * FROM {self.schema}.{tablename}")
        else:
            return pd.read_sql_table(tablename, con=self.engine, schema=self.schema)
