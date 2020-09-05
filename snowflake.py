"""
Module to connect to various databases using python and query data
"""

import cx_Oracle
import psycopg2
import pandas as pd
from io import StringIO
import boto3


class Database:
    """

    """
    def __init__(self, db_type, user, password, host, db_name, query):
        """
        :param db_type: type of database
        :param user: database user
        :param password: database password
        :param host: database host
        :param db_name: database name
        """
        self.conn = None
        self.user = user
        self.password = password
        self.host = host
        self.database = db_name
        self.db_type = db_type
        self.query = query
        self.functions = {"oracle": self.connect_to_oracle,
                          "postgres": self.connect_to_postgres}

    def get_conn(self):
        """
        Get the connection object based on a database type
        """
        self.functions[self.db_type]()

    def connect_to_oracle(self):
        """

        :return: connection object
        """
        self.conn = cx_Oracle.connect(self.user, self.password, self.host)

    def connect_to_postgres(self):
        """

        :return: postgres connection object
        """
        self.conn = psycopg2.connect(host=self.host,database=self.database, user=self.user, password=self.password)

    def close_conn(self):
        """
        Closing the database connection on call when the connection is active
        """
        if self.conn is not None:
            self.conn.close()

    def fetch_data(self):
        """
        query the data from database into a data frame
        :returns
        data frame object
        """
        df = pd.read_sql(self.query, self.conn)
        return df


def write_file_to_s3(df, file_name):
    """

    :param df:
    :param file_name:
    :return:
    """
    bucket = ""
    io_buffer = StringIO()
    df.to_csv(io_buffer)
    s3_obj = boto3.resource('s3')
    s3_obj.Object(bucket, file_name + '.csv').put(Body=io_buffer.getvalue())

