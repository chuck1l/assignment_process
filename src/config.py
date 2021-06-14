import os
import atexit
import psycopg2
import logging


def database_disconnect(db_connection=None):
    logging.info("Disconnecting from the database")
    db_connection.close()


def database_connect():
    """ Connected to the dw_main data warehouse """
    conn = None
    try:
        conn = psycopg2.connect(dbname="dw_main",
                           user=os.getenv("REDSHIFT_USER"),
                           host="rs-prd-bi-cluster.cdw0722fdg2r.us-east-1.redshift.amazonaws.com",
                           password=os.getenv("REDSHIFT_PASSWORD"),
                           port=5439)
        atexit.register(database_disconnect, db_connection=conn)
        logging.info("Connected to DB")
        return conn

    except psycopg2.OperationalError:
        logging.error("OOPS, I am unable to connect to the database")