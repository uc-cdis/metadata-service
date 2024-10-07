import psycopg2
import time
from . import config, logger

conn = None


def get_discovery_metadata():
    try:
        # connect to the PostgreSQL server
        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(
            host=config.DB_HOST,
            dbname=config.DB_DATABASE,
            user=config.DB_USER,
            password=config.DB_PASSWORD,
            port=5432,
        )

        # Creating a cursor with name cur.
        cur = conn.cursor()
        print("Connected to the PostgreSQL database")

        # Execute a query:
        # To display the PostgreSQL
        # database server version
        postgreSQL_select_Query = "SELECT * FROM metadata WHERE data->>'_guid_type' = 'discovery_metadata' ORDER BY guid limit 2000 OFFSET 0;"
        start_time = time.time()
        logger.info(
            f" Current time stamp -- {start_time} at the beginning of the cur execute"
        )
        cur.execute(postgreSQL_select_Query)
        metadata_records = cur.fetchall()
        end_time = time.time()
        logger.info(
            f" Current time stamp -- {end_time} at the end of the cur execute. Total time spent in seconds = {(end_time - start_time)}"
        )

        # Close the connection
        cur.close()
        return metadata_records

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")
