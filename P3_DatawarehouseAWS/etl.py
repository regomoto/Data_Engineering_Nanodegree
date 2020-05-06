import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Inserts data from the JSON files into the songs staging table 
    and events staging tables using the queries in the 'copy_table_queries' list
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data into dimension table from the staging tables using the 'insert_table_queries' list
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Establishes connection with the AWS cluster using information in the 'dwh.cfg' file
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    """
    
    - Loads staging tables using the JSON files
    
    - Inserts data into dimension tables
    
    - Closes connection
    
    """
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()