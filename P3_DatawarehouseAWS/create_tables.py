import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function drops existing tables. It will reset the database everytime we test the ETL pipeline
    Uses the 'drop_table_queries list' to run all of the drop queries
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create staging, dimension, and fact tables in the sql_queries.py script using the 'create_table_queries' list
    """
    for query in create_table_queries:
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
    
    - Drops all tables
    
    - Creates all tables
    
    - Closes connection
    
    """
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()