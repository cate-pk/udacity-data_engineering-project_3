import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Goal: 
            This function reads data from S3 and load to staging tables. 
    
    Arguments:
            cursor: cursor linked to the database is required
            connection: connection to the database is required            
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Goal: 
            This function will insert tables. 
    
    Arguments:
            cursor: cursor linked to the database is required
            connection: connection to the database is required            
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Goal: 
            - Create connection
            - Set up a cursor
            - Process load_staging_tables and insert_tables function
            - Close connection
    
    Arguments:
            None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()