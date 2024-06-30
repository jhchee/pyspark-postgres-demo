from typing import List, Iterable, Dict

from psycopg2 import connect, DatabaseError
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame


def get_postgres_connection(host: str,
                            database: str,
                            user: str,
                            password: str,
                            port: str):
    """
    Connect to postgres database and get the connection.
    :param host: host name of database instance.
    :param database: name of the database to connect to.
    :param user: username.
    :param password: password for the username.
    :param port: port to connect.
    :return: Database connection.
    """
    try:
        conn = connect(
            host=host, database=database,
            user=user, password=password,
            port=port
        )

    except (Exception, DatabaseError) as ex:
        print("Unable to connect to database !!")
        raise ex

    return conn


def batch_and_upsert(dataframe_partition: Iterable,
                     sql: str,
                     database_credentials: dict,
                     batch_size: int = 1000):
    """
    Batch the input dataframe_partition as per batch_size and upsert
    to postgres using psycopg2 execute values.
    :param dataframe_partition: Pyspark DataFrame partition or any iterable.
    :param sql: query to insert/upsert the spark dataframe partition to postgres.
    :param database_credentials: postgres database credentials.
        Example: database_credentials = {
                host: <host>,
                database: <database>,
                user: <user>,
                password: <password>,
                port: <port>
            }
    :param batch_size: size of batch per round trip to database.
    :return: total records processed.
    """
    conn, cur = None, None
    counter = 0
    batch = []

    for record in dataframe_partition:

        counter += 1
        batch.append(record)

        if not conn:
            conn = get_postgres_connection(**database_credentials)
            cur = conn.cursor()

        if counter % batch_size == 0:
            execute_values(
                cur=cur, sql=sql,
                argslist=batch,
                page_size=batch_size
            )
            conn.commit()
            batch = []

    if batch:
        execute_values(
            cur=cur, sql=sql,
            argslist=batch,
            page_size=batch_size
        )
        conn.commit()

    if cur:
        cur.close()
    if conn:
        conn.close()

    yield counter


def build_upsert_query(cols: List[str],
                       table_name: str,
                       unique_key: List[str],
                       cols_not_for_update: List[str] = None) -> str:
    """
    Builds postgres upsert query using input arguments.
    Example : build_upsert_query(
        ['col1', 'col2', 'col3', 'col4'],
        "my_table",
        ['col1'],
        ['col2']
    ) ->
    INSERT INTO my_table (col1, col2, col3, col4) VALUES %s  
    ON CONFLICT (col1) DO UPDATE SET (col3, col4) = (EXCLUDED.col3, EXCLUDED.col4) ;
    :param cols: the postgres table columns required in the 
        insert part of the query.
    :param table_name: the postgres table name.
    :param unique_key: unique_key of the postgres table for checking 
        unique constraint violations.
    :param cols_not_for_update: columns in cols which are not required in
        the update part of upsert query.
    :return: Upsert query as per input arguments.
    """

    cols_str = ', '.join(cols)

    insert_query = """ INSERT INTO %s (%s) VALUES %%s """ % (
        table_name, cols_str
    )

    if cols_not_for_update is not None:
        cols_not_for_update.extend(unique_key)
    else:
        cols_not_for_update = [col for col in unique_key]

    unique_key_str = ', '.join(unique_key)

    update_cols = [col for col in cols if col not in cols_not_for_update]
    update_cols_str = ', '.join(update_cols)

    update_cols_with_excluded_markers = [f'EXCLUDED.{col}' for col in update_cols]
    update_cols_with_excluded_markers_str = ', '.join(update_cols_with_excluded_markers)

    on_conflict_clause = """ ON CONFLICT (%s) DO UPDATE SET (%s) = (%s) ;""" % (
        unique_key_str,
        update_cols_str,
        update_cols_with_excluded_markers_str
    )

    return insert_query + on_conflict_clause


def upsert_spark_df_to_postgres(dataframe_to_upsert: DataFrame,
                                table_name: str,
                                table_unique_key: List[str],
                                database_credentials: Dict[str, str],
                                batch_size: int = 1000,
                                parallelism: int = 1) -> None:
    """
    Upsert a spark DataFrame into a postgres table.
    Note: If the target table lacks any unique index, data will be appended through
    INSERTS as UPSERTS in postgres require a unique constraint to be present in the table.
    :param dataframe_to_upsert: spark DataFrame to upsert to postgres.
    :param table_name: postgres table name to upsert.
    :param table_unique_key: postgres table primary key.
    :param database_credentials: database credentials.
    :param batch_size: desired batch size for upsert.
    :param parallelism: No. of parallel connections to postgres database.
    :return:None
    """
    upsert_query = build_upsert_query(
        cols=dataframe_to_upsert.schema.names,
        table_name=table_name, unique_key=table_unique_key
    )
    upsert_stats = dataframe_to_upsert.coalesce(parallelism).rdd.mapPartitions(
        lambda dataframe_partition: batch_and_upsert(
            dataframe_partition=dataframe_partition,
            sql=upsert_query,
            database_credentials=database_credentials,
            batch_size=batch_size
        )
    )

    total_recs_loaded = 0

    for counter in upsert_stats.collect():
        total_recs_loaded += counter

    print("")
    print("#################################################")
    print(f" Total records loaded - {total_recs_loaded}")
    print("#################################################")
    print("")
