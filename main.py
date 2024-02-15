import logging
import os
import sys
import threading
import time
import psycopg
from typing import LiteralString, cast

DATABASE_URL = "postgresql://postgres:password@localhost:5432/postgres"

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(name)s - %(asctime)s - %(message)s")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger.addHandler(handler)


def create_connection() -> psycopg.Connection:
    conn = psycopg.connect(DATABASE_URL, autocommit=True)
    return conn


def create_parent_table(connection: psycopg.Connection):
    create_parent_table_query = (
        "CREATE TABLE parent (data INTEGER, date TIMESTAMP) PARTITION BY LIST (date);"
    )
    create_default_partition_query = (
        f"CREATE TABLE parent_default PARTITION OF parent FOR VALUES IN ('2021-01-02');"
    )
    with connection.cursor() as cursor:
        cursor.execute(create_parent_table_query)
        cursor.execute(create_default_partition_query)


def create_child_table(date: str, sleep: int = 5):
    logger = logging.getLogger("CREATE PARTITION WORKER")

    logger.info("Creating connection...")
    connection = create_connection()

    query = cast(
        LiteralString,
        f"CREATE TABLE child_{date} PARTITION OF parent FOR VALUES IN ('{date}');",
    )

    with connection.cursor() as cursor:
        with connection.transaction():
            cursor.execute(query)
            logger.info(f"Waiting for {sleep} seconds...")
            time.sleep(sleep)
            logger.info("Transaction committed.")


def create_and_attach_child_table(date: str, sleep: int = 5):
    logger = logging.getLogger("ATTACH PARTITION WORKER")

    logger.info("Creating connection...")
    connection = create_connection()

    create_query = cast(
        LiteralString, f"CREATE TABLE child_{date} (LIKE parent INCLUDING ALL)"
    )
    attach_query = cast(
        LiteralString,
        f"ALTER TABLE parent ATTACH PARTITION child_{date} FOR VALUES IN ('{date}');",
    )
    with connection.cursor() as cursor:
        with connection.transaction():
            cursor.execute(create_query)
            cursor.execute(attach_query)
            logger.info(f"Waiting for {sleep} seconds...")
            time.sleep(sleep)
            logger.info("Transaction committed.")


def select_from_parent_table(date: str):
    logger = logging.getLogger("SELECT WORKER")

    logger.info("Creating connection...")
    connection = create_connection()
    t1 = time.perf_counter()

    query = f"SELECT * FROM parent WHERE date = '{date}';"
    with connection.cursor() as cursor:
        cursor.execute(cast(LiteralString, query))

    t2 = time.perf_counter()
    logger.info(f"Select executed in {t2 - t1} seconds.")


def insert_into_parent_table(date: str):
    logger = logging.getLogger("INSERT WORKER")

    logger.info("Creating connection...")
    connection = create_connection()

    t1 = time.perf_counter()

    query = f"INSERT INTO parent (data, date) VALUES (1, '{date}');"
    with connection.cursor() as cursor:
        cursor.execute(cast(LiteralString, query))

    t2 = time.perf_counter()
    logger.info(f"Insert executed in {t2 - t1} seconds.")


def cleanup(connection: psycopg.Connection, date: str):
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE parent;")
        # cursor.execute(cast(LiteralString, f"DROP TABLE child_{date};"))


def main():
    clean_up_conn = psycopg.connect(DATABASE_URL, autocommit=True)

    create_parent_table(clean_up_conn)

    thread_1 = threading.Thread(
        target=create_and_attach_child_table,
        args=("2021_01_01",),
        kwargs={"sleep": 5},
    )
    thread_2 = threading.Thread(target=select_from_parent_table, args=("2021_01_02",))
    thread_3 = threading.Thread(target=insert_into_parent_table, args=("2021_01_02",))

    thread_1.start()
    time.sleep(1)
    thread_3.start()
    thread_2.start()

    thread_3.join()
    thread_1.join()
    thread_2.join()

    logger.info("Threads finished.")

    cleanup(clean_up_conn, "2021_01_01")
    clean_up_conn.close()


if __name__ == "__main__":
    main()
