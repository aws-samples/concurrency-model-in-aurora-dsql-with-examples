#!/usr/bin/env python3
#
# retry_backoff_jitter.py - Load testing script for bulk data insertion into a PostgreSQL table with built-in retry mechanisms, allowing multiple retry attempts before failure.
#
# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.
import boto3
import psycopg
import psycopg.sql as sql
import argparse
import asyncio
import random
import time
import sys
import secrets

def generate_token(hostname, action, region, expires_in_secs=900):
    """
    Generates an IAM authentication token.

    :param str hostname: The hostname of the cluster.
    :param str action: The specific IAM action for which the token is required.
    :param str region: The AWS region where the cluster is located.
    :param int expires_in_secs: The duration (in seconds) for which the generated token will remain valid.
    :return: A generated authentication token.
    :rtype: str
    """
    client = boto3.client("axdbfrontend", region_name=region)
    try:
        response = client.generate_db_auth_token(hostname, action, region, str(expires_in_secs))
        return response
    except TypeError as e:
        print(f"Error generating token: {e}")
        print("Attempting to call without expires_in_secs...")
        response = client.generate_db_auth_token(hostname, action)
        return response


async def connect_to_database():
    """
    Establishes an asynchronous connection to the database using IAM authentication.

    :return: An asynchronous database connection.
    :rtype: psycopg.AsyncConnection
    """
    global args

    # Generate the IAM authentication token
    action = "DbConnectSuperuser"
    expires_in_secs = 900  # You can adjust this value as needed
    token = generate_token(args.host, action, args.region, expires_in_secs)

    try:
        # Connect to the PostgreSQL database using the generated token
        conn = await psycopg.AsyncConnection.connect(
            host=args.host,
            dbname=args.database,
            user=args.user,
            password=token,
            autocommit=True  # Autocommit mode to handle each insert independently
        )
        print("Successfully connected to the database.")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None  

async def main():
    parse_args(sys.argv[1:])
    
    await load_test()

async def load_test():
    conn = await connect_to_database()
    if conn is None:
        print("Failed to connect to the database. Exiting.")
        return

    # Fetch column names and types dynamically
    column_list = await get_table_columns(conn, args.schema, args.tablename)

    # Initialize the DataGenerator with the column list
    sfr = DataGenerator(column_list)

    start_time = time.time()

    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(loader(sfr, start_time)) for _ in range(int(args.threads))]
    await asyncio.gather(*tasks)

    total_time = time.time() - start_time
    print(f"Load test completed in {total_time / 60:.2f} minutes")

    await conn.close()

class DataGenerator:
    """Simple class to generate random test data based on the table column types."""

    def __init__(self, column_list):
        # Store column list with types
        self.column_list = column_list
    
    def generate_row(self):
        # Generate a random row of data based on the expected column types
        row = []
        secure_random = random.SystemRandom()  # Use SystemRandom for cryptographically secure random generation

        for column_name, column_type in self.column_list:
            if column_type == 'integer':
                row.append(secrets.randbelow(100000))  # Generate random integer using secrets
            elif column_type == 'numeric' or column_type == 'float':
                row.append(secure_random.random() * 1000)  # Generate random float securely
            elif column_type == 'varchar' or column_type == 'text':
                row.append(f"user_{secrets.randbelow(100000)}@test.com")  # Generate random email using secrets
            else:
                row.append(None)  # Default for unsupported types
        return row


    async def get_n_rows(self, n_rows):
        """Generate `n_rows` of test data."""
        rows = []
        for _ in range(n_rows):
            rows.append(self.generate_row())
        return rows



async def connect_db():
    """Establish a connection to PostgreSQL."""
    conn = await psycopg.AsyncConnection.connect(
        host=args.host,
        dbname=args.database,
        user=args.user,
        password=args.password,
        autocommit=True  # Autocommit mode to handle each insert independently
    )
    return conn

async def get_table_columns(conn, schema, table_name):
    """Fetch the column names and their data types from the given table."""
    async with conn.cursor() as cur:
        await cur.execute(
            sql.SQL("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """), (schema, table_name)
        )
        columns = await cur.fetchall()
        column_list = [(col[0], col[1]) for col in columns]  # Extract column names and types

        return column_list

async def load_test():
    conn = await connect_to_database()
    if conn is None:
        print("Failed to connect to the database. Exiting.")
        return

    # Fetch column names and types dynamically
    column_list = await get_table_columns(conn, args.schema, args.tablename)

    # Initialize the DataGenerator with the column list
    sfr = DataGenerator(column_list)

    start_time = time.time()

    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(loader(sfr, start_time)) for _ in range(int(args.threads))]
    await asyncio.gather(*tasks)

    total_time = time.time() - start_time
    print(f"Load test completed in {total_time / 60:.2f} minutes")

    await conn.close()


async def loader(sfr, start_time):
    """Perform bulk data loading serially using INSERT with backoff and jitter."""
    conn = await connect_to_database()
    if conn is None:
        print("Failed to connect to the database. Exiting loader.")
        return
    cur = conn.cursor()

    max_retries = 5
    base_backoff = 1  # Initial backoff time (in seconds)

    while True:
        # Check if 15 minutes have passed
        elapsed_time = time.time() - start_time
        if elapsed_time >= 15 * 60:
            break  # Stop after 15 minutes

        # Get a batch of data
        batch = await sfr.get_n_rows(args.batchsize)
        retry_attempts = 0

        while retry_attempts < max_retries:
            try:
                for row in batch:
                    # Use INSERT statement for each row
                    insert_cmd = sql.SQL("""
                        INSERT INTO {} ({}) 
                        VALUES ({})
                        ON CONFLICT DO NOTHING
                    """).format(
                        sql.Identifier(args.schema, args.tablename),
                        sql.SQL(', ').join(map(sql.Identifier, [col[0] for col in sfr.column_list])),  # Column names
                        sql.SQL(', ').join(sql.Placeholder() * len(row))  # Placeholders for values
                    )
                    await cur.execute(insert_cmd, row)  # Execute insert for each row

                await conn.commit()  # Commit the transaction
                break  # Exit retry loop on success

            except Exception as e:
                retry_attempts += 1
                backoff_time = base_backoff * (2 ** retry_attempts)  # Exponential backoff
                jitter = secrets.SystemRandom().uniform(0, backoff_time * 0.1)  # Add some jitter (10% of backoff time)
                sleep_time = backoff_time + jitter

                print(f"Error during batch insert: {e}, retrying in {sleep_time:.2f} seconds (attempt {retry_attempts}/{max_retries})")
                await asyncio.sleep(sleep_time)

                if retry_attempts >= max_retries:
                    print(f"Max retries reached for this batch. Skipping the batch after {max_retries} attempts.")
                    break  # Exit the retry loop after reaching the max retries

    await cur.close()
    await conn.close()

def parse_args(in_args):
    global args

    # Create the argument parser
    parser = argparse.ArgumentParser()

    parser.add_argument("--host", required=True, help="PostgreSQL host")
    parser.add_argument("--database", required=True, help="Database name")
    parser.add_argument("--user", required=True, help="Database user")
    parser.add_argument("--region", required=True, help="AWS region")
    parser.add_argument("--tablename", required=True, help="Target table name")
    parser.add_argument("--schema", required=False, default="public", help="Schema for specified table")
    parser.add_argument("--batchsize", required=False, default=1000, type=int, help="Commit batch size")
    parser.add_argument("--threads", required=False, default=1, type=int, help="Number of threads for parallel loading")

    # Parse the arguments
    args = parser.parse_args(in_args)

if __name__ == '__main__':
    asyncio.run(main())
