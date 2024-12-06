#!/usr/bin/env python3
#
# load_generator.py - Load test script for bulk data insertion into a Aurora DSQL
#
# Copyright 2024 Amazon.com, Inc. or its affiliates. All Rights Reserved.

import boto3
import psycopg
from psycopg import sql, AsyncConnection
import psycopg.sql as sql
import argparse
import asyncio
import random
import time
import sys
import secrets


async def connect_to_database():
    """
    Connect to the PostgreSQL database using an inline IAM token generation.
    :return: psycopg.AsyncConnection
    """
    # Generate the IAM authentication token inline
    client = boto3.client("dsql", region_name=args.region)
    password_token = client.generate_db_connect_admin_auth_token(args.host, args.region)

    # Connect to the PostgreSQL database using the generated token
    conn = await AsyncConnection.connect(
        host=args.host,
        dbname=args.database,
        user=args.user,
        password=password_token,
        sslmode="require",
        autocommit=True
    )
    return conn

async def main():
    parse_args(sys.argv[1:])
    
    await load_test()

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
    if not conn:
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
    """Perform bulk data loading serially using INSERT."""
    conn = await connect_to_database()
    if not conn:
        print("Failed to connect to the database in loader. Exiting.")
        return

    cur = conn.cursor()

    while True:
        # Check if 10 minutes have passed
        elapsed_time = time.time() - start_time
        if elapsed_time >= 10 * 60:  # Stop after 10 minutes
            break

        # Get a batch of data
        batch = await sfr.get_n_rows(args.batchsize)

        for row in batch:
            try:
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

            except Exception as e:
                print(f"Error during insert: {e}")
                break

        await conn.commit()  # Commit after processing the batch

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
