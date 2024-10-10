#!/usr/bin/env python3

import argparse
import asyncio
import boto3
import os
import psycopg
from psycopg import sql

# Create the argument parser
parser = argparse.ArgumentParser()

# Add the required arguments
parser.add_argument("--host", required=True, help="PostgreSQL host")
parser.add_argument("--database", required=True, help="Database name")
parser.add_argument("--user", required=True, help="Database user")
parser.add_argument("--region", required=True, help="AWS region")
parser.add_argument("--schema", required=True, help="Schema name")

# Parse the arguments
args = parser.parse_args()


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
    # Generate the IAM authentication token
    action = "DbConnectSuperuser"
    expires_in_secs = 900  # You can adjust this value as needed
    token = generate_token(args.host, action, args.region, expires_in_secs)

    # Connect to the PostgreSQL database using the generated token
    conn = await psycopg.AsyncConnection.connect(
        host=args.host,
        dbname=args.database,
        user=args.user,
        password=token,
        autocommit=True  # Enabled autocommit mode
    )

    return conn


async def create_tables():
    conn = await connect_to_database()

    try:
        # SQL command to create the schema if it doesn't exist
        create_schema_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(args.schema))
        await conn.execute(create_schema_query)
    except Exception as e:
        print(f"An error occurred while creating the schema: {e}")

    async with conn.cursor() as cur:
        # SQL command to create 'orders' table
        create_orders_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.orders (
                order_id int PRIMARY KEY,
                customer_id INTEGER NOT NULL,
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_amount NUMERIC(10, 2)
            )
        """).format(sql.Identifier(args.schema))

        # SQL command to create 'accounts' table
        create_accounts_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.accounts (
                account_id int PRIMARY KEY,
                account_name VARCHAR(100),
                email VARCHAR(100) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).format(sql.Identifier(args.schema))

        # Execute the queries to create the tables
        try:
            await cur.execute(create_orders_table_query)
        except Exception as e:
            print(f"An error occurred while creating the 'orders' table: {e}")

        try:
            await cur.execute(create_accounts_table_query)
        except Exception as e:
            print(f"An error occurred while creating the 'accounts' table: {e}")

    # Close the connection
    await conn.close()

    print("Schema and table creation process completed.")


async def main():
    await create_tables()


# Run the script asynchronously
if __name__ == "__main__":
    asyncio.run(main())
