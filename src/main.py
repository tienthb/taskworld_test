"""Entry point for the ETL application

Sample usage:
docker-compose run etl python main.py \
    --source /opt/data/activity.csv \
    --database warehouse
    --table user_activity
"""

# TODO: Implement a pipeline that loads the provided activity.csv file, performs the required
# transformations, and loads the result into the PostgreSQL table.

# Note: You can write the ETL flow with regular Python code, or you can also make use of a
# framework such as PySpark or others. The choice is yours.

import os
import argparse
from controller import pg_controller

def parse_args():
    parser = argparse.ArgumentParser(description="ETL application")
    parser.add_argument("--source", help="Path to the source file")
    parser.add_argument("--database", help="Database name for inserting data")
    parser.add_argument("--table", help="Table name for inserting data")
    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    pg_db = args.database
    pg = pg_controller.PGController(pg_db)
    source = args.source
    table = args.table
    pg.calc_user_activity(source, table)

if __name__ == "__main__":
    main()