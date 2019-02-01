#!/usr/bin/env python3

import yaml
import mysql.connector
import argparse

def fetch_table(user, passw, dbname, dbtable):
    "Fetch all entries from a database table."
    conn = mysql.connector.connect(user=user, password=passw, host='localhost', database=dbname)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM %s" % dbtable)
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dump a table from a SQL database in YAML.")
    parser.add_argument('-u', dest='user', required=True, help='The database user')
    parser.add_argument('-p', dest='passw', required=True, help='The database password')
    parser.add_argument('-db', dest='db', required=True, help='The database name')
    parser.add_argument('-t', dest='table', required=True, help='The database table to dump')
    parser.add_argument('-w', dest='output', required=True, help='The output YAML file')
    args = parser.parse_args()

    records = fetch_table(args.user, args.passw, args.db, args.table)
    print(records)
