#!/usr/bin/env python3

import mysql.connector
import argparse
import csv

def fetch_table(user, passw, dbname, dbtable):
    "Fetch all entries from a database table."
    conn = mysql.connector.connect(user=user, password=passw, host='localhost', database=dbname)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM %s" % dbtable)
    records = cursor.fetchall()
    cursor.close()
    conn.close()
    return records

def dump_to_csv(data, csvFile):
    "Dump the data to a csv file."
    with open(csvFile, 'w') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dump a table from a SQL database.")
    parser.add_argument('-u', dest='user', required=True, help='The database user')
    parser.add_argument('-p', dest='passw', required=True, help='The database password')
    parser.add_argument('-db', dest='db', required=True, help='The database name')
    parser.add_argument('-t', dest='table', required=True, help='The database table to dump')
    parser.add_argument('-w', dest='outfile', required=True, help='The output file')
    args = parser.parse_args()

    records = fetch_table(args.user, args.passw, args.db, args.table)
    dump_to_csv(records, args.outfile)
