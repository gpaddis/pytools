#!/usr/bin/env python3

from collections import OrderedDict
import argparse
import csv
import yaml
from pymysql import connect
from pymysql.cursors import DictCursorMixin, Cursor


class OrderedDictCursor(DictCursorMixin, Cursor):
    dict_type = OrderedDict

def fetch_table(host, user, passw, dbname, dbtable):
    "Fetch all entries from a database table."
    conn = connect(user=user, password=passw, host=host, database=dbname)
    cursor = conn.cursor(OrderedDictCursor)
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

def dump_to_yaml(data, yamlFile):
    "Dump the data to a yaml file"
    def noop(self, *args, **kw):
        "Don't emit tags: see https://stackoverflow.com/a/48823424/7874784"
        pass
    yaml.emitter.Emitter.process_tag = noop
    yaml.add_representer(
        dict, lambda self, data: yaml.representer.SafeRepresenter.represent_dict(self, data.items()))
    with open(yamlFile, 'w') as outfile:
        outfile.writelines(yaml.dump(data, default_flow_style=False))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dump a table from a SQL database.")
    parser.add_argument('-host', dest='host', required=False, default='localhost',
                        help='The database host')
    parser.add_argument('-u', dest='user', required=True,
                        help='The database user')
    parser.add_argument('-p', dest='passw', required=True, help='The database password')
    parser.add_argument('-db', dest='db', required=True, help='The database name')
    parser.add_argument('-t', dest='table', required=True, help='The database table to dump')
    parser.add_argument('-w', dest='outfile',
                        required=True, help='The output file')
    parser.add_argument('-f', dest='format',
                        required=True, help='The output format desired (csv, yaml)')
    args = parser.parse_args()

    records = fetch_table(args.host, args.user, args.passw, args.db, args.table)

    if args.format == 'csv':
        dump_to_csv(records, args.outfile)
    elif args.format == 'yaml':
        dump_to_yaml(records, args.outfile)
