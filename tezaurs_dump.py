#!/usr/bin/env python3
from db_config import db_connection_info

import psycopg2
from psycopg2.extras import NamedTupleCursor

connection = None
def db_connect():
    global connection

    if db_connection_info is None or db_connection_info["host"] is None or len(db_connection_info["host"])==0:
        print("Postgres connection error: connection information must be supplied in db_config")
        raise Exception("Postgres connection error: connection information must be supplied in <conn_info>")

    print('Connecting to database %s' % (db_connection_info["dbname"],))
    connection = psycopg2.connect(
            host = db_connection_info["host"],
            port = db_connection_info["port"],
            dbname = db_connection_info["dbname"],
            user = db_connection_info["user"],
            password = db_connection_info["password"],
        )

def query(sql, parameters):
    global connection
    cursor = connection.cursor(cursor_factory=NamedTupleCursor) 
    cursor.execute(sql, parameters)
    r = cursor.fetchall()
    cursor.close()
    return r

def fetch_lexemes():
    global connection
    cursor = connection.cursor(cursor_factory=NamedTupleCursor) 
    sql = """
select * from tezaurs.lexemes
limit 10
"""
    cursor.execute(sql)
    while True:
        rows = cursor.fetchmany(1000)
        if not rows:
            break
        for row in rows:
            yield row

def dump_lexemes(filename):
    with open(filename, 'w') as f:
        for lexeme in fetch_lexemes():
            f.write(str(lexeme))
            f.write('\n')

db_connect()

dump_lexemes('test.json')

print('Done!')
