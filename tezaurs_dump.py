#!/usr/bin/env python3
from db_config import db_connection_info

import psycopg2
from psycopg2.extras import NamedTupleCursor
import json

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
select l.id as lexeme_id, e.id as entry_id, e.human_id, p.legacy_id as paradigm_id, l.data, stem1, stem2, stem3, lemma
from tezaurs.lexemes l
join tezaurs.entries e on l.entry_id = e.id
join tezaurs.paradigms p on l.paradigm_id = p.id
where l.type_id = 1 -- words, not named entities or MWE's
"""
# TODO - filtrs uz e.release_id lai ņemtu svaigāko relīzi nevis visas

    cursor.execute(sql)
    while True:
        rows = cursor.fetchmany(1000)
        if not rows:
            break
        for row in rows:
            if not row.paradigm_id:
                continue

            lexeme = {
                'lexeme_id' : row.lexeme_id,
                'entry_id'  : row.entry_id,
                'human_id'  : row.human_id,
                'paradigm'  : row.paradigm_id,
                'lemma'     : row.lemma
            }
            if row.stem1:
                lexeme['stem1'] = row.stem1[1:-1] # noņemam {}
            if row.stem2:
                if ',' in row.stem2:
                    print('Dubultcelms %s'%(row.stem2, ))
                    continue
                lexeme['stem2'] = row.stem2[1:-1] # noņemam {}
            if row.stem3:
                if ',' in row.stem3:
                    print('Dubultcelms %s'%(row.stem3, ))
                    continue
                lexeme['stem3'] = row.stem3[1:-1] # noņemam {}
            if row.data:
                dati = row.data
                gram = dati.get('Gram')
                if gram and gram.get('Flags'):
                    flags = dict(gram.get('Flags'))
                    for key in gram.get('Flags'):   
                        value = flags[key]
                        if type(value) is list and len(value) == 1:
                            flags[key] = value[0]
                            value = flags[key]
                        if key == 'Dzimte' and type(value) is not list and value.endswith(' dzimte'):
                            flags[key] = value[:-7]
                            value = flags[key]
                    gram = dict(gram)
                    gram.update(flags)
                    del gram['Flags']

                if dati.get('Pronunciation'):
                    if not gram:
                        gram = {}
                    gram['Pronunciation'] = dati['Pronunciation']
                    dati = dict(dati)
                    del dati['Pronunciation']

                if not gram or len(dati) != 1:
                    print('Interesting data: %s' % (row.data, ))
                lexeme['attributes'] = gram
            yield lexeme

def dump_lexemes(filename):
    with open(filename, 'w') as f:
        for lexeme in fetch_lexemes():            
            f.write(json.dumps(lexeme, ensure_ascii=False))
            f.write('\n')

db_connect()

dump_lexemes('tezaurs_lexemes.json')

print('Done!')
