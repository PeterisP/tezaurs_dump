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
            if row.lemma in ['būt']:
                continue # Hardcoded exceptions

            lexeme = {
                'lexeme_id' : row.lexeme_id,
                'entry_id'  : row.entry_id,
                'human_id'  : row.human_id,
                'paradigm'  : row.paradigm_id,
                'lemma'     : row.lemma
            }
            altstem2 = None
            altstem3 = None
            if row.stem1:
                stem = row.stem1
                if stem.startswith('{') and stem.endswith('}'):
                    stem = stem[1:-1] # noņemam {}
                lexeme['stem1'] = stem
            if row.stem2:
                if ',' in row.stem2:
                    assert row.stem2.startswith('{"')
                    assert row.stem2.endswith('"}')
                    stem = row.stem2.split(',', maxsplit=1)[0][2:]
                    altstem2 = row.stem2.split(',', maxsplit=1)[1][:-2]
                    # print('Dubultcelms %s - sadalīju "%s" un "%s"'%(row.stem2, stem, altstem2))
                else:
                    stem = row.stem2
                    if stem.startswith('{') and stem.endswith('}'):
                        stem = stem[1:-1] # noņemam {}
                lexeme['stem2'] = stem
            if row.stem3:
                if ',' in row.stem3:
                    assert row.stem3.startswith('{"')
                    assert row.stem3.endswith('"}')
                    stem = row.stem3.split(',', maxsplit=1)[0][2:]
                    altstem3 = row.stem3.split(',', maxsplit=1)[1][:-2]
                else:
                    stem = row.stem3
                    if stem.startswith('{') and stem.endswith('}'):
                        stem = stem[1:-1] # noņemam {}
                lexeme['stem3'] = stem

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

                if gram and (gram.get('Citi') == 'Pārejošs' or gram.get('Citi') == 'Nepārejošs'):
                    gram['Transitivitāte'] = gram.get('Citi')
                    del gram['Citi']

                if gram and gram.get('Lietojuma ierobežojumi'):
                    if isinstance(gram.get('Lietojuma ierobežojumi'), str):
                        gram['Lietojuma ierobežojumi'] = [gram.get('Lietojuma ierobežojumi')]
                    for i in gram['Lietojuma ierobežojumi']:
                        if i in ['Sarunvaloda', 'Vēsturisks', 'Novecojis', 'Nevēlams', 'Žargonvārds', 'Apvidvārds', 'Neaktuāls', 'Īsziņās', 'Neliterārs', 'Vulgārisms', 'Barbarisms', 'Bērnu valoda', 'Biblisms']:
                            gram['Lietojums'] = i        
                        elif i in ['Poētiska stilistiskā nokrāsa', 'Vienkāršrunas stilistiskā nokrāsa', 'Nievājoša ekspresīvā nokrāsa', 'Sirsnīga emocionālā nokrāsa', 'Ironiska ekspresīvā nokrāsa', 'Folkloras valodai raksturīga stilistiskā nokrāsa', 'Humoristiska ekspresīvā nokrāsa', 'Pārnestā nozīmē']:
                            gram['Stils'] = i
                        else:
                            print("Nesaprasti lietojuma ierobežojumi: '%s'" % (i,))
                    del gram['Lietojuma ierobežojumi']

                if gram and gram.get('Lietojuma biežums'):
                    gram['Biežums'] = gram.get('Lietojuma biežums')
                    del gram['Lietojuma biežums']

                if gram and gram.get('Joma'):
                    gram['Nozare'] = gram.get('Joma')
                    del gram['Joma']

                if gram and gram.get('Darbības vārda prefikss'):
                    gram['Priedēklis'] = gram.get('Darbības vārda prefikss')
                    del gram['Darbības vārda prefikss']

                if not gram or len(dati) != 1:
                    print('Interesting data: %s' % (row.data, ))
                lexeme['attributes'] = gram
            yield lexeme
            if altstem2 or altstem3:
                if altstem2:
                    lexeme['stem2'] = altstem2
                if altstem3:
                    lexeme['stem3'] = altstem3
                yield lexeme

def dump_lexemes(filename):
    with open(filename, 'w') as f:
        for lexeme in fetch_lexemes():            
            f.write(json.dumps(lexeme, ensure_ascii=False))
            f.write('\n')

db_connect()

dump_lexemes('tezaurs_lexemes.json')

print('Done!')
