#!/usr/bin/env python3
from db_config import db_connection_info

import psycopg2
from psycopg2.extras import NamedTupleCursor
import json
from collections import Counter

connection = None
attribute_stats = Counter()


def db_connect():
    global connection

    if db_connection_info is None or db_connection_info["host"] is None or len(db_connection_info["host"]) == 0:
        print("Postgres connection error: connection information must be supplied in db_config")
        raise Exception("Postgres connection error: connection information must be supplied in <conn_info>")

    print('Connecting to database %s' % (db_connection_info["dbname"],))
    connection = psycopg2.connect(
            host=db_connection_info["host"],
            port=db_connection_info["port"],
            dbname=db_connection_info["dbname"],
            user=db_connection_info["user"],
            password=db_connection_info["password"],
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
    global attribute_stats
    cursor = connection.cursor(cursor_factory=NamedTupleCursor)
    sql = """
select l.id as lexeme_id, e.id as entry_id, e.human_id,
  p.legacy_id as paradigm_id, l.data, stem1, stem2, stem3, lemma
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
                continue  # Hardcoded exceptions

            lexeme = {
                'lexeme_id': row.lexeme_id,
                'entry_id': row.entry_id,
                'human_id': row.human_id,
                'paradigm': row.paradigm_id,
                'lemma': row.lemma
            }
            altstem2 = None
            altstem3 = None
            if row.stem1:
                stem = row.stem1
                if stem.startswith('{') and stem.endswith('}'):
                    stem = stem[1:-1]  # noņemam {}
                lexeme['stem1'] = stem
            if row.stem2:
                if ',' in row.stem2:
                    stem = row.stem2.split(',', maxsplit=1)[0]
                    altstem2 = row.stem2.split(',', maxsplit=1)[1]
                    # print('Dubultcelms %s - sadalīju "%s" un "%s"'%(row.stem2, stem, altstem2))
                else:
                    stem = row.stem2
                    if stem.startswith('{') and stem.endswith('}'):
                        stem = stem[1:-1]  # noņemam {}
                lexeme['stem2'] = stem
            if row.stem3:
                if ',' in row.stem3:
                    stem = row.stem3.split(',', maxsplit=1)[0]
                    altstem3 = row.stem3.split(',', maxsplit=1)[1]
                else:
                    stem = row.stem3
                    if stem.startswith('{') and stem.endswith('}'):
                        stem = stem[1:-1]  # noņemam {}
                lexeme['stem3'] = stem

            if row.data:
                dati = row.data
                gram = dati.get('Gram')
                if gram and gram.get('Flags'):
                    flags = dict(gram.get('Flags'))
                    if 'Vārda daļa' in str(flags.get('Kategorija')):
                        continue

                    for key in dict(flags):
                        value = flags[key]
                        if type(value) is list and len(value) == 1:
                            flags[key] = value[0]

                    if flags.get('Kategorija') == 'Ģenitīvenis':
                        if flags.get('Locīšanas īpatnības') == 'Sastingusi forma':
                            del flags['Locīšanas īpatnības']
                        else:
                            print('Ģenitīvenim dīvainas "Locīšanas īpatnības"', row.lemma, dati)

                        if flags.get('Citi') == 'Nelokāms vārds':
                            del flags['Citi']
                        elif flags.get('Citi') == ['Nelokāms vārds', 'Vietvārds']:
                            del flags['Citi']
                        else:
                            print('Ģenitīvenim dīvaini "Citi"', row.lemma, dati)

                        if flags.get('Locījums') == 'Ģenitīvs':
                            del flags['Locījums']

                        if flags.get('Skaitlis') == 'Daudzskaitlis':
                            del flags['Skaitlis']
                            flags['Skaitlis 2'] = 'Daudzskaitlinieks'

                    if type(flags.get('Kategorija')) is not list:
                        if flags.get('Kategorija'):
                            flags['Kategorija'] = [flags.get('Kategorija')]
                        else:
                            flags['Kategorija'] = []

                    if row.paradigm_id in [33, 34]:
                        if 'Lietvārds' in set(flags['Kategorija']) and 'Atgriezeniskais lietvārds' in set(flags['Kategorija']):
                            flags['Kategorija'].remove('Atgriezeniskais lietvārds')

                    if row.paradigm_id == 50:
                        if 'Nekārtns darbības vārds' in set(flags['Kategorija']) and 'Darbības vārds' in set(flags['Kategorija']):
                            flags['Kategorija'].remove('Nekārtns darbības vārds')

                    if row.paradigm_id in (13, 30, 40, 41, 42, 43):
                        for tips in ['Lokāmais ciešamās kārtas tagadnes divdabis (-ams, -ama, -āms, -āma)',
                                     'Lokāmais darāmās kārtas tagadnes divdabis (-ošs, -oša)',
                                     'Lokāmais ciešamās kārtas pagātnes divdabis (-ts, -ta)',
                                     'Lokāmais darāmās kārtas pagātnes divdabis (-is, -usi, -ies, -usies)']:
                            if tips in set(flags.get('Kategorija')):
                                flags['Piezīmes'] = tips
                                flags['Kategorija'] = 'Īpašības vārds'

                    if len(flags['Kategorija']) == 1:
                        flags['Kategorija'] = flags['Kategorija'][0]
                    if len(flags['Kategorija']) == 0:
                        del flags['Kategorija']

                    for key in dict(flags):
                        value = flags[key]
                        if key == 'Dzimte' and type(value) is not list and value.endswith(' dzimte'):
                            flags[key] = value[:-7]
                            value = flags[key]
                        # if key == 'Skaitlis':
                        #     if value == 'Vienskaitlis':
                        #         del flags[key]
                        #     else:
                        #         print(value, row.lemma, flags)
                        if key == 'Locījums':
                            print(value, row.lemma, flags)

                    if flags.get('Skaitlis'):
                        del flags['Skaitlis']  # Nav precīza informācija, konfliktē ar analizatora prasībām
                    gram = dict(gram)
                    gram.update(flags)
                    del gram['Flags']

                if dati.get('Pronunciations'):
                    if not gram:
                        gram = {}
                    gram['Pronunciations'] = dati['Pronunciations']
                    dati = dict(dati)
                    del dati['Pronunciations']

                if gram and gram.get('Citi'):
                    if type(gram['Citi']) is not list:
                        gram['Citi'] = [gram['Citi']]

                    if 'Pārejošs' in set(gram['Citi']):
                        gram['Transitivitāte'] = 'Pārejošs'
                        gram['Citi'].remove('Pārejošs')
                    if 'Nepārejošs' in set(gram['Citi']):
                        gram['Transitivitāte'] = 'Nepārejošs'
                        gram['Citi'].remove('Nepārejošs')
                    if 'Vārds bez priedēkļa' in set(gram['Citi']):
                        gram['Citi'].remove('Vārds bez priedēkļa')
                    if 'Nelokāms vārds' in set(gram['Citi']) and row.paradigm_id in [12, 49]:
                        gram['Citi'].remove('Nelokāms vārds')
                    if 'Noteiktā galotne' in set(gram['Citi']) and row.paradigm_id in [30, 40]:
                        gram['Citi'].remove('Noteiktā galotne')
                    if 'Refleksīvs' in set(gram['Citi']) and row.paradigm_id in [18, 19, 20, 46]:
                        gram['Citi'].remove('Refleksīvs')

                    if len(gram['Citi']) == 1:
                        gram['Citi'] = gram['Citi'][0]
                    if len(gram['Citi']) == 0:
                        del gram['Citi']

                if gram and gram.get('Lietojuma ierobežojumi'):
                    if isinstance(gram.get('Lietojuma ierobežojumi'), str):
                        gram['Lietojuma ierobežojumi'] = [gram.get('Lietojuma ierobežojumi')]
                    for i in gram['Lietojuma ierobežojumi']:
                        if i in ['Sarunvaloda', 'Vēsturisks', 'Novecojis', 'Nevēlams', 'Žargonvārds', 'Apvidvārds', 'Neaktuāls', 'Īsziņās', 'Neliterārs', 'Vulgārisms', 'Barbarisms', 'Bērnu valoda', 'Biblisms', 'Slengs']:
                            gram['Lietojums'] = i
                        elif i in ['Poētiska stilistiskā nokrāsa', 'Vienkāršrunas stilistiskā nokrāsa', 'Nievājoša ekspresīvā nokrāsa', 'Sirsnīga emocionālā nokrāsa', 'Ironiska ekspresīvā nokrāsa', 'Folkloras valodai raksturīga stilistiskā nokrāsa', 'Humoristiska ekspresīvā nokrāsa', 'Pārnestā nozīmē']:
                            gram['Stils'] = i
                        else:
                            print(f"Nesaprasti lietojuma ierobežojumi: '{i}'")
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
                    print(f'Interesting data: {dati} / {row.data}')
                lexeme['attributes'] = gram

                for attribute in gram:
                    attribute_stats[attribute] += 1
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


def dump_attribute_stats(filename):
    global attribute_stats
    with open(filename, 'w') as f:
        for attribute, count in attribute_stats.items():
            print(attribute, count)
            f.write(f'{attribute}\t{count}\n')


db_connect()

filename = 'tezaurs_lexemes.json'
dump_lexemes(filename)
dump_attribute_stats('attributes.txt')

print(f'Done! Output written to {filename}')
