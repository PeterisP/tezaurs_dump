#!/usr/bin/env python3
from db_config import db_connection_info

import psycopg2
from psycopg2.extras import NamedTupleCursor
import json
from collections import Counter
from copy import deepcopy

connection = None
attribute_stats = Counter()

paradigms_with_multiple_stems = set([15, 18, 50])

debuglist = set(['kā'])

def db_connect():
    global connection

    if db_connection_info is None or db_connection_info["host"] is None or len(db_connection_info["host"]) == 0:
        print("Postgres connection error: connection information must be supplied in db_config")
        raise Exception("Postgres connection error: connection information must be supplied in <conn_info>")

    print('Connecting to database %s' % (db_connection_info['dbname'],))
    schema = db_connection_info['schema'] or 'tezaurs'
    connection = psycopg2.connect(
            host=db_connection_info['host'],
            port=db_connection_info['port'],
            dbname=db_connection_info['dbname'],
            user=db_connection_info['user'],
            password=db_connection_info['password'],
            options=f'-c search_path={schema}',
        )


def query(sql, parameters):
    global connection
    cursor = connection.cursor(cursor_factory=NamedTupleCursor)
    cursor.execute(sql, parameters)
    r = cursor.fetchall()
    cursor.close()
    return r


def decode_sr(oldgram, sr, paradigm_id):
    if sr.get('AND'):
        saprasts = True
        newgram = dict()
        for sub_sr in sr.get('AND'):
            saprasts1, newgram1 = decode_sr(oldgram, sub_sr , paradigm_id)
            saprasts = saprasts and saprasts1
            newgram = newgram | newgram1
        return saprasts, newgram

    v = sr.get('Value')
    gram = dict()
    saprasts = False
    if sr.get('Restriction') == 'Formā/atvasinājumā' and v.get('Flags') and v.get('Flags').get('Noliegums') == 'Jā' and (sr.get('Frequency') == 'Tikai' or not sr.get('Frequency')):
        gram['Noliegums'] = 'Jā'
        saprasts = True

    if sr.get('Restriction') == 'Formā/atvasinājumā' and v.get('Flags') and v.get('Flags').get('Izteiksme') and 'Divdabis' in v.get('Flags').get('Izteiksme') and v.get('Flags').get('Divdabja veids'):
        veids = v.get('Flags').get('Divdabja veids')
        if veids == 'Lokāmais ciešamās kārtas tagadnes divdabis (-ams, -ama, -āms, -āma)':
            gram['Izteiksme'] = 'Divdabis'
            gram['Lokāmība'] = 'Lokāms'
            gram['Kārta'] = 'Ciešamā'
            gram['Laiks'] = 'Tagadne'                    
            saprasts = True
        if veids == 'Lokāmais ciešamās kārtas pagātnes divdabis (-ts, -ta)':
            gram['Izteiksme'] = 'Divdabis'
            gram['Lokāmība'] = 'Lokāms'
            gram['Kārta'] = 'Ciešamā'
            gram['Laiks'] = 'Pagātne'  
            saprasts = True
        if veids == 'Lokāmais darāmās kārtas tagadnes divdabis (-ošs, -oša)':
            gram['Izteiksme'] = 'Divdabis'
            gram['Lokāmība'] = 'Lokāms'
            gram['Kārta'] = 'Darāmā'
            gram['Laiks'] = 'Tagadne'  
            saprasts = True
        if veids == 'Lokāmais darāmās kārtas pagātnes divdabis (-is, -usi, -ies, -usies)':
            gram['Izteiksme'] = 'Divdabis'
            gram['Lokāmība'] = 'Lokāms'
            gram['Kārta'] = 'Darāmā'
            gram['Laiks'] = 'Pagātne'  
            saprasts = True

    if not saprasts and sr.get('Restriction') == 'Formā/atvasinājumā' and (not sr.get('Frequency') or sr.get('Frequency') == 'Tikai'):
        if v.get('Flags') and v.get('Flags').get('Skaitlis'):
            if 'Daudzskaitlis' in v.get('Flags').get('Skaitlis') and paradigm_id not in [19, 20]:
                gram['Skaitlis 2'] = 'Daudzskaitlinieks'
                saprasts = True
            if 'Vienskaitlis' in v.get('Flags').get('Skaitlis'):
                gram['Skaitlis 2'] = 'Vienskaitlinieks'
                saprasts = True
    if not saprasts and sr.get('Restriction') == 'Formā/atvasinājumā' and (not sr.get('Frequency') or sr.get('Frequency') == 'Parasti'):
        if v.get('Flags') and v.get('Flags').get('Skaitlis'):
            # if 'Daudzskaitlis' in v.get('Flags').get('Skaitlis') and oldgram.get('Leksēmas pamatformas īpatnības') and 'Daudzskaitlis' in oldgram.get('Leksēmas pamatformas īpatnības'):
            #     gram['Skaitlis 2'] = 'Gandrīz daudzskaitlinieks'
            #     saprasts = True
            if 'Daudzskaitlis' in v.get('Flags').get('Skaitlis'):
                saprasts = True
            if 'Vienskaitlis' in v.get('Flags').get('Skaitlis'):
                # Parasti vienskaitlis - bez sekām uz morfoloģiju
                saprasts = True 
        if v.get('Flags') and v.get('Flags').get('Persona') and v.get('Flags').get('Persona') == 'Trešā':
            # Parasti trešā persona - bez sekām uz morfoloģiju
                saprasts = True 
        if v.get('Flags') and v.get('Flags').get('Noteiktība') and v.get('Flags').get('Noteiktība') == 'Noteiktā':
            # Parasti noteiktā forma - bez sekām uz morfoloģiju
                saprasts = True 
    if not saprasts and (sr.get('Restriction') == 'Kopā ar' or sr.get('Restriction') == 'Teikumos / noteikta veida struktūrās'):
        saprasts = True
    if not saprasts and sr.get('Restriction') == 'Vispārīgais lietojuma biežums' and sr.get('Frequency') in ['Reti', 'Pareti', 'Retāk']:
        gram['Lietojuma biežums'] = sr.get('Frequency')
        saprasts = True
    return saprasts, gram


def fetch_lexemes():
    global connection
    global attribute_stats
    cursor = connection.cursor(cursor_factory=NamedTupleCursor)
    sql = """
select l.id as lexeme_id, e.id as entry_id, e.human_key,
  p.legacy_no as paradigm_id, l.data, sense_flags, stem1, stem2, stem3, lemma
from lexemes l
join entries e on l.entry_id = e.id
join paradigms p on l.paradigm_id = p.id
left join (
select entry_id, json_agg(distinct data->'Gram'->'Flags') sense_flags
    from senses
    where data->'Gram'->'Flags' is not null
    group by entry_id) s on (s.entry_id = e.id and l.type_id not in (4,6))
"""
# TODO - filtrs uz e.release_id lai ņemtu svaigāko relīzi nevis visas. Relevants produkcijai
    nesaprastie = 0

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
            if debuglist:
                if row.lemma in debuglist:
                    print(row)
                else:
                    continue

            lexeme = {
                'lexeme_id': row.lexeme_id,
                'entry_id': row.entry_id,   
                'human_id': row.human_key,
                'paradigm': row.paradigm_id,
                'lemma': row.lemma
            }

            # Handling for alternate stems which result in multiple lexemes from single thesaurus lexeme
            altstem2 = None
            altstem3 = None
            if row.stem1:
                stem = row.stem1
                if stem.startswith('{') and stem.endswith('}'):
                    stem = stem[1:-1]  # noņemam {}
                lexeme['stem1'] = stem
            if row.paradigm_id in paradigms_with_multiple_stems and row.stem2:
                if ',' in row.stem2:
                    stem = row.stem2.split(',', maxsplit=1)[0]
                    altstem2 = row.stem2.split(',', maxsplit=1)[1]
                    # print('Dubultcelms %s - sadalīju "%s" un "%s"'%(row.stem2, stem, altstem2))
                else:
                    stem = row.stem2
                    if stem.startswith('{') and stem.endswith('}'):
                        stem = stem[1:-1]  # noņemam {}
                lexeme['stem2'] = stem
            if row.paradigm_id in paradigms_with_multiple_stems and row.stem3:
                if ',' in row.stem3:
                    stem = row.stem3.split(',', maxsplit=1)[0]
                    altstem3 = row.stem3.split(',', maxsplit=1)[1]
                else:
                    stem = row.stem3
                    if stem.startswith('{') and stem.endswith('}'):
                        stem = stem[1:-1]  # noņemam {}
                lexeme['stem3'] = stem

            alt_ssf = False # Multiple options for conjunction syntactic function

            if row.data or row.sense_flags:
                if row.data:
                    dati = deepcopy(row.data)
                else:
                    dati = {}
                for intentionaldiscard in ['ImportNotices', 'Pronunciations']:
                    if dati.get(intentionaldiscard):
                        del dati[intentionaldiscard]

                gram = dati.get('Gram')
                if gram and gram.get('Flags'):
                    flags = dict(gram.get('Flags'))
                    if 'Vārda daļa' in str(flags.get('Kategorija')):
                        continue

                    for key in dict(flags):
                        value = flags[key]
                        if type(value) is list and len(value) == 1:
                            flags[key] = value[0]

                    if type(flags.get('Kategorija')) is not list:
                        if flags.get('Kategorija'):
                            flags['Kategorija'] = [flags.get('Kategorija')]
                        else:
                            flags['Kategorija'] = []

                    # if row.paradigm_id == 36: # FIXME - šos Lauma labošot
                    #     if 'Vārds svešvalodā' in set(flags['Kategorija']) and 'Saīsinājums' in set(flags['Kategorija']):
                    #         print(f'Saīsinājums svešvalodā 36 {row.lemma}')
                    #         flags['Kategorija'].remove('Vārds svešvalodā')
                    #         flags['Piezīmes'] = 'Vārds svešvalodā'

                    # if row.paradigm_id == 39:
                    #     if 'Vārds svešvalodā' in set(flags['Kategorija']) and 'Saīsinājums' in set(flags['Kategorija']):
                    #         lexeme['paradigm'] = 36  # FIXME - šis ir dirty, tas būtu Laumas galā jārisina
                    #         print(f'Saīsinājums svešvalodā 39 {row.lemma}')
                    #         flags['Kategorija'].remove('Vārds svešvalodā')
                    #         flags['Piezīmes'] = 'Vārds svešvalodā'

                    if len(flags['Kategorija']) == 1:
                        flags['Kategorija'] = flags['Kategorija'][0]
                    if len(flags['Kategorija']) == 0:
                        del flags['Kategorija']

                    # for key in dict(flags):
                    #     value = flags[key]
                    #     if key in ['Lieto arī noteiktā formā/atvasinājumā', 'Bieži lieto noteiktā formā/atvasinājumā',
                    #                'Lieto tikai noteiktā formā/atvasinājumā', 'Lieto noteiktā formā/atvasinājumā']:
                    #         print(row.lemma, row.paradigm_id, value, flags)

                    # if flags.get('Skaitlis'):
                    #     del flags['Skaitlis']  # Nav precīza informācija, konfliktē ar analizatora prasībām
                    gram = dict(gram)
                    gram.update(flags)
                    del gram['Flags']

                if gram and gram.get('StructuralRestrictions'):
                    sr = gram.get('StructuralRestrictions')
                    saprasts, newgram = decode_sr(gram, sr, row.paradigm_id)
                    gram |= newgram

                    if saprasts:
                        # FIXME - varbūt daļu saprasto SR ir jāsaglabā kā leksiskā info?
                        del gram['StructuralRestrictions']
                    else:
                        print(f'Nesaprasts SR: {sr} {row.lemma}')
                        nesaprastie += 1

                ssf = set()
                if gram and gram.get('Saikļa sintaktiskā funkcija'):
                    ssf.add(gram.get('Saikļa sintaktiskā funkcija'))
                if row.sense_flags:
                    for sense_flag in row.sense_flags:
                        if sense_flag.get('Saikļa sintaktiskā funkcija'):
                            ssf.add(sense_flag.get('Saikļa sintaktiskā funkcija'))
                if ssf:
                    if not gram:
                        gram = {}
                    ssf = list(ssf)
                    if len(ssf) == 1:
                        ssf = ssf[0]
                    else:
                        alt_ssf = True
                    gram['Saikļa sintaktiskā funkcija'] = ssf

                if dati and (not gram or len(dati) != 1):
                    print(f'Interesting data for "{row.lemma}": {dati} / {row.data}')
                    # Ja izdrukā dažas lemmas kā 'agrākā' utt, tad tas šķiet ok

                if gram:
                    lexeme['attributes'] = gram
                    for attribute in gram:
                        attribute_stats[attribute] += 1

            if alt_ssf:
                lexeme['attributes']['Saikļa sintaktiskā funkcija'] = 'Pakārtojuma'
                yield lexeme
                lexeme['attributes']['Saikļa sintaktiskā funkcija'] = 'Sakārtojuma'

            yield lexeme
            if altstem2 or altstem3:
                if altstem2:
                    lexeme['stem2'] = altstem2
                if altstem3:
                    lexeme['stem3'] = altstem3
                yield lexeme
    print(f'Bija {nesaprastie} nesaprasti StructuralRestrictions')


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

if __name__ == "__main__":
    db_connect()

    filename = 'tezaurs_lexemes.json'
    dump_lexemes(filename)
    dump_attribute_stats('attributes.txt')
    if not debuglist:
        filename = f'/Users/pet/Documents/NLP/morphology/src/main/resources/{filename}'
        dump_lexemes(filename)

    print(f'Done! Output written to {filename}')
