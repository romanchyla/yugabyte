#!/usr/bin/env python

import os
import sys
import adsputils
import argparse
import warnings
import json
import time
from ybload import tasks
from adsputils import setup_logging, get_date, load_config
from ybload.models import KeyValue, Records,BigTable
from sqlalchemy.orm import load_only
from sqlalchemy.orm.attributes import InstrumentedAttribute
import psycopg2

# ============================= INITIALIZATION ==================================== #
proj_home = os.path.realpath(os.path.dirname(__file__))
config = load_config(proj_home=proj_home)

logger = setup_logging('run.py', proj_home=proj_home,
                       level=config.get('LOGGING_LEVEL', 'INFO'),
                       attach_stdout=config.get('LOG_STDOUT', False))

app = tasks.app

# =============================== FUNCTIONS ======================================= #


def _print_record(bibcode):
    with app.session_scope() as session:
        print('stored by us:', bibcode)
        r = session.query(Records).filter_by(bibcode=bibcode).first()
        if r:
            print(json.dumps(r.toJSON(), indent=2, default=str, sort_keys=True))
        else:
            print('None')
        print('-' * 80)

        

def diagnostics(bibcodes):
    """
    Show information about what we have in our storage.

    :param: bibcodes - list of bibcodes
    """

    if not bibcodes:
        print('Printing 3 randomly selected records (if any)')
        bibcodes = []
        with app.session_scope() as session:
            for r in session.query(Records).limit(3).all():
                bibcodes.append(r.bibcode)

    for b in bibcodes:
        _print_record(b)

    with app.session_scope() as session:
        for x in dir(Records):
            if isinstance(getattr(Records, x), InstrumentedAttribute):
                print('# of %s' % x, session.query(Records).filter(getattr(Records, x) != None).count())


def print_kvs():
    """Prints the values stored in the KeyValue table."""
    print('Key, Value from the storage:')
    print('-' * 80)
    with app.session_scope() as session:
        for kv in session.query(KeyValue).order_by('key').yield_per(100):
            print(kv.key, kv.value)


def ingest_binary_files(location, report=100):
    """Will receive a list full of file locations; will read it, open each
    and insert the binary data into the database (as blob).
    
    We are going to use direct db cursor and no ORM (io order to abuse the
    db as much as possible)
    """

    connection = cursor = None

    sql = "INSERT INTO {} VALUES(%s, %s) ON CONFLICT DO NOTHING".format(BigTable.__tablename__)

    i = 0
    with open(location, 'r') as fi:
        for line in fi:
            if cursor is None:
                connection = app._engine.raw_connection()
                #connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = connection.cursor()
                
                

            l = line.strip().split(maxsplit=1)
            if len(l) == 1:
                l.append(l[0])
            
            if os.path.exists(l[1]):
                with open(l[1], 'rb') as input:
                    # Perform the insertions
                    cursor.execute(sql, (l[0], psycopg2.Binary(input.read())))
                    i += 1
                    if i % report == 0:
                        connection.commit()
                        cursor.close()
                        connection.close()
                        cursor = connection = None

                        app.logger.info('Read and inserted %s binary files so far', i)
        else:
            if connection:
                connection.commit()
                cursor.close()
                connection.close()

    app.logger.info('Done inserting %s binary files', i)

    return i


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process user input.')

    parser.add_argument('-d',
                        '--diagnostics',
                        dest='diagnostics',
                        action='store_true',
                        help='Show diagnostic message')

    parser.add_argument('-b',
                        '--bibcodes',
                        dest='bibcodes',
                        action='store',
                        help='List of bibcodes separated by spaces')
    parser.add_argument('-k',
                        '--kv',
                        dest='kv',
                        action='store_true',
                        default=False,
                        help='Show current values of KV store')
    parser.add_argument('-e',
                        '--batch_size',
                        dest='batch_size',
                        action='store',
                        default=100,
                        type=int,
                        help='How many records to process/index in one batch')
    parser.add_argument('-i',
                        '--ingest_keyvalue',
                        dest='ingest_keyvalue',
                        action='store',
                        help='File containing key\tlocation; location will be read in as binary and inserted into bigtable')


    args = parser.parse_args()

    logger.info('Executing run.py: %s', args)

    if args.bibcodes:
        args.bibcodes = args.bibcodes.split(' ')

    if args.kv:
        print_kvs()

    if args.ingest_keyvalue:
        if os.path.exists(args.ingest_keyvalue):
            print('Starting ingest')
            print('Done ingesting {} objects'.format(ingest_binary_files(args.ingest_keyvalue, args.batch_size)))
        else:
            exit('The {} does not exist'.format(args.ingest_keyvalue))


    if args.diagnostics:
        diagnostics(args.bibcodes)



