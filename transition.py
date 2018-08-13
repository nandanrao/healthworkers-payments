from lib.utils import get_mongo_client, convert_entry, bulk_upsert
from pymongo import UpdateOne
from datetime import datetime
from lib.logger import logging
import os

DEFAULT_DATE = datetime(1970,1,1)
EVENTS = ['called', 'attempted', 'noConsent']

def called(rec):
    c = convert_entry(rec['originalEntry'])
    return {
        'event': 'called',
        'event_time': DEFAULT_DATE,
        'record': c,
        'updates': {
            'code': rec['code']
        }
    }

def attempted(rec):
    c = convert_entry(rec['originalEntry'])
    timestamps = rec['attempts']
    events = [{ 'event': 'attempted', 'event_time': t, 'record': c }
              for t in timestamps]
    return events

def noconsent(rec):
    c = convert_entry(rec['originalEntry'])
    return {
        'event': 'noConsent',
        'event_time': DEFAULT_DATE,
        'record': c
    }

def get_events(coll, event):
    if event == 'called':
        res = coll.find({'called':True})
        return (called(rec) for rec in res)
    elif event == 'attempted':
        res = coll.find({ 'attempts': { '$exists': True, '$ne': [] }})
        return (a for rec in res for a in attempted(rec))
    elif event == 'noConsent':
        res = coll.find({ 'noConsent': True })
        return (noconsent(rec) for rec in res)
    else:
        raise Exception('WHAT THE FUCK. THIS EVENT DOESNT EXIST.')

def write_events():
    client = get_mongo_client()
    coll = client['healthworkers'].messages
    event_coll = client['healthworkers'].events
    for event in EVENTS:
        events = get_events(coll, event)
        bulk_upsert(event_coll, events, 1000, ['event_time', 'event', 'record._id'])

if __name__ == '__main__':
    logging.info('Writing Events from host: {}'.format(os.getenv('MONGO_HOST', 'localhost:27017')))
    write_events()
