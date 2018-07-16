from itertools import takewhile, islice, count
import hashlib, logging, re, os
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
from lib.exceptions import *


def chunk(n, it):
    src = iter(it)
    return takewhile(bool, (list(islice(src, n)) for _ in count(0)))

def md5(s):
    h = hashlib.new('md5')
    h.update(s.encode('utf-8'))
    return h.hexdigest()

def get_mongo_client(test = False):
    host = os.getenv('MONGO_HOST_TEST') if test else os.getenv('MONGO_HOST')
    client = MongoClient(host,
                     username = os.getenv('MONGO_USER'),
                     password = os.getenv('MONGO_PASSWORD'))
    return client

not_d = re.compile(r'[^\d]+')
start = re.compile(r'^[^\d]')

def get_service_date(entry):
    report_date = entry['Report_Date']
    fallback = report_date.replace(hour=0, minute=0, second=0)
    date = entry['Service_Date']
    date = re.sub(not_d, '.', date)
    date = re.sub(start, '', date)
    try:
        date = datetime.strptime(date, '%d.%m.%Y')
        if date > report_date:
            raise MalformedDateException('Service date in future: {}'.format(date))
        if report_date - date > timedelta(weeks = 8):
            raise MalformedDateException('Service date too far in the past: {}'.format(date))
    except MalformedDateException as e:
        logging.debug(e)
        date = fallback
    except Exception as e:
        logging.error(e)
        date = fallback
    return date

def make_id(m):
    return md5('{}{}{}{}'.format(m['ogServiceDate'],
                                  m['patientName'],
                                  m['patientPhone'],
                                  m['code']))

def convert_entry(og):
    """ Changes keys and formats for ID creation """
    e = {
        'serviceDate': get_service_date(og),
        'ogServiceDate': og['Service_Date'],
        'patientPhone': og['Patient_Phone_Number'],
        'patientName': og['Patient_Name'].upper(),
        'senderPhone': og['Sender_Phone_Number'],
        'timestamp': og['Report_Date'],
        'code': og['Service_Code'].upper()
    }
    e['_id'] = make_id(e)
    return e

def get_messages_df(collection):
    df = pd.DataFrame(list((convert_entry(e) for e in collection.find({}))))
    return df

def get_events(collection):
    return collection.find({})

def get_roster(path):
    df = pd.read_excel(path)
    df['reporting_number'] = df.reporting_number.astype(str)
    df['training_date'] = df.training_date.map(lambda d: datetime.strptime(d, '%d.%m.%y'))
    return df

def get_crosswalk(path):
    crosswalk = pd.read_excel(path)
    crosswalk['old_number'] = crosswalk.z08_2.astype(str)
    crosswalk['new_payment_number'] = crosswalk.new_payment_number.astype(str)
    return crosswalk
