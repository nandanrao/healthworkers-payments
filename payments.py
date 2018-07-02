import pandas as pd
from pymongo import MongoClient, InsertOne
import numpy as np
import os, re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta as rd
from functools import reduce
from itertools import islice, takewhile, count
import boto3
import io

import logging

def chunk(n, it):
    src = iter(it)
    return takewhile(bool, (list(islice(src, n)) for _ in count(0)))

def get_mongo_client(test = False):
    host = os.getenv('MONGO_HOST_TEST') if test else os.getenv('MONGO_HOST')
    client = MongoClient(host,
                     username = os.getenv('MONGO_USER'),
                     password = os.getenv('MONGO_PASSWORD'))
    return client

class MalformedDateException(Exception):
    pass

# COPIED FROM RETRIEVER --> TODO: COMBINE!
not_d = re.compile(r'[^\d]+')
start = re.compile(r'^[^\d]')

def get_service_date(entry):
    report_date = entry['Report_Date']
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
        date = report_date
    except Exception as e:
        logging.error(e)
        date = report_date
    return date

def get_attempts(a, idx):
    try:
        return a[idx]
    except:
        return None

def convert_entry(d):
    og = d['originalEntry']
    service_date = get_service_date(og)
    return {
        'attempts': d.get('attempts'),
        'code': d.get('code'), # og??
        'serviceDate': service_date,
        'noConsent': d.get('noConsent'),
        'timestamp': og['Report_Date'],
        'workerPhone': og['Sender_Phone_Number'],
        'patientPhone': og['Patient_Phone_Number'],
        'patientName': og['Patient_Name'],
    }


def get_og_messages(collection):
    df = pd.DataFrame(list((convert_entry(e) for e in collection.find({}))))
    return df

def monther(date, now = datetime.utcnow()):
    date = date + timedelta(days = 1)
    months = rd(now, date).months
    return [(date + rd(months = m), date + rd(months = m+1))
            for m in range(months)]

def count_reports(collection, start, end, nums):
    try:
        nums = nums.tolist()
    except:
        pass

    cursor = collection.aggregate([
        { '$match': { 'workerPhone': { '$in': nums }, 'serviceDate': { '$gte': start, '$lt': end }}},
        { '$group': { '_id': {'workerPhone': '$workerPhone'}, 'count': { '$sum': 1 }}}
    ])
    dicts = ({'reporting_number': c['_id']['workerPhone'],
              'reports': c['count'],
              'payment_due': end}
             for c in cursor)
    li = list(dicts)
    if len(li) > 0:
        df = pd.DataFrame(li)
        return df.assign(reports = df.reports.map(lambda r: min(30, r)))
    else:
        return pd.DataFrame([])


def bonus_worker(treat, reports, **kwargs):
    if treat == 2:
        return reports*2000
    if treat == 3:
        return reports*1000
    return 0

def bonus_super(treat, reports, **kwargs):
    if treat == 1:
        return reports*2000
    if treat == 3:
        return reports*1000
    return 0

def base(df):
    df = df[df.reports > 0] # should be unnecesary
    return df.assign(base = 10000)

def bonus(df, fn):
    bonuses = [fn(**d) for d in df.to_dict(orient='records')]
    return df.assign(bonus = bonuses)

def pay_supers(df):
    return (df
            .groupby(['phone_ps'])
            .agg({ 'reports': np.sum, 'treat': 'first'})
            .reset_index()
            .pipe(base)
            .pipe(bonus, fn = bonus_super)
            .pipe(lambda df: df.assign(payment = df.bonus + df.base))
            .rename( columns = {'phone_ps': 'number'})
            [['number', 'payment']])

def calc_payment_per_worker(df):
    return (df
            .pipe(base)
            .pipe(bonus, fn = bonus_worker)
            .pipe(lambda df: df.assign(payment = df.bonus + df.base)))

def pay_workers(df):
    return (calc_payment_per_worker(df)
            .rename( columns = {'reporting_number': 'number'})
            [['number', 'payment']])

def translate_numbers(df, crosswalk, key = 'reporting_number'):
    d = df.merge(crosswalk, how = 'left', left_on = key, right_on= 'old_number')
    idx = d.new_payment_number.notna()
    d.loc[idx, key] = d[idx].new_payment_number
    return d.drop(crosswalk.columns, 1)

def agg_reports(df):
    others = df.groupby('reporting_number').agg('first').drop('reports', axis=1)
    reports = df.groupby('reporting_number').agg({ 'reports': np.sum }).reports
    return others.assign(reports = reports).reset_index()

def get_count_df(coll, df, crosswalk):
    df = translate_numbers(df, crosswalk)
    groups = [g for g in df.groupby('training_date')]
    reports = [[count_reports(coll, start, end, g[1].reporting_number) for start,end in monther(g[0])] for g in groups]
    reports = [i for r in reports for i in r if not i.empty]
    translated = [r.pipe(agg_reports)
                  for r in reports]
    return pd.concat(translated).merge(df, on = 'reporting_number', how = 'left')


def calc_payments(coll, df, crosswalk, fn):
    return (get_count_df(coll, df, crosswalk)
            .groupby('payment_due')
            .apply(fn)
            .reset_index()
            .drop('level_1', axis=1))

def get_numbers(path):
    df = pd.read_excel(path)
    df['reporting_number'] = df.reporting_number.astype(str)
    df['training_date'] = df.training_date.map(lambda d: datetime.strptime(d, '%d.%m.%y'))
    return df

def get_crosswalk(path):
    crosswalk = pd.read_excel(path)
    crosswalk['old_number'] = crosswalk.z08_2.astype(str)
    crosswalk['new_payment_number'] = crosswalk.new_payment_number.astype(str)
    return crosswalk

def calcs():
    df = get_numbers('rosters/chw.xlsx')
    crosswalk = get_crosswalk('number-changes/number_changes.xlsx')

    client = get_mongo_client()
    old_coll = client['healthworkers'].messages
    temp_coll = client['healthworkers'].temp
    create_clean_collection(old_coll, temp_coll, crosswalk)

    workers = calc_payments(temp_coll, df, crosswalk, pay_workers)
    supers = calc_payments(temp_coll, df, crosswalk, pay_supers)
    temp_coll.drop()
    return workers, supers

def write_to_s3(df, key):
    out = io.StringIO()
    df.to_csv(out, index=False)

    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='healthworkers-payments',
        Key="{1}-{0:%Y-%m-%d_%H:%M}.csv".format(datetime.now(), key),
        Body=out.getvalue()
    )

def fix_time(d):
    return datetime.combine(datetime.date(d), datetime.min.time())

def create_clean_collection(old_coll, temp_coll, crosswalk):
    messages = get_og_messages(old_coll)

    messages = (messages
                .assign(serviceDate = messages.serviceDate.map(fix_time))
                .assign(patientName = messages.patientName.str.upper())
                .assign(code = messages.code.str.upper())
                .groupby(['workerPhone', 'patientName', 'code', 'patientPhone', 'serviceDate'])
                .apply(lambda df: df.head(1)))

    dicts = (messages
             .pipe(translate_numbers, crosswalk = crosswalk, key = 'workerPhone')
             .to_dict(orient = 'records'))

    i = 0
    chunked = chunk(400, dicts)
    for c in chunked:
        requests = [ InsertOne(obj) for obj in c]
        i += len(requests)
        temp_coll.bulk_write(requests, ordered=False)
    logging.info('WROTE {} MESSAGES TO NEW COLLECTION'.format(i))
    return temp_coll

if __name__ == '__main__':
    workers, supers = calcs()
    write_to_s3(workers, 'workers')
    write_to_s3(supers, 'supers')
