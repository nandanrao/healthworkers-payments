import pandas as pd
from pymongo import MongoClient
import numpy as np
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta as rd
from functools import reduce
import boto3
import io

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
    return pd.DataFrame(list(dicts))

def bonus_worker(treat, reports, **kwargs):
    if treat == 2:
        return min(reports*2000, 60000)
    if treat == 3:
        return min(reports*1000, 30000)
    return 0

def bonus_super(treat, reports, **kwargs):
    if treat == 1:
        return min(reports*2000, 60000)
    if treat == 3:
        return min(reports*1000, 30000)
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

def pay_workers(df):
    return (df
            .pipe(base)
            .pipe(bonus, fn = bonus_worker)
            .pipe(lambda df: df.assign(payment = df.bonus + df.base))
            .rename( columns = {'reporting_number': 'number'})
            [['number', 'payment']])



def translate_numbers(df, crosswalk):
    d = df.merge(crosswalk, how = 'left', left_on = 'reporting_number', right_on= 'old_number')
    idx = d.new_payment_number.notna()
    d.loc[idx, 'reporting_number'] = d[idx].new_payment_number
    return d[['reporting_number', 'reports', 'payment_due']]

def agg_reports(df):
    others = df.groupby('reporting_number').agg('first').drop('reports', axis=1)
    reports = df.groupby('reporting_number').agg({ 'reports': np.sum }).reports
    return others.assign(reports = reports).reset_index()

def get_count_df(coll, df, crosswalk):
    groups = [g for g in df.groupby('training_date')]
    reports = [[count_reports(coll, start, end, g[1].reporting_number) for start,end in monther(g[0])] for g in groups]
    reports = [i for r in reports for i in r]
    translated = [r.pipe(translate_numbers, crosswalk = crosswalk).pipe(agg_reports)
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
    return crosswalk


def calcs():
    client = MongoClient(os.getenv('MONGO_HOST') or None,
                     username = os.getenv('MONGO_USER'),
                     password = os.getenv('MONGO_PASSWORD'))
    coll = client['healthworkers'].messages
    df = get_numbers('rosters/chw.xlsx')
    crosswalk = get_crosswalk('number-changes/number_changes.xlsx')
    workers = calc_payments(coll, df, crosswalk, pay_workers)
    supers = calc_payments(coll, df, crosswalk, pay_supers)
    return workers, supers

def write_to_s3(df, key):
    out = io.StringIO()
    df.to_csv(out, index=False)

    s3 = boto3.client('s3')
    s3.put_object(
        Bucket='healthworkers-payments',
        Key="{1}-{0:%d-%m-%y}.csv".format(datetime.now(), key),
        Body=out.getvalue()
    )

if __name__ == '__main__':
    workers, supers = calcs()
    write_to_s3(workers, 'workers')
    write_to_s3(supers, 'supers')
