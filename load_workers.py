import pandas as pd
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv
from payments import get_numbers, get_crosswalk, get_mongo_client, chunk
import os

def get_testers(collection, numbers, crosswalk):
    nums = numbers.reporting_number.tolist()
    nums += crosswalk.new_payment_number.tolist()
    res = collection.find({ 'workerPhone' : {'$nin': nums }})
    phones = (r['workerPhone'] for r in res)
    tester_phones = list(set([p for p in phones]))

    df = pd.DataFrame([
        {'reporting_number': t,
         'contact_number': t,
         'name': 'Tester McTesterson',
         'chw_district': 'Test',
         'training_date': None} for t in tester_phones])
    return df

def reformat_record(r):
    r['reporting_number'] = '{}'.format(r['reporting_number'])
    r['contact_number'] = '{}'.format(r['contact_number'])
    return r

if __name__ == '__main__':
    load_dotenv()
    # client = get_mongo_client()
    client = get_mongo_client(test=True)

    collection = client['healthworkers'].messages

    crosswalk = get_crosswalk('number-changes/number_changes.xlsx')
    df = get_numbers('rosters/chw.xlsx')
    df_testers = get_testers(collection, df, crosswalk)
    df_all = pd.concat([df, df_testers], sort=False)

    workers_coll = client['healthworkers'].workers
    records = df_all.to_dict(orient='records')


    prepared = (reformat_record(r) for r in records)
    chunked = chunk(100, prepared)

    for c in chunked:
        requests = [ UpdateOne({ 'reporting_number': obj['reporting_number']},
                           { '$setOnInsert': obj }
                           , upsert=True) for obj in c]
        workers_coll.bulk_write(requests, ordered=False)
