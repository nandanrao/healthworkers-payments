from payments import get_mongo_client, get_numbers
from pymongo import UpdateOne
from datetime import timedelta
from itertools import takewhile, islice, count
from dotenv import load_dotenv
import pandas as pd
# client = get_mongo_client()
# coll = client['healthworkers'].messages

def working_from(date):
    return date + timedelta(days=1)

def get_updates(num, date, coll):
    in_action = working_from(date)
    msgs = list(coll.find({ 'workerPhone': num }, { 'serviceDate': 1 }))
    trainings = [m['_id'] for m in msgs if m['serviceDate'] <= in_action ]
    updates = [UpdateOne({ '_id': i}, { '$set': {'training': True}}) for i in trainings]
    return updates

def chunk(n, it):
    src = iter(it)
    return takewhile(bool, (list(islice(src, n)) for _ in count(0)))

def tag_training(training_dates, coll):
    updates = (get_updates(num, date, coll) for num, date in training_dates)
    updates = (i for u in updates for i in u)
    chunked = chunk(100, updates)
    for c in chunked:
        coll.bulk_write(c, ordered=False)
    return None

def tag_training_df(numbers, messages, testers):

    # for each number in numbers, find messages before date and tag as training
    for number, date in numbers:
        in_action = working_from(date)
        idx = (messages.workerPhone == number) & (messages.serviceDate <= in_action)
        messages.loc[idx, 'training'] = True

    # for each number in testers, find all messages and tag as training
    merged = messages.merge(testers,
                            how = 'left',
                            left_on='workerPhone',
                            right_on='reporting_number')
    testers_idx = merged.reporting_number.notna()
    messages.loc[testers_idx, 'training'] = True
    return messages

if __name__ == '__main__':
    load_dotenv()
    numbers = get_numbers('rosters/chw.xlsx')
    training_dates = [(n['reporting_number'], n['training_date'])
         for n in numbers.to_dict(orient='records')]
    client = get_mongo_client()
    collection = client['healthworkers'].messages
    tag_training(training_dates, collection)

# Add function for tagging training messages


# n = numbers.reporting_number[0]
# list(coll.find({ 'workerPhone': n }))
