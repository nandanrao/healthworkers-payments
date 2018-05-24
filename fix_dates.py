from pymongo import MongoClient, UpdateOne
from itertools import islice, takewhile, count
from datetime import datetime
import logging
import re

logging.basicConfig(level = logging.DEBUG)

def update_one(entry):
    try:
        service_date = datetime.strptime(entry['serviceDate'], '%d.%m.%Y')
    except:
        service_date = entry['serviceDate']
    return UpdateOne({ '_id': entry['_id']}, { '$set': { 'serviceDate' : service_date}})

def get_service_date(entry):
    report_date = entry['Report_Date']
    date = entry['Service_Date']
    date = re.sub(r'[^\d]+', '.', date)
    date = re.sub(r'^[^\d]', '', date)
    try:
        date = datetime.strptime(date, '%d.%m.%Y')
        if date > report_date:
            raise FutureException('Service date in future: {}'.format(date))
    except Exception as e:
        logging.error(e)
        date = report_date
    return date

def update_year(entry):
    og = entry['originalEntry']
    service_date = get_service_date(og)
    return UpdateOne({ '_id': entry['_id']}, { '$set': { 'serviceDate' : service_date}})

def chunk(n, it):
    src = iter(it)
    return takewhile(bool, (list(islice(src, n)) for _ in count(0)))

# if __name__ == '__main__':
    # host = "mongodb+srv://bubbles-t7bij.mongodb.net"
host = os.getenv('MONGO_HOST') or None
client = MongoClient(host,
                     username = os.getenv('MONGO_USER'),
                     password = os.getenv('MONGO_PASSWORD'))
coll = client['healthworkers'].messages
chunks = chunk(100, coll.find())
for c in chunks:
    reqs = [update_year(e) for e in c]
    coll.bulk_write(reqs, ordered=False)
