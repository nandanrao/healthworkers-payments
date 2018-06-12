from datetime import datetime, timedelta
import pytest
from pymongo import MongoClient
from tag_training import tag_training, tag_training_df
import pandas as pd

time = datetime(2018, 5, 1)
time2 = datetime(2018, 7, 1)

records = [
    { '_id': 'a', 'workerPhone': 'bar', 'serviceDate': time + timedelta(days=1, hours=1)},
    { '_id': 'b', 'workerPhone': 'bar', 'serviceDate': time + timedelta(hours=1) },
    { '_id': 'c', 'workerPhone': 'bar', 'serviceDate': time + timedelta(hours=23) },
    { '_id': 'd', 'workerPhone': 'foo', 'serviceDate': time2 + timedelta(hours=10) },
    { '_id': 'e', 'workerPhone': 'baz', 'serviceDate': time + timedelta(days=45) },
    { '_id': 'f', 'workerPhone': 'qux', 'serviceDate': time  }
]

testers = [
    { 'training_date': None, 'reporting_number': 'qux' },
    { 'training_date': None, 'reporting_number': 'qux2' }
]

@pytest.fixture(scope="module")
def collection():
    client = MongoClient()
    db = client['hw-test']
    collection = db.messages
    collection.insert_many(records)
    yield collection
    collection.drop()
    client.close()

def test_tag_training(collection):
    tag_training([('bar', time), ('foo', time2), ('baz', time)], collection)
    res = list(collection.find({'workerPhone': 'bar', 'training': True}))
    assert(len(res) == 2)
    res = list(collection.find({'workerPhone': 'foo', 'training': True}))
    assert(len(res) == 1)
    res = list(collection.find({'workerPhone': 'baz', 'training': True}))
    assert(len(res) == 0)

def test_tag_training_df():
    messages = pd.DataFrame(records)
    t = pd.DataFrame(testers)
    trainings = [('bar', time), ('foo', time2), ('baz', time)]
    with_training = tag_training_df(trainings, messages, t)
    trained = with_training[with_training.training == True]

    assert(with_training.shape[0] == messages.shape[0])
    assert(trained.shape[0] == 4)
    assert(trained[trained.workerPhone == 'bar'].shape[0] == 2)
