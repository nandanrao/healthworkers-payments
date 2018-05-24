from payments import *
import pandas as pd
import pytest

records = [
    { 'name': 'mark', 'workerPhone': 'bar', 'serviceDate': datetime(2018,1,1)},
    { 'name': 'mark', 'workerPhone': 'bar', 'serviceDate': datetime(2018,2,1)},
    { 'name': 'mark', 'workerPhone': 'bar', 'serviceDate': datetime(2018,4,1)},
    { 'name': 'mark', 'workerPhone': 'bar', 'serviceDate': datetime(2018,6,1)},
    { 'name': 'mark', 'workerPhone': 'foo', 'serviceDate': datetime(2018,5,1)},
    { 'name': 'mark', 'workerPhone': 'foo', 'serviceDate': datetime(2018,5,1)},
    { 'name': 'clue', 'workerPhone': 'baz', 'serviceDate': datetime(2018,5,1)},
    { 'text': 'baz'}
]

@pytest.fixture(scope="module")
def df():
    df = pd.DataFrame([
        { 'phone_ps': 'a', 'reporting_number': 'foo', 'treat': 0},
        { 'phone_ps': 'a', 'reporting_number': 'baz', 'treat': 0},
        { 'phone_ps': 'b', 'reporting_number': 'baz', 'treat': 0},
    ])
    yield df

@pytest.fixture(scope="module")
def collection():
    client = MongoClient()
    db = client['hw-test']
    collection = db.messages
    collection.insert_many(records)
    yield collection
    collection.drop()
    client.close()


def test_monther_makes_range():
    d = datetime(2018,3,5)
    now = datetime(2018,5,10)
    m = monther(d, now)
    assert(len(m) == 2)
    assert(m[0][0] == d + timedelta(days = 1))
    assert(m[0][1] == datetime(2018, 4, 6))
    assert(m[1][0] == datetime(2018, 4, 6))
    assert(m[1][1] == datetime(2018, 5, 6))

def test_monther_handles_all_dates():
    now = datetime(2018,5,10)
    d = datetime(2017,1,10)
    assert(len(monther(d, now)) == 3)
    d = datetime(2018,1,9)
    assert(len(monther(d, now)) == 4)
    d = datetime(2017,12,10)
    assert(len(monther(d, now)) == 4)


def test_monther_works_with_now():
    now = datetime(2018,5,10)
    assert(len(monther(now, now)) == 0)

def test_count_reports(collection):
    start, end = datetime(2018,3,5), datetime(2018,5,6)
    reports = count_reports(collection, start, end, pd.Series(['foo', 'bar']))
    assert(reports.iloc[0].reporting_number == 'foo')
    assert(reports.iloc[0].reports == 2)
    assert(reports.iloc[1].reporting_number == 'bar')
    assert(reports.iloc[1].reports == 1)
    assert(reports.shape == (2,3))

def test_count_reports_with_no_reports(collection):
    start, end = datetime(2018,6,5), datetime(2018,7,5)
    reports = count_reports(collection, start, end, ['foo', 'bar'])
    assert(reports.shape == (0,0))

def test_count_reports_with_no_nums(collection):
    start, end = datetime(2018,3,5), datetime(2018,5,5)
    reports = count_reports(collection, start, end, pd.Series([]))
    assert(reports.shape == (0,0))

def test_translate_numbers():
    df = pd.DataFrame([
        { 'reporting_number': 'foo', 'payment_due':datetime(2018,5,1), 'reports': 2 },
        { 'reporting_number': 'bar', 'payment_due': datetime(2018,5,2), 'reports': 1 },
        { 'reporting_number': 'baz', 'payment_due': datetime(2018,5,1), 'reports': 1 }])
    crosswalk = pd.DataFrame([ {'old_number': 'foo', 'new_payment_number': 'baz'}])
    translated = translate_numbers(df, crosswalk)
    assert(translated.reporting_number.tolist() == ['baz', 'bar', 'baz'])
    assert(translated.shape == (3,3))


def test_aggs_reports():
    df = pd.DataFrame([
        { 'reporting_number': 'foo', 'payment_due':datetime(2018,5,1), 'reports': 2 },
        { 'reporting_number': 'bar', 'payment_due': datetime(2018,5,2), 'reports': 1 },
        { 'reporting_number': 'foo', 'payment_due': datetime(2018,5,1), 'reports': 1 }])
    agged = agg_reports(df)
    print(agged)
    assert(agged.shape == (2,3))
    assert(agged.reports.tolist() == [1, 3])
    assert(agged.payment_due.tolist() == [datetime(2018,5,2), datetime(2018,5,1)])
    assert(agged.reporting_number.tolist() == ['bar', 'foo'])
