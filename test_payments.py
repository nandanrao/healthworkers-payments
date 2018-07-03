from payments import *
import pandas as pd
import pytest

records = [
    { 'name': 'mark', 'workerPhone': 'bar', 'serviceDate': datetime(2018,1,3)},
    { 'name': 'mark', 'workerPhone': 'bar', 'serviceDate': datetime(2018,2,1)},

    { 'name': 'mark', 'workerPhone': 'bar', 'serviceDate': datetime(2018,4,1)},
    { 'name': 'clue', 'workerPhone': 'baz', 'serviceDate': datetime(2018,4,1)},
    { 'name': 'clue', 'workerPhone': 'brat', 'serviceDate': datetime(2018,4,1)},

    { 'name': 'mark', 'workerPhone': 'bar', 'serviceDate': datetime(2018,5,1)},
    { 'name': 'mark', 'workerPhone': 'foo', 'serviceDate': datetime(2018,5,1)},
    { 'name': 'mark', 'workerPhone': 'foo', 'serviceDate': datetime(2018,5,1)},
    { 'name': 'clue', 'workerPhone': 'baz', 'serviceDate': datetime(2018,5,1)},

    { 'name': 'clue', 'workerPhone': 'qux', 'serviceDate': datetime(2100,12,1)},
    { 'text': 'baz'}
]

records_og = [
    { 'code': 'a', 'originalEntry': { 'Report_Date': datetime(2018,2,5), 'Sender_Phone_Number': 'foo', 'Patient_Phone_Number': '034', 'Patient_Name': 'mark', 'Service_Date': '01.05.2018', 'code': 'a' }},
    { 'code': 'a', 'originalEntry': { 'Report_Date': datetime(2018,2,5), 'Sender_Phone_Number': 'foo', 'Patient_Phone_Number': '034', 'Patient_Name': 'mark', 'Service_Date': '01.05.2018', 'code': 'a' }},
    { 'code': 'a', 'originalEntry': { 'Report_Date': datetime(2018,2,5), 'Sender_Phone_Number': 'bar', 'Patient_Phone_Number': '034', 'Patient_Name': 'mark', 'Service_Date': '01.05.2018', 'code': 'a' }},
    { 'code': 'a', 'originalEntry': { 'Report_Date': datetime(2018,2,5), 'Sender_Phone_Number': 'baz', 'Patient_Phone_Number': '024', 'Patient_Name': 'mark', 'Service_Date': '12.09.2017', 'code': 'a' }}
]

fat_records = [
    { 'name': 'mark-{}'.format(i), 'workerPhone': 'qux', 'serviceDate': datetime(2018,m,3)}
for m in range (3,5) for i in range(1,50)]

@pytest.fixture(scope="module")
def df():
    df = pd.DataFrame([
        { 'phone_ps': 'a', 'reporting_number': 'foo', 'treat': 0, 'training_date': datetime(2018,1,1)},
        { 'phone_ps': 'b', 'reporting_number': 'baz', 'treat': 1, 'training_date': datetime(2018,1,1)},
        { 'phone_ps': 'c', 'reporting_number': 'bar', 'treat': 2, 'training_date': datetime(2018,1,1)},
        { 'phone_ps': 'd', 'reporting_number': 'qux', 'treat': 3, 'training_date': datetime(2018,1,1)},
        { 'phone_ps': 'd', 'reporting_number': 'brat', 'treat': 3, 'training_date': datetime(2018,1,1)},
        { 'phone_ps': 'f', 'reporting_number': 'ham', 'treat': 3, 'training_date': datetime(2018,1,1)},
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


@pytest.fixture(scope="module")
def fat_collection():
    client = MongoClient()
    db = client['hw-test']
    collection = db.messages_fat
    collection.insert_many(records)
    collection.insert_many(fat_records)
    yield collection
    collection.drop()
    client.close()

@pytest.fixture(scope="module")
def colls_for_cleaning():
    client = MongoClient()
    old_coll = client['hw-test'].m2
    new_coll = client['hw-test'].temp
    old_coll.insert_many(records_og)
    yield old_coll, new_coll
    old_coll.drop()
    new_coll.drop()
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
    print(reports)
    assert(reports.iloc[0].reporting_number == 'foo')
    assert(reports.iloc[0].reports == 2)
    assert(reports.iloc[1].reporting_number == 'bar')
    assert(reports.iloc[1].reports == 2)
    assert(reports.shape == (2,3))

def test_count_reports_caps_at_30(fat_collection):
    start, end = datetime(2018,1,1), datetime(2018,8,5)
    reports = count_reports(fat_collection, start, end, ['foo', 'qux'])
    assert(reports.reports.loc[0] == 30)

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


def test_translate_numbers_with_multiple_levels():
    df = pd.DataFrame([
        { 'reporting_number': 'foo', 'payment_due':datetime(2018,5,1), 'reports': 2 },
        { 'reporting_number': 'foo1', 'payment_due':datetime(2018,5,1), 'reports': 2 },
        { 'reporting_number': 'bar', 'payment_due': datetime(2018,5,2), 'reports': 1 },
        { 'reporting_number': 'baz', 'payment_due': datetime(2018,5,1), 'reports': 1 }])

    crosswalk = pd.DataFrame([
        {'old_number': 'foo', 'new_payment_number': 'baz'},
        {'old_number': 'foo1', 'new_payment_number': 'baz'}
    ])
    translated = translate_numbers(df, crosswalk)
    assert(translated.reporting_number.tolist() == ['baz', 'baz', 'bar', 'baz'])
    assert(translated.shape == (4,3))


def test_aggs_reports():
    df = pd.DataFrame([
        { 'reporting_number': 'foo', 'payment_due':datetime(2018,5,1), 'reports': 2 },
        { 'reporting_number': 'bar', 'payment_due': datetime(2018,5,2), 'reports': 1 },
        { 'reporting_number': 'foo', 'payment_due': datetime(2018,5,1), 'reports': 1 }])
    agged = agg_reports(df)
    assert(agged.shape == (2,3))
    assert(agged.reports.tolist() == [1, 3])
    assert(agged.payment_due.tolist() == [datetime(2018,5,2), datetime(2018,5,1)])
    assert(agged.reporting_number.tolist() == ['bar', 'foo'])

# def test_payments(collection, df):
#     crosswalk = pd.DataFrame([{ 'old_number': 'no', 'new_payment_number': 'yes'}])
#     # workers = calc_payments(collection, df, crosswalk, pay_workers)
#     # assert(workers.payment.tolist() == [14000, 12000, 11000, 12000, 10000, 10000])
#     supers = calc_payments(collection, df, crosswalk, pay_supers)
#     assert(supers.payment.tolist() == [10000, 10000, 11000, 10000, 12000, 10000])

def test_payments_gives_base_for_each_month_with_one_message(collection, df):
    crosswalk = pd.DataFrame([{ 'old_number': 'no', 'new_payment_number': 'yes'}])
    workers = calc_payments(collection, df, crosswalk, pay_workers)
    workers[workers.number == 'baz'].payment.tolist() == [10000, 10000]

def test_payments_gives_base_for_each_month_with_one_message(collection, df):
    crosswalk = pd.DataFrame([{ 'old_number': 'no', 'new_payment_number': 'yes'}])
    workers = calc_payments(collection, df, crosswalk, pay_workers)
    assert(workers[workers.number == 'baz'].payment.tolist() == [10000, 10000])

def test_payments_caps_at_30(fat_collection, df):
    crosswalk = pd.DataFrame([{ 'old_number': 'no', 'new_payment_number': 'yes'}])
    workers = calc_payments(fat_collection, df, crosswalk, pay_workers)
    workers = workers[workers.number == 'qux']
    assert(workers.payment.tolist() == [40000, 40000])
    supers = calc_payments(fat_collection, df, crosswalk, pay_supers)
    supers = supers[supers.number == 'd']
    assert(supers.payment.tolist() == [41000, 40000])

def test_create_clean_collection(colls_for_cleaning):
    old_coll, new_coll = colls_for_cleaning
    crosswalk = pd.DataFrame([ {'old_number': 'foo', 'new_payment_number': 'baz'}])
    m = create_clean_collection(old_coll, new_coll, crosswalk)
    a = list(new_coll.find())
    assert(len(a) == 3)
    li = [i['workerPhone'] for i in a]
    assert('foo' not in li)

def test_payments_df_has_count(collection, df):
    crosswalk = pd.DataFrame([{ 'old_number': 'no', 'new_payment_number': 'yes'}])
    workers = calc_payments(collection, df, crosswalk, pay_workers)
    assert(workers[workers.number == 'bar'].reports.tolist() == [2,1,1])
