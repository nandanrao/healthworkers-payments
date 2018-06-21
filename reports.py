import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil import parser
import os, re
import requests
from payments import get_numbers, get_mongo_client, get_crosswalk, get_og_messages
from tag_training import tag_training_df
from load_workers import get_testers

import logging
logging.basicConfig(level = logging.DEBUG)

class FutureException(Exception):
    pass

def get_windows(start = datetime(2018, 5, 1), weeks = 1):
    times = []
    while start < datetime.utcnow():
        end = start + timedelta(weeks = weeks)
        times.append((start.isoformat(), end.isoformat()))
        start = end
    return times

def make_request(form_id, token, since, until):
    url = 'https://api.typeform.com/forms/{}/responses'.format(form_id)
    q = {'since': since, 'until': until, 'page_size': 1000}
    return requests.get(url,
                       headers = {'Authorization': 'bearer {}'.format(token)},
                       params = q)

def get_responses(form_id):
    token = os.getenv('TYPEFORM_TOKEN')
    responses = []
    windows = get_windows()
    for since,until in windows:
        res = make_request(form_id, token, since, until)
        responses += res.json()['items']
    return responses

def clean_typeform(typeform):
    collapse = lambda df: df.head(1).assign(provided_care = np.any(df.provided_care))

    typeform = typeform[~typeform.provided_care.isna()]
    groups = typeform.groupby(['patientphone', 'patient', 'code', 'visitdate'])
    norms = pd.concat([g for i,g in groups if g.shape[0] == 1])
    funkies = pd.concat([collapse(g) for i,g in groups if g.shape[0] > 1])
    return pd.concat([norms, funkies])


def get_question(response, qid, key):
    return next((a[key] for a in response['answers']
                 if a['field']['id'] == qid), None)

def flatten_response(res):
    d = res['hidden']
    provided_care = get_question(res, 'tN8EHB01kUah', 'boolean')
    if provided_care == None:
        d['called'] = False
    else:
        d['called'] = True
        d['provided_care'] = provided_care
    return d


def get_typeform_responses(form_id):
    responses = get_responses(form_id)
    df = pd.DataFrame([flatten_response(r) for r in responses])
    return df



def merge_typeform(messages, typeform):

    typeform = (typeform
                .assign(visitdate = typeform.visitdate.map(parser.parse).map(datetime.date))
                .assign(patient = typeform.patient.str.upper())
                .assign(code = typeform.code.str.upper()))

    messages = (messages.assign(serviceDate = messages.serviceDate.map(datetime.date))
                .assign(patientName = messages.patientName.str.upper())
                .assign(code = messages.code.str.upper())
                .groupby(['workerPhone', 'patientName', 'code', 'patientPhone', 'serviceDate'])
                .apply(lambda df: df.head(1)))

    return (messages
            .merge(typeform,
                   how = 'left',
                   left_on=['workerPhone', 'patientPhone', 'patientName', 'code'],
                   right_on=['workerphone', 'patientphone', 'patient', 'code'],
                   indicator = True)
            [[
              'first_attempt',
              'last_attempt',
              'called',
              '_merge',
              'noConsent',
              'patientName',
              'patientPhone',
              'serviceDate',
              'training',
              'workerPhone',
              'code',
              'provided_care']]
            )

class DataCorruptionError(BaseException):
    pass



# from pymongo import UpdateOne
# from dotenv import load_dotenv
# load_dotenv()

# client = get_mongo_client()
# messages = get_og_messages(client['healthworkers'].messages)

# form_id = 'a1cQMO'

# typeform = clean_typeform(get_typeform_responses(form_id))

# crosswalk = get_crosswalk('number-changes/number_changes.xlsx')
# numbers = get_numbers('rosters/chw_database_20180608.xlsx')

# testers = get_testers(client['healthworkers'].messages, numbers, crosswalk)
# training_dates = [(n['reporting_number'], n['training_date'])
#                   for n in numbers.to_dict(orient='records')]

# tagged = tag_training_df(training_dates, messages, testers)

# # uniques = (tagged
# #            .groupby(['patientName', 'code', 'serviceDate', 'patientPhone'])
# #            .apply(lambda df: df.head(1)))

# messages = None

# merged = merge_typeform(tagged, typeform)

# missing = merged[(~merged.workerPhone.isin(numbers.reporting_number)) &
#                  (merged.training == False)].shape[0]
# if missing:
#     raise Exception('Missing numbers!!')

# numbers = numbers.assign(training_date = numbers.training_date.map(datetime.date))

# final = (merged
#          .merge(numbers, left_on='workerPhone', right_on='reporting_number')
#          .assign(training = merged.training.map(lambda x: x if x == True else False))
#          .assign(called = merged.called.map(lambda x: x if x == True else False))
#          .drop(['workerPhone', '_merge'], 1))

# idx = final.called == True

# final.loc[idx, 'multiple'] = (final
#                               [final.called == True]
#                               .groupby(['patientName', 'code', 'patientPhone', 'reporting_number'])
#                               .apply(lambda df: df.assign(multiple = True if df.shape[0] > 1 else False ))
#                               .multiple.reset_index([0,1,2,3]).multiple)


# final = final.assign(multiple = final.multiple.map(lambda x: x if x == True else False))

# final.to_csv('report_2018-6-13.csv', index=False)
