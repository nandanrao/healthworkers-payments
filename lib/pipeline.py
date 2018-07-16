def tag_training_messages(messages, numbers, crosswalk):
    """ Tags messages as training based on dates/numbers """
    pass

def translate_numbers(df, crosswalk, old_key, new_key):
    d = df.merge(crosswalk, how = 'left', left_on = old_key, right_on= 'old_number')
    idx_old = d.new_payment_number.isna()
    idx_new = d.new_payment_number.notna()
    d.loc[idx_old, new_key] = d[idx_old][old_key]
    d.loc[idx_new, new_key] = d[idx_new].new_payment_number
    return d.drop(crosswalk.columns, 1)

def merge_worker_info(messages, roster, drop_keys):
    m = messages.merge(roster, how = 'left', left_on = 'paymentPhone', right_on = 'reporting_number')
    return m.drop(drop_keys, 1)

def assign_tester_numbers(messages, roster):
    idx = ~messages.paymentPhone.isin(roster.reporting_number)
    messages.loc[idx, 'training'] = True
    return messages

def assign_training_messages(messages):
    message_days = messages.serviceDate.map(lambda d: d.replace(hour=0,minute=0,second=0))
    idx = messages.training_date >= message_days
    messages.loc[idx, 'training'] = True
    return messages

def add_db_events(messages, events):
    messages['called'] = False
    messages['noConsent'] = False
    messages['attempted'] = False
    for e in events:
        key = e['event']
        i = e['record']['_id']
        messages.loc[messages._id == i, key] = True
    return messages

def pipeline(messages, events, roster, crosswalk):
    k = 'reporting_number'
    roster = translate_numbers(roster, crosswalk, old_key = k, new_key = k)
    return (messages
            # Remove duplicates based on above id
            .drop_duplicates('_id')
            # Convert numbers to current payment number
            .pipe(translate_numbers,
                  crosswalk = crosswalk,
                  old_key = 'senderPhone',
                  new_key = 'paymentPhone')
            # tag name/district/area/training-day of worker
            .pipe(merge_worker_info,
                  roster = roster,
                  drop_keys = ['reporting_number', 'contact_number'])
            # Add column for training
            .assign(training = False)
            # Tag training messages from non-worker numbers
            .pipe(assign_tester_numbers, roster=roster)
            # Tag training messages from training day of worker
            .pipe(assign_training_messages)
            # Add events: no-consent, attempts, and called
            .pipe(add_db_events, events = events))



# Calculate needed calls per worker, groupby district, write to redis list

# Calculate payment per worker and super

# Write out reports (with typeform data)

# Convert attempts into events
# Convert noConsent into events (makeup timestamp)
# Convert called into events ( timestamp from Typeform?? )
