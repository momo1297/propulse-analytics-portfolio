import duckdb
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

fake = Faker('fr_FR')
np.random.seed(42)
random.seed(42)

# ================================================
# CONFIG
# ================================================
N_SUBSCRIBERS = 10_000
START_DATE = datetime(2023, 1, 1)
END_DATE   = datetime(2025, 3, 1)

PARTNERS = [
    {'id': 'P001', 'name': 'HealthMedia',     'focus': 'health'},
    {'id': 'P002', 'name': 'WellnessPress',   'focus': 'wellness'},
    {'id': 'P003', 'name': 'NutriDigital',    'focus': 'nutrition'},
]

CHANNELS = {
    'organic_search': 0.25,
    'email_campaign': 0.30,
    'paid_social':    0.20,
    'referral':       0.15,
    'direct':         0.10,
}

PLANS = {
    'monthly':    {'amount': 9.90,  'churn_base': 0.08},
    'quarterly':  {'amount': 24.90, 'churn_base': 0.04},
    'annual':     {'amount': 79.90, 'churn_base': 0.015},
}

COUNTRIES = {'FR': 0.65, 'BE': 0.15, 'CH': 0.12, 'CA': 0.08}

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

# ================================================
# TABLE 1 : SUBSCRIBERS
# ================================================
print("Generating subscribers...")

rows = []
for i in range(N_SUBSCRIBERS):
    partner  = random.choices(PARTNERS, weights=[0.45, 0.35, 0.20])[0]
    channel  = random.choices(list(CHANNELS.keys()), weights=list(CHANNELS.values()))[0]
    plan     = random.choices(list(PLANS.keys()), weights=[0.55, 0.30, 0.15])[0]
    country  = random.choices(list(COUNTRIES.keys()), weights=list(COUNTRIES.values()))[0]

    acq_date = random_date(START_DATE, END_DATE - timedelta(days=90))

    # Conversion : pas tous les leads deviennent payants
    converted = random.random() < 0.62
    if converted:
        days_to_convert = int(np.random.exponential(scale=7))
        conv_date = acq_date + timedelta(days=max(1, days_to_convert))
    else:
        conv_date = None

    # Churn : dépend du plan + canal + engagement email
    churn_prob = PLANS[plan]['churn_base']
    if channel == 'paid_social':   churn_prob *= 1.4
    if channel == 'email_campaign': churn_prob *= 0.8
    if channel == 'referral':       churn_prob *= 0.7
    if country != 'FR':             churn_prob *= 1.1

    # Probabilité cumulée sur la durée — plafonnée à 85% max
    months_active = max(1, (END_DATE - acq_date).days / 30)
    cumulative_churn = 1 - (1 - churn_prob) ** months_active
    cumulative_churn = min(cumulative_churn, 0.85)
    churned = converted and (random.random() < cumulative_churn)
    if churned and conv_date:
        days_active = int(np.random.exponential(scale=120))
        churn_date  = conv_date + timedelta(days=max(30, days_active))
        churn_date  = min(churn_date, END_DATE)
        churn_reason = random.choices(
            ['price', 'content', 'competitor', 'inactive', 'payment_failed'],
            weights=[0.25, 0.30, 0.15, 0.20, 0.10]
        )[0]
        status = 'churned'
    else:
        churn_date   = None
        churn_reason = None
        status       = 'active' if converted else 'lead'

    rows.append({
        'subscriber_id':       f'SUB{i+1:06d}',
        'partner_id':          partner['id'],
        'partner_name':        partner['name'],
        'acquisition_channel': channel,
        'acquisition_date':    acq_date.date(),
        'conversion_date':     conv_date.date() if conv_date else None,
        'plan_type':           plan if converted else None,
        'monthly_amount':      PLANS[plan]['amount'] if converted else 0.0,
        'status':              status,
        'churn_date':          churn_date.date() if churn_date else None,
        'churn_reason':        churn_reason,
        'country':             country,
    })

subscribers_df = pd.DataFrame(rows)
print(f"  -> {len(subscribers_df)} subscribers | {subscribers_df['status'].value_counts().to_dict()}")

# ================================================
# TABLE 2 : EMAIL EVENTS
# ================================================
print("Generating email events...")

email_rows = []
email_types = ['welcome', 'newsletter_weekly', 'promo_offer',
               'reengagement', 'payment_reminder', 'upsell']

for _, sub in subscribers_df.iterrows():
    # Chaque subscriber reçoit entre 4 et 80 emails
    n_emails = random.randint(4, 80)

    start = sub['acquisition_date']
    end   = sub['churn_date'] if sub['churn_date'] else END_DATE.date()

    if start >= end:
        continue

    # Engagement : varie selon le statut et le canal
    if sub['status'] == 'active':
        open_rate  = random.uniform(0.35, 0.75)
        click_rate = random.uniform(0.10, 0.30)
    elif sub['status'] == 'churned':
        # Engagement baisse avant le churn
        open_rate  = random.uniform(0.05, 0.40)
        click_rate = random.uniform(0.02, 0.12)
    else:
        open_rate  = random.uniform(0.20, 0.55)
        click_rate = random.uniform(0.05, 0.20)

    for j in range(n_emails):
        days_range = (end - start).days
        if days_range <= 0:
            continue
        sent_date = start + timedelta(days=random.randint(0, days_range))
        opened    = random.random() < open_rate
        clicked   = opened and (random.random() < click_rate)
        unsub     = (not opened) and (random.random() < 0.002)

        email_rows.append({
            'event_id':       f'EML{len(email_rows)+1:08d}',
            'subscriber_id':  sub['subscriber_id'],
            'partner_id':     sub['partner_id'],
            'sent_date':      sent_date,
            'email_type':     random.choice(email_types),
            'opened':         opened,
            'clicked':        clicked,
            'unsubscribed':   unsub,
        })

emails_df = pd.DataFrame(email_rows)
print(f"  -> {len(emails_df):,} email events")

# ================================================
# TABLE 3 : TRANSACTIONS
# ================================================
print("Generating transactions...")

payment_methods = {'card': 0.60, 'sepa': 0.25, 'paypal': 0.15}
tx_rows = []

converted = subscribers_df[subscribers_df['conversion_date'].notna()].copy()

for _, sub in converted.iterrows():
    start = pd.to_datetime(sub['conversion_date'])
    end   = pd.to_datetime(sub['churn_date']) if sub['churn_date'] else pd.to_datetime(END_DATE)

    if sub['plan_type'] == 'monthly':
        interval_days = 30
    elif sub['plan_type'] == 'quarterly':
        interval_days = 90
    else:
        interval_days = 365

    amount  = sub['monthly_amount']
    method  = random.choices(list(payment_methods.keys()),
                              weights=list(payment_methods.values()))[0]
    tx_date = start

    while tx_date <= end:
        # ~5% de paiements échoués
        failed = random.random() < 0.05
        status = 'failed' if failed else 'success'

        # Retry si échec
        if failed and random.random() < 0.70:
            retry_date = tx_date + timedelta(days=3)
            tx_rows.append({
                'transaction_id':  f'TX{len(tx_rows)+1:08d}',
                'subscriber_id':   sub['subscriber_id'],
                'partner_id':      sub['partner_id'],
                'transaction_date': retry_date.date(),
                'amount':          amount,
                'status':          'success',
                'payment_method':  method,
                'plan_type':       sub['plan_type'],
                'is_retry':        True,
            })

        tx_rows.append({
            'transaction_id':   f'TX{len(tx_rows)+1:08d}',
            'subscriber_id':    sub['subscriber_id'],
            'partner_id':       sub['partner_id'],
            'transaction_date': tx_date.date(),
            'amount':           amount,
            'status':           status,
            'payment_method':   method,
            'plan_type':        sub['plan_type'],
            'is_retry':         False,
        })

        tx_date += timedelta(days=interval_days)

transactions_df = pd.DataFrame(tx_rows)
success_tx = transactions_df[transactions_df['status'] == 'success']
print(f"  -> {len(transactions_df):,} transactions | revenue total: {success_tx['amount'].sum():,.0f} EUR")

# ================================================
# SAVE
# ================================================
print("\nSaving files...")

os.makedirs('data/bronze', exist_ok=True)

subscribers_df.to_csv('data/bronze/subscribers_raw.csv', index=False)
emails_df.to_csv('data/bronze/email_events_raw.csv', index=False)
transactions_df.to_csv('data/bronze/transactions_raw.csv', index=False)

print("  -> data/bronze/subscribers_raw.csv")
print("  -> data/bronze/email_events_raw.csv")
print("  -> data/bronze/transactions_raw.csv")
print("\nDone. Run your pipeline next.")
