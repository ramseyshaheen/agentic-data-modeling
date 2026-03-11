import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

# GLOBAL SETTINGS

np.random.seed(42)

DATE_START = pd.Timestamp("2024-01-01")
DATE_END = pd.Timestamp("2024-12-31")

OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# USERS

n_users = 3000
user_ids = np.arange(1, n_users + 1)

signup_dates = pd.to_datetime(
    np.random.randint(
        pd.Timestamp("2023-01-01").value // 10**9,
        pd.Timestamp("2024-12-31").value // 10**9,
        n_users
    ),
    unit="s"
)

is_active = np.random.choice([True, False], size=n_users, p=[0.95, 0.05])
is_suspended = np.zeros(n_users, dtype=bool)

# Suspended users must be inactive
suspended_indices = np.random.choice(n_users, size=int(0.02 * n_users), replace=False)
is_suspended[suspended_indices] = True
is_active[suspended_indices] = False

cities = np.random.choice(["NYC", "LA", "Chicago", "Houston", "Phoenix"], size=n_users)

users = pd.DataFrame({
    "user_id": user_ids,
    "signup_date": signup_dates,
    "is_active": is_active,
    "is_suspended": is_suspended,
    "city": cities
})

# CONTRACTOR PROFILES

n_contractors = 810
contractor_ids = np.random.choice(user_ids, size=n_contractors, replace=False)

tiers = np.random.choice(
    ["standard", "premium"],
    size=n_contractors,
    p=[0.9, 0.1]
)

commission_rate = np.where(tiers == "premium", 0.15, 0.20)

contractor_profiles = pd.DataFrame({
    "user_id": contractor_ids,
    "contractor_tier": tiers,
    "commission_rate": commission_rate,
    "onboarding_completed": np.random.choice([True, False], size=n_contractors, p=[0.95, 0.05]),
    "background_check_passed": np.random.choice([True, False], size=n_contractors, p=[0.98, 0.02])
})

# SERVICES

n_services = 1800
service_ids = np.arange(1, n_services + 1)

service_categories = np.random.choice(
    ["cleaning", "plumbing", "electrical", "landscaping"],
    size=n_services,
    p=[0.35, 0.25, 0.20, 0.20]
)

service_contractors = np.random.choice(contractor_ids, size=n_services)

created_at = pd.to_datetime(
    np.random.randint(DATE_START.value // 10**9,
                      DATE_END.value // 10**9,
                      n_services),
    unit="s"
)

def generate_hourly_rate(category, tier):
    ranges = {
        "cleaning": (25, 60),
        "plumbing": (60, 150),
        "electrical": (70, 160),
        "landscaping": (40, 120)
    }
    low, high = ranges[category]
    if tier == "premium":
        low = (low + high) / 2
    return np.round(np.random.uniform(low, high), 2)

tier_lookup = contractor_profiles.set_index("user_id")["contractor_tier"]

hourly_rates = [
    generate_hourly_rate(cat, tier_lookup[contractor])
    for cat, contractor in zip(service_categories, service_contractors)
]

services = pd.DataFrame({
    "service_id": service_ids,
    "contractor_id": service_contractors,
    "service_category": service_categories,
    "hourly_rate": hourly_rates,
    "created_at": created_at,
    "is_available": np.random.choice([True, False], size=n_services, p=[0.9, 0.1])
})

# JOBS

n_jobs = np.random.randint(22000, 26001)
job_ids = np.arange(1, n_jobs + 1)

service_choices = services.sample(n=n_jobs, replace=True)

job_requested_at = pd.to_datetime(
    np.random.randint(DATE_START.value // 10**9,
                      DATE_END.value // 10**9,
                      n_jobs),
    unit="s"
)

job_status = np.random.choice(
    ["completed", "canceled_by_customer", "canceled_by_contractor"],
    size=n_jobs,
    p=[0.85, 0.10, 0.05]
)

estimated_hours = np.round(np.random.uniform(1, 8, n_jobs), 2)
actual_hours = []
gross_amount = []
job_completed_at = []

for i in range(n_jobs):
    if job_status[i] == "completed":
        variation = np.random.uniform(0.7, 1.3)
        hours = np.round(estimated_hours[i] * variation, 2)
        actual_hours.append(hours)
        rate = service_choices.iloc[i]["hourly_rate"]
        gross_amount.append(np.round(hours * rate, 2))
        job_completed_at.append(job_requested_at[i] + timedelta(hours=np.random.randint(1, 72)))
    else:
        actual_hours.append(None)
        gross_amount.append(None)
        job_completed_at.append(None)

customer_ids = np.setdiff1d(user_ids, contractor_ids)
job_customers = np.random.choice(customer_ids, size=n_jobs)

jobs = pd.DataFrame({
    "job_id": job_ids,
    "service_id": service_choices["service_id"].values,
    "contractor_id": service_choices["contractor_id"].values,
    "customer_id": job_customers,
    "job_requested_at": job_requested_at,
    "job_completed_at": job_completed_at,
    "job_status": job_status,
    "estimated_hours": estimated_hours,
    "actual_hours": actual_hours,
    "gross_amount": gross_amount
})

# ADJUSTMENTS

completed_jobs = jobs[jobs["job_status"] == "completed"].copy()
n_adjustments = int(len(completed_jobs) * np.random.uniform(0.08, 0.12))

adjusted_jobs = completed_jobs.sample(n=n_adjustments)

adjustment_types = np.random.choice(
    ["partial_refund", "full_refund", "goodwill_bonus"],
    size=n_adjustments,
    p=[0.7, 0.2, 0.1]
)

adjustment_amounts = []
adjustment_dates = []

for idx, row in adjusted_jobs.iterrows():
    max_amount = row["gross_amount"]
    adj_type = adjustment_types[list(adjusted_jobs.index).index(idx)]
    if adj_type == "goodwill_bonus":
        amount = np.round(np.random.uniform(5, 50), 2)
    elif adj_type == "full_refund":
        amount = max_amount
    else:
        amount = np.round(np.random.uniform(0.1, 0.5) * max_amount, 2)
    adjustment_amounts.append(min(amount, max_amount))

    comp_date = row["job_completed_at"]
    delta_days = np.random.randint(0, 30)
    adjustment_dates.append(comp_date + timedelta(days=delta_days))

adjustments = pd.DataFrame({
    "adjustment_id": np.arange(1, n_adjustments + 1),
    "job_id": adjusted_jobs["job_id"].values,
    "adjustment_date": adjustment_dates,
    "adjustment_type": adjustment_types,
    "adjustment_amount": adjustment_amounts
})

# PAYOUTS

payout_rows = []
payout_id = 1

for _, row in completed_jobs.iterrows():
    commission = contractor_profiles.set_index("user_id").loc[row["contractor_id"], "commission_rate"]
    gross = row["gross_amount"]

    job_adjustments = adjustments[adjustments["job_id"] == row["job_id"]]
    adj_before_payout = 0

    payout_date = row["job_completed_at"] + timedelta(days=np.random.randint(7, 15))

    for _, adj in job_adjustments.iterrows():
        if adj["adjustment_date"] <= payout_date:
            if adj["adjustment_type"] in ["partial_refund", "full_refund"]:
                adj_before_payout += adj["adjustment_amount"]
            else:
                adj_before_payout -= adj["adjustment_amount"]

    payout_amount = gross - (gross * commission) - adj_before_payout
    payout_amount = max(0, np.round(payout_amount, 2))

    payout_rows.append({
        "payout_id": payout_id,
        "contractor_id": row["contractor_id"],
        "job_id": row["job_id"],
        "payout_date": payout_date,
        "commission_rate": commission,
        "payout_amount": payout_amount,
        "payout_status": "completed" if np.random.rand() < 0.95 else "pending"
    })

    payout_id += 1

payouts = pd.DataFrame(payout_rows)

# VALIDATIONS

assert users["user_id"].is_unique
assert contractor_profiles["user_id"].isin(users["user_id"]).all()
assert services["contractor_id"].isin(contractor_profiles["user_id"]).all()
assert jobs["service_id"].isin(services["service_id"]).all()
assert jobs["contractor_id"].isin(contractor_profiles["user_id"]).all()
assert jobs["customer_id"].isin(users["user_id"]).all()
assert adjustments["job_id"].isin(jobs["job_id"]).all()
assert payouts["job_id"].isin(jobs["job_id"]).all()
assert (adjustments["adjustment_amount"] >= 0).all()
assert (payouts["payout_amount"] >= 0).all()

# WRITE PARQUET

users.to_parquet(f"{OUTPUT_DIR}/users.parquet", index=False)
contractor_profiles.to_parquet(f"{OUTPUT_DIR}/contractor_profiles.parquet", index=False)
services.to_parquet(f"{OUTPUT_DIR}/services.parquet", index=False)
jobs.to_parquet(f"{OUTPUT_DIR}/jobs.parquet", index=False)
adjustments.to_parquet(f"{OUTPUT_DIR}/adjustments.parquet", index=False)
payouts.to_parquet(f"{OUTPUT_DIR}/payouts.parquet", index=False)

print("Data generation complete:")
print(f"Users: {len(users)}")
print(f"Contractors: {len(contractor_profiles)}")
print(f"Services: {len(services)}")
print(f"Jobs: {len(jobs)}")
print(f"Adjustments: {len(adjustments)}")
print(f"Payouts: {len(payouts)}")
