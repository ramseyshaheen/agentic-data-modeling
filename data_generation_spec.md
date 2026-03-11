1. Global Parameters

Random seed: 42

Date range: 2024-01-01 to 2024-12-31

Output format: Parquet

Output directory: data/raw/

Libraries: pandas, numpy, pyarrow

Script must be deterministic and reproducible.

All foreign key relationships must be validated before writing files.

2. Business Context

This dataset simulates a managed contractor marketplace connecting customers with independent service providers.

Platform characteristics:

Customers book contractors for home services.

Contractors set service rates.

Platform charges commission (varies by contractor tier).

Some jobs are canceled.

Some completed jobs receive post-service adjustments (refunds or bonuses).

Contractors are paid after a 7–14 day settlement delay.

Some contractors may be suspended.

This dataset must support modeling of:

Gross Booking Value (GBV)

Net Booking Value (after adjustments)

Platform revenue

Contractor earnings

Cancellation rate

3. Table Specifications
3.1 users

Core identity table for all users.

Row count: 3,000

Columns:

user_id (int, unique)

signup_date (date between 2023-01-01 and 2024-12-31)

is_active (boolean)

is_suspended (boolean)

city (string: NYC, LA, Chicago, Houston, Phoenix)

Rules:

95% of users are active.

Suspended users must be inactive.

Signup dates uniformly distributed.

Users may be either customers or contractors (role defined by presence in contractor_profiles).

3.2 contractor_profiles

Role-specific table for contractors only.

Row count: 27% of users (exactly 810 contractors)

Columns:

user_id (int, FK → users.user_id)

contractor_tier (standard / premium)

commission_rate (float)

onboarding_completed (boolean)

background_check_passed (boolean)

Rules:

10% of contractors are premium.

Commission rate:

standard = 20%

premium = 15%

98% passed background check.

95% completed onboarding.

Suspended contractors may still exist but cannot complete jobs after suspension.

3.3 services

Services offered by contractors.

Row count: 1,800

Columns:

service_id (int, unique)

contractor_id (int, FK → contractor_profiles.user_id)

service_category (plumbing, electrical, cleaning, landscaping)

hourly_rate (float)

created_at (date in 2024)

is_available (boolean)

Rules:

Each contractor may have 0–5 services.

Some contractors have zero services.

Category distribution:

cleaning: 35%

plumbing: 25%

electrical: 20%

landscaping: 20%

Hourly rate ranges:

cleaning: 25–60

plumbing: 60–150

electrical: 70–160

landscaping: 40–120

Premium contractors tend toward upper half of rate range.

3.4 jobs

Customer bookings.

Row count: 22,000–26,000 (random in this range)

Columns:

job_id (int, unique)

service_id (int, FK → services.service_id)

contractor_id (int, FK → contractor_profiles.user_id)

customer_id (int, FK → users.user_id not in contractor_profiles)

job_requested_at (datetime in 2024)

job_completed_at (datetime, nullable)

job_status (completed / canceled_by_customer / canceled_by_contractor)

estimated_hours (float: 1–8)

actual_hours (float, nullable)

gross_amount (float)

Rules:

contractor_id must match service.contractor_id.

12–18% of jobs are canceled.

Only completed jobs have:

job_completed_at

actual_hours

gross_amount

actual_hours may vary ±30% from estimated_hours.

gross_amount = actual_hours × hourly_rate.

Seasonality:

Landscaping higher in summer (Jun–Aug +40%).

Cleaning higher in December (+30%).

3.5 adjustments

Post-job financial adjustments.

Applies only to completed jobs.

Refund/bonus rate: 8–12% of completed jobs.

Columns:

adjustment_id (int, unique)

job_id (int, FK → jobs.job_id)

adjustment_date (date ≥ job_completed_at)

adjustment_type (partial_refund / full_refund / goodwill_bonus)

adjustment_amount (float)

Rules:

70% are partial_refund.

20% full_refund.

10% goodwill_bonus.

adjustment_amount ≤ gross_amount.

Some adjustments occur in the month following job completion.

Adjustments must occur within 30 days of job completion.

3.6 payouts

Contractor settlement payments.

One payout per completed job.

Columns:

payout_id (int, unique)

contractor_id (int, FK → contractor_profiles.user_id)

job_id (int, FK → jobs.job_id)

payout_date (job_completed_at + 7–14 days)

commission_rate (copied from contractor_profiles at job time)

payout_amount (float)

payout_status (pending / completed)

Rules:

payout_amount = gross_amount − (gross_amount × commission_rate) − refund adjustments.

Goodwill bonuses increase payout.

If adjustment occurs before payout_date → reflected in payout_amount.

If adjustment occurs after payout_date → payout_amount unchanged.

95% of payouts completed by year end.

payout_amount must be ≥ 0.

4. Integrity Constraints

The generation script must enforce:

All foreign keys valid.

No orphan contractor profiles.

No orphan services.

No orphan jobs.

No orphan adjustments.

No orphan payouts.

adjustment_amount ≤ gross_amount.

payout_amount ≥ 0.

Deterministic results with seed = 42.

No duplicate IDs.

Script must validate constraints before writing Parquet files.

5. Output Files

Write:

data/raw/users.parquet

data/raw/contractor_profiles.parquet

data/raw/services.parquet

data/raw/jobs.parquet

data/raw/adjustments.parquet

data/raw/payouts.parquet

Print final row counts for each table.