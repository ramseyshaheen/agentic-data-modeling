import pandas as pd
import os

parquet_files = [
    "data/raw/adjustments.parquet",
    "data/raw/contractor_profiles.parquet",
    "data/raw/jobs.parquet",
    "data/raw/payouts.parquet",
    "data/raw/services.parquet",
    "data/raw/users.parquet",
]

for file in parquet_files:
    if not os.path.exists(file):
        print(f"\n FILE NOT FOUND: {file}")
        continue

    print(f"\n{'='*50}")
    print(f"📄 FILE: {file}")
    print(f"{'='*50}")

    df = pd.read_parquet(file)

    # --- Row Count ---
    print(f"\n Row Count: {len(df):,}")
    print(f"📐 Column Count: {len(df.columns)}")

    # --- Schema / Column Types ---
    print(f"\n Schema (Column Names & Types):")
    print(df.dtypes.to_string())

    # --- Null Check ---
    null_counts = df.isnull().sum()
    nulls_present = null_counts[null_counts > 0]
    print(f"\n🔍 Null Value Check:")
    if nulls_present.empty:
        print(" No nulls found.")
    else:
        print(nulls_present.to_string())

    # --- Duplicate Check ---
    duplicate_count = df.duplicated().sum()
    print(f"\n Duplicate Row Check:")
    if duplicate_count == 0:
        print("   No duplicate rows found.")
    else:
        print(f"   {duplicate_count:,} duplicate rows found.")