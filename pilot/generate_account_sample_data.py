"""Quick account-entity sample dataset, matching config/account_column_maps.yml's
raw schemas, for exercising the account pipeline path end to end (the
customer pilot dataset in generate_pilot_dataset.py doesn't cover account).
Not as elaborate as the customer pilot generator — no ground truth file,
this is for a smoke-test run, not threshold tuning.
"""
import random
from pathlib import Path

import pandas as pd

random.seed(7)

OUT_DIR = Path(__file__).resolve().parent / "sample_data" / "account"
COUNTRIES = ["US", "CA", "UK"]
TYPES = ["Prospect", "Customer", "Partner", "Reseller"]
INDUSTRIES = ["Manufacturing", "Retail", "Healthcare", "Technology", "Finance"]
NAME_STEMS = ["Acme", "Globex", "Initech", "Umbrella", "Stark", "Wayne", "Wonka", "Hooli", "Soylent", "Vandelay"]
SUFFIXES = ["Corp", "Industries", "Holdings", "Group", "LLC"]


def _name():
    return f"{random.choice(NAME_STEMS)} {random.choice(SUFFIXES)}"


def generate(n=30):
    erp_rows, crm_rows = [], []
    for i in range(n):
        name = _name()
        acct_type = random.choice(TYPES)
        industry = random.choice(INDUSTRIES)
        revenue = random.randint(100_000, 50_000_000)
        owner = f"CUST-{random.randint(1, 50):04d}"
        country = random.choice(COUNTRIES)
        postal = str(random.randint(10000, 99999))
        date = f"2026-{random.randint(1, 7):02d}-{random.randint(10, 28):02d}"

        # ~40% of accounts appear in both sources (near-duplicate for match/merge)
        in_erp = True
        in_crm = random.random() < 0.4 or i < n // 3

        if in_erp:
            erp_rows.append({
                "acct_no": f"ERP-ACCT-{i:04d}",
                "acct_name": name,
                "acct_type_cd": acct_type,
                "industry_cd": industry,
                "annual_rev": revenue,
                "owning_cust_id": owner,
                "ctry_cd": country,
                "acct_zip_cd": postal,
                "last_upd_ts": date,
            })
        if in_crm:
            crm_rows.append({
                "account_id": f"CRM-ACCT-{i:04d}",
                "name": name.upper() if random.random() < 0.5 else name,
                "type": acct_type,
                "industry": industry,
                "revenue": revenue,
                "owner_id": owner,
                "billing_country": country,
                "billing_postal_code": postal,
                "modified_date": date,
            })

    (OUT_DIR / "erp").mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "crm").mkdir(parents=True, exist_ok=True)
    pd.DataFrame(erp_rows).to_parquet(OUT_DIR / "erp" / "accounts.parquet", index=False)
    pd.DataFrame(crm_rows).to_json(OUT_DIR / "crm" / "accounts.json", orient="records")
    print(f"Generated {len(erp_rows)} erp + {len(crm_rows)} crm account records.")


if __name__ == "__main__":
    generate()
