"""Generates a synthetic multi-source pilot dataset for locally tuning the
DQ/MDM fallback thresholds in config/dq_rules.yml and config/match_rules.yml
without needing a Databricks cluster or live Informatica connectivity.

Writes one raw file per source, in that source's real raw schema (matching
src/silver/silver_transform.py's column maps exactly), under
pilot/sample_data/. Also writes pilot/ground_truth.csv recording which
records are duplicates of the same underlying "true" customer — the known
answer run_pilot_validation.py checks match/merge precision/recall against.

Usage: python pilot/generate_pilot_dataset.py
"""
import csv
import random
from pathlib import Path

import pandas as pd

random.seed(42)

OUT_DIR = Path(__file__).resolve().parent / "sample_data"
N_TRUE_CUSTOMERS = 200
COUNTRIES = ["US", "CA", "UK", "IN", "DE"]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda", "David", "Elizabeth",
    "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
    "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
]


def _random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def _noisy_variant(name):
    """Simulates realistic near-duplicate spellings across source systems."""
    first, last = name.split()
    variants = [
        f"{first[0]}. {last}",   # "J. Smith"
        f"{first} {last[0]}.",   # "James S."
        name.upper(),
        name.lower(),
        f"{first}  {last}",      # double-space typo
    ]
    return random.choice(variants)


def _random_email(name):
    local = name.lower().replace(" ", ".")
    return f"{local}@{random.choice(['acme.com', 'example.com', 'mail.com'])}"


def _random_tax_id():
    return f"TAX-{random.randint(10000, 99999)}"


def _random_date():
    return f"2026-{random.randint(1, 7):02d}-{random.randint(10, 28):02d}"


def generate():
    erp_rows, crm_rows, flatfile_rows, api_rows = [], [], [], []
    ground_truth = []

    for i in range(N_TRUE_CUSTOMERS):
        true_id = f"TRUE-{i:04d}"
        name = _random_name()
        email = _random_email(name)
        tax_id = _random_tax_id()
        country = random.choice(COUNTRIES)
        postal = str(random.randint(10000, 99999))
        address = f"{random.randint(1, 999)} Main St"

        # Each true customer shows up in 1-4 of the 4 source systems.
        sources = random.sample(["erp", "crm", "flatfile", "partner_api"], k=random.randint(1, 4))

        for src in sources:
            raw_id = f"{src.upper()}-{i:04d}"
            noisy_name = name if random.random() > 0.4 else _noisy_variant(name)
            record_email = email if random.random() > 0.15 else "not-an-email"
            record_tax = tax_id if random.random() > 0.15 else "???"
            record_name = None if random.random() < 0.05 else noisy_name
            updated_at = _random_date()

            if src == "erp":
                erp_rows.append({
                    "cust_no": raw_id, "cust_name": record_name, "email_addr": record_email,
                    "fed_tax_id": record_tax, "addr1": address, "ctry_cd": country,
                    "zip_cd": postal, "last_upd_ts": updated_at,
                })
            elif src == "crm":
                crm_rows.append({
                    "account_id": raw_id, "account_name": record_name, "primary_email": record_email,
                    "vat_number": record_tax, "billing_address": address, "country": country,
                    "postal_code": postal, "modified_date": updated_at,
                })
            elif src == "flatfile":
                flatfile_rows.append({
                    "id": raw_id, "name": record_name, "email": record_email,
                    "tax_id": record_tax, "address": address, "country": country,
                    "zip": postal, "load_date": updated_at,
                })
            else:
                api_rows.append({
                    "partnerCustomerId": raw_id, "legalName": record_name, "contactEmail": record_email,
                    "taxIdentifier": record_tax, "address": address, "countryCode": country,
                    "postalCode": postal, "lastModified": updated_at,
                })

            ground_truth.append({"source": src, "raw_id": raw_id, "true_customer_id": true_id})

    for sub in ("erp", "crm", "flatfile", "api"):
        (OUT_DIR / sub).mkdir(parents=True, exist_ok=True)

    pd.DataFrame(erp_rows).to_parquet(OUT_DIR / "erp" / "customers.parquet", index=False)
    pd.DataFrame(crm_rows).to_json(OUT_DIR / "crm" / "customers.json", orient="records")
    pd.DataFrame(flatfile_rows).to_csv(OUT_DIR / "flatfile" / "customers.csv", index=False)
    pd.DataFrame(api_rows).to_json(OUT_DIR / "api" / "customers.json", orient="records")

    with open(Path(__file__).resolve().parent / "ground_truth.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["source", "raw_id", "true_customer_id"])
        writer.writeheader()
        writer.writerows(ground_truth)

    print(f"Generated {len(ground_truth)} raw records across {N_TRUE_CUSTOMERS} true customers:")
    print(f"  erp={len(erp_rows)} crm={len(crm_rows)} flatfile={len(flatfile_rows)} api={len(api_rows)}")


if __name__ == "__main__":
    generate()
