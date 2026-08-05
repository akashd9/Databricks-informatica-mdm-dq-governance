"""Generates a larger, messier synthetic customer dataset than
generate_pilot_dataset.py — same raw schema per source (so Autoloader's
already-established schema for the live bronze tables doesn't need to
evolve), but scaled up and deliberately harder:

  - 3,000 true customers instead of 200 (roughly 6x the raw record volume).
  - Higher per-field corruption rate (35% vs the pilot's ~15%), plus new
    corruption types the pilot didn't exercise: unicode/accented names,
    multiple date formats (including outright garbage), lowercase/invalid
    country codes, exact duplicate rows within a source, and "burst"
    duplicates (some true customers appear 5-8x in the *same* source, not
    just once per source — a bad-upstream-export pattern the original
    pilot never modeled).
  - Deliberately UNPREFIXED raw IDs that can collide across sources (ERP
    customer "1042" and CRM customer "1042" being different people) — the
    known gap PILOT_REPORT.md documents but the original pilot's
    source-prefixed IDs (ERP-0001, CRM-0001, ...) never actually exercised.
    match_merge.py/survivorship.py use the raw ID as-is with no prefix, so
    this is the more realistic (and harder) case.

Writes new files under pilot/sample_data/ alongside (not overwriting) the
original pilot files, named *_stress.<ext> — Autoloader picks up new files
in an already-watched directory incrementally, without touching the
established schema, as long as the column set stays identical to the
existing files.

Usage: python pilot/generate_stress_dataset.py
"""
import random
from pathlib import Path

import pandas as pd

random.seed(1337)

OUT_DIR = Path(__file__).resolve().parent / "sample_data"
N_TRUE_CUSTOMERS = 3000
CORRUPTION_RATE = 0.35
COUNTRIES = ["US", "CA", "UK", "IN", "DE", "FR", "BR", "AU"]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda", "David", "Elizabeth",
    "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen",
    "José", "François", "Müller", "Søren", "Nguyễn", "Zoë", "Renée", "André", "Björn", "Håkon",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
    "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "García", "Müller", "O'Brien", "Van Der Berg", "Nguyễn", "Øst", "Fürst", "Łukasz",
]


def _random_name():
    return f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"


def _noisy_variant(name):
    first, last = name.split()[0], name.split()[-1]
    variants = [
        f"{first[0]}. {last}",
        f"{first} {last[0]}.",
        name.upper(),
        name.lower(),
        f"  {name}  ",
        f"{first}\t{last}",
        f"{first}-{last}",
    ]
    return random.choice(variants)


def _random_email(name, force_bad=False):
    local = name.lower().replace(" ", ".").replace("'", "")
    if force_bad:
        return random.choice([
            "not-an-email", f"{local}@@example.com", f"{local}@", f"@example.com",
            f"{local} @example.com", "", None,
        ])
    return f"{local}@{random.choice(['acme.com', 'example.com', 'mail.com', 'globex.co'])}"


def _random_tax_id(force_bad=False):
    if force_bad:
        return random.choice(["???", "N/A", "", "0000000000", "TAX-", None])
    return f"TAX-{random.randint(10000, 999999)}"


def _random_date(force_bad=False):
    if force_bad:
        return random.choice([
            "not-a-date", "0000-00-00", "13/45/2026", "", None, "2026",
        ])
    fmt = random.choice(["iso", "us", "compact"])
    y, m, d = 2026, random.randint(1, 7), random.randint(1, 28)
    if fmt == "iso":
        return f"{y}-{m:02d}-{d:02d}"
    if fmt == "us":
        return f"{m:02d}/{d:02d}/{y}"
    return f"{y}{m:02d}{d:02d}"


def _random_country(force_bad=False):
    c = random.choice(COUNTRIES)
    if force_bad:
        return random.choice([c.lower(), f" {c}", "XX", "", None])
    return c


def _maybe(value, bad_value_fn):
    return bad_value_fn() if random.random() < CORRUPTION_RATE else value


def generate():
    erp_rows, crm_rows, flatfile_rows, api_rows = [], [], [], []

    for i in range(N_TRUE_CUSTOMERS):
        name = _random_name()
        email = _random_email(name)
        tax_id = _random_tax_id()
        country = random.choice(COUNTRIES)
        postal = str(random.randint(10000, 99999))
        address = f"{random.randint(1, 9999)} {'Main St' if random.random() > 0.5 else 'Oak Ave'}"

        sources = random.sample(["erp", "crm", "flatfile", "partner_api"], k=random.randint(1, 4))

        for src in sources:
            # Unprefixed raw ID, deliberately allowed to collide across
            # sources — see module docstring. Small ID space relative to
            # N_TRUE_CUSTOMERS guarantees real collisions.
            raw_id = str(random.randint(1, N_TRUE_CUSTOMERS // 2))
            # Burst duplicates: ~8% of (true customer, source) pairs get
            # written multiple times, some as exact repeats, some as
            # independently-corrupted repeats — both are real patterns from
            # a flaky upstream export.
            copies = random.choices([1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 3, 5, 8], k=1)[0]

            for _ in range(copies):
                noisy_name = _maybe(name, lambda: _noisy_variant(name) if random.random() > 0.1 else None)
                record_email = _maybe(email, lambda: _random_email(name, force_bad=True))
                record_tax = _maybe(tax_id, lambda: _random_tax_id(force_bad=True))
                record_country = _maybe(country, lambda: _random_country(force_bad=True))
                updated_at = _maybe(_random_date(), lambda: _random_date(force_bad=True))

                if src == "erp":
                    erp_rows.append({
                        "cust_no": raw_id, "cust_name": noisy_name, "email_addr": record_email,
                        "fed_tax_id": record_tax, "addr1": address, "ctry_cd": record_country,
                        "zip_cd": postal, "last_upd_ts": updated_at,
                    })
                elif src == "crm":
                    crm_rows.append({
                        "account_id": raw_id, "account_name": noisy_name, "primary_email": record_email,
                        "vat_number": record_tax, "billing_address": address, "country": record_country,
                        "postal_code": postal, "modified_date": updated_at,
                    })
                elif src == "flatfile":
                    flatfile_rows.append({
                        "id": raw_id, "name": noisy_name, "email": record_email,
                        "tax_id": record_tax, "address": address, "country": record_country,
                        "zip": postal, "load_date": updated_at,
                    })
                else:
                    api_rows.append({
                        "partnerCustomerId": raw_id, "legalName": noisy_name, "contactEmail": record_email,
                        "taxIdentifier": record_tax, "address": address, "countryCode": record_country,
                        "postalCode": postal, "lastModified": updated_at,
                    })

    for sub in ("erp", "crm", "flatfile", "api"):
        (OUT_DIR / sub).mkdir(parents=True, exist_ok=True)

    pd.DataFrame(erp_rows).to_parquet(OUT_DIR / "erp" / "customers_stress.parquet", index=False)
    pd.DataFrame(crm_rows).to_json(OUT_DIR / "crm" / "customers_stress.json", orient="records")
    pd.DataFrame(flatfile_rows).to_csv(OUT_DIR / "flatfile" / "customers_stress.csv", index=False)
    pd.DataFrame(api_rows).to_json(OUT_DIR / "api" / "customers_stress.json", orient="records")

    total = len(erp_rows) + len(crm_rows) + len(flatfile_rows) + len(api_rows)
    print(f"Generated {total} raw records from {N_TRUE_CUSTOMERS} true customers (with burst duplicates and ID collisions):")
    print(f"  erp={len(erp_rows)} crm={len(crm_rows)} flatfile={len(flatfile_rows)} api={len(api_rows)}")


if __name__ == "__main__":
    generate()
