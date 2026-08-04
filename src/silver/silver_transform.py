"""Silver layer: maps each source's raw column names onto the canonical
customer entity schema, applies light standardization (trim, casing), and
unions everything into one entity-resolved-ready stream. This is deliberately
"dumb" standardization only — validation and match-readiness scoring belong
to the DQ gate and MDM gate downstream, not here.
"""
import dlt
from pyspark.sql import functions as F

_BRONZE_TABLES = {
    "erp": "bronze_erp_customer",
    "crm": "bronze_crm_customer",
    "flatfile": "bronze_flatfile_customer",
    "partner_api": "bronze_partner_api_customer",
}

# Raw column name -> canonical column name, per source system.
_COLUMN_MAPS = {
    "erp": {
        "customer_id": "cust_no",
        "full_name": "cust_name",
        "email": "email_addr",
        "tax_id": "fed_tax_id",
        "address_line1": "addr1",
        "country_code": "ctry_cd",
        "postal_code": "zip_cd",
        "updated_at": "last_upd_ts",
    },
    "crm": {
        "customer_id": "account_id",
        "full_name": "account_name",
        "email": "primary_email",
        "tax_id": "vat_number",
        "address_line1": "billing_address",
        "country_code": "country",
        "postal_code": "postal_code",
        "updated_at": "modified_date",
    },
    "flatfile": {
        "customer_id": "id",
        "full_name": "name",
        "email": "email",
        "tax_id": "tax_id",
        "address_line1": "address",
        "country_code": "country",
        "postal_code": "zip",
        "updated_at": "load_date",
    },
    "partner_api": {
        "customer_id": "partnerCustomerId",
        "full_name": "legalName",
        "email": "contactEmail",
        "tax_id": "taxIdentifier",
        "address_line1": "address",
        "country_code": "countryCode",
        "postal_code": "postalCode",
        "updated_at": "lastModified",
    },
}

_CANONICAL_COLUMNS = [
    "customer_id", "full_name", "email", "tax_id",
    "address_line1", "country_code", "postal_code", "updated_at",
]


def _standardize(source_name: str, bronze_table: str):
    colmap = _COLUMN_MAPS[source_name]

    @dlt.table(
        name=f"silver_{source_name}_customer_std",
        comment=f"Standardized {source_name} customer records mapped to the canonical entity schema.",
        table_properties={"quality": "silver"},
    )
    def _std():
        df = dlt.read_stream(bronze_table)
        select_exprs = [
            F.trim(F.col(colmap[c])).alias(c) if c in colmap else F.lit(None).cast("string").alias(c)
            for c in _CANONICAL_COLUMNS
        ]
        return (
            df.select(*select_exprs, "_source_system", "_source_priority", "_ingested_at", "_source_file")
            .withColumn("full_name", F.initcap("full_name"))
            .withColumn("email", F.lower("email"))
            .withColumn("country_code", F.upper("country_code"))
            .withColumn("email_domain", F.regexp_extract("email", r"@(.+)$", 1))
        )

    return _std


for _name, _table in _BRONZE_TABLES.items():
    _standardize(_name, _table)


@dlt.table(
    name="silver_customer_standardized",
    comment="Union of all source-standardized customer records, prior to the DQ gate.",
    table_properties={"quality": "silver"},
)
def silver_customer_standardized():
    frames = [dlt.read_stream(f"silver_{s}_customer_std") for s in _BRONZE_TABLES]
    unioned = frames[0]
    for f in frames[1:]:
        unioned = unioned.unionByName(f)
    return unioned
