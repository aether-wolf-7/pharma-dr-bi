"""
Pharma DR · Normalizer
========================
Final transformation step that:
  1. Resolves dimension surrogate keys (city_key, product_key, client_key, etc.)
  2. Computes derived metrics (margin_amount, margin_pct, date_key)
  3. Validates referential integrity
  4. Produces fact_sales-ready DataFrames for the loader
"""

from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import sqlalchemy as sa
from loguru import logger

from etl.config.settings import settings


# ── Embedded product reference (used in demo mode) ────────────
PRODUCTS_REFERENCE = [
    {"product_id": "SAP-001", "product_name": "Amoxicilina 500mg Cápsulas",    "category": "ANTIBIOTICO",    "lab_code": "GENFAR"},
    {"product_id": "SAP-002", "product_name": "Ciprofloxacino 500mg Tabletas", "category": "ANTIBIOTICO",    "lab_code": "BAYER"},
    {"product_id": "SAP-003", "product_name": "Azitromicina 500mg Tabletas",   "category": "ANTIBIOTICO",    "lab_code": "PFIZER"},
    {"product_id": "SAP-004", "product_name": "Clindamicina 300mg Cápsulas",   "category": "ANTIBIOTICO",    "lab_code": "PFIZER"},
    {"product_id": "SAP-005", "product_name": "Ceftriaxona 1g Inyectable",     "category": "ANTIBIOTICO",    "lab_code": "ROCHE"},
    {"product_id": "SAP-006", "product_name": "Metronidazol 500mg Tabletas",   "category": "ANTIBIOTICO",    "lab_code": "SANOFI"},
    {"product_id": "SAP-007", "product_name": "Ibuprofeno 400mg Tabletas",     "category": "ANALGESICO",     "lab_code": "ABBOTT"},
    {"product_id": "SAP-008", "product_name": "Paracetamol 500mg Tabletas",    "category": "ANALGESICO",     "lab_code": "GSK"},
    {"product_id": "SAP-009", "product_name": "Diclofenaco 50mg Tabletas",     "category": "ANALGESICO",     "lab_code": "NOVARTIS"},
    {"product_id": "SAP-010", "product_name": "Naproxeno 500mg Tabletas",      "category": "ANALGESICO",     "lab_code": "ROCHE"},
    {"product_id": "SAP-011", "product_name": "Tramadol 50mg Cápsulas",        "category": "ANALGESICO",     "lab_code": "MERCK"},
    {"product_id": "SAP-012", "product_name": "Vitamina C 500mg Tabletas",     "category": "VITAMINAS",      "lab_code": "BAYER"},
    {"product_id": "SAP-013", "product_name": "Vitamina D3 1000UI Cápsulas",   "category": "VITAMINAS",      "lab_code": "ABBOTT"},
    {"product_id": "SAP-014", "product_name": "Complejo B Tabletas",           "category": "VITAMINAS",      "lab_code": "MERCK"},
    {"product_id": "SAP-015", "product_name": "Zinc 20mg Tabletas",            "category": "VITAMINAS",      "lab_code": "MEDCO"},
    {"product_id": "SAP-016", "product_name": "Calcio + D3 600mg Tabletas",    "category": "VITAMINAS",      "lab_code": "ABBOTT"},
    {"product_id": "SAP-017", "product_name": "Multivitamínico Adultos",       "category": "VITAMINAS",      "lab_code": "PFIZER"},
    {"product_id": "SAP-018", "product_name": "Atorvastatina 20mg Tabletas",   "category": "CARDIOVASCULAR", "lab_code": "PFIZER"},
    {"product_id": "SAP-019", "product_name": "Losartán 50mg Tabletas",        "category": "CARDIOVASCULAR", "lab_code": "MERCK"},
    {"product_id": "SAP-020", "product_name": "Metoprolol 50mg Tabletas",      "category": "CARDIOVASCULAR", "lab_code": "ASTRA"},
    {"product_id": "SAP-025", "product_name": "Metformina 850mg Tabletas",     "category": "DIABETES",       "lab_code": "MERCK"},
    {"product_id": "SAP-026", "product_name": "Glibenclamida 5mg Tabletas",    "category": "DIABETES",       "lab_code": "ROCHE"},
    {"product_id": "SAP-029", "product_name": "Salbutamol 100mcg Inhalador",   "category": "RESPIRATORIO",   "lab_code": "GSK"},
    {"product_id": "SAP-030", "product_name": "Loratadina 10mg Tabletas",      "category": "RESPIRATORIO",   "lab_code": "MERCK"},
    {"product_id": "SAP-034", "product_name": "Omeprazol 20mg Cápsulas",       "category": "GASTRO",         "lab_code": "ASTRA"},
]


class DimensionResolver:
    """
    Resolves natural keys to surrogate dimension keys using PostgreSQL lookups.
    Caches dimension tables in memory for the duration of a pipeline run.
    """

    def __init__(self):
        self._engine = sa.create_engine(settings.postgres_dsn)
        self._cache: dict = {}

    def _load(self, table: str, key_col: str, val_cols: list) -> pd.DataFrame:
        """Load a dimension table into cache."""
        if table not in self._cache:
            cols = ", ".join([key_col] + val_cols)
            df = pd.read_sql(f"SELECT {cols} FROM {table}", self._engine)
            self._cache[table] = df
        return self._cache[table]

    def get_city_keys(self) -> pd.DataFrame:
        return self._load("dw.dim_city", "city_key", ["city_name", "zone_key"])

    def get_product_keys(self) -> pd.DataFrame:
        return self._load("dw.dim_product", "product_key", ["product_id", "laboratory_key"])

    def get_client_keys(self) -> pd.DataFrame:
        return self._load("dw.dim_client", "client_key", ["client_id", "client_name"])

    def get_zone_keys(self) -> pd.DataFrame:
        return self._load("dw.dim_zone", "zone_key", ["zone_code"])

    def get_distributor_keys(self) -> pd.DataFrame:
        return self._load("dw.dim_distributor", "distributor_key", ["distributor_code"])

    def get_salesperson_keys(self) -> pd.DataFrame:
        return self._load("dw.dim_salesperson", "salesperson_key", ["salesperson_id"])

    def get_date_keys(self) -> pd.DataFrame:
        return self._load("dw.dim_date", "date_key", ["full_date"])

    def invalidate_cache(self, table: Optional[str] = None):
        if table:
            self._cache.pop(table, None)
        else:
            self._cache.clear()


def normalize_for_load(
    df: pd.DataFrame,
    source_system: str,
    resolver: Optional[DimensionResolver] = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Final normalization step: resolve all dimension keys and prepare
    fact_sales-ready DataFrame.

    Returns: (ready_df, rejected_df)
    """
    if resolver is None:
        resolver = DimensionResolver()

    df = df.copy()
    rejected_rows = []

    # ── 1. Date Key ──────────────────────────────────────────────
    date_col = next((c for c in ["sale_date", "full_date"] if c in df.columns), None)
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        df["date_key"] = df[date_col].dt.strftime("%Y%m%d").astype("Int64")
    else:
        logger.error("No date column found in DataFrame")
        return pd.DataFrame(), df

    # ── 2. City Key ──────────────────────────────────────────────
    cities_df = resolver.get_city_keys()
    city_name_col = "city_name_clean" if "city_name_clean" in df.columns else "city_name"
    df = df.merge(
        cities_df[["city_key", "city_name", "zone_key"]].rename(
            columns={"city_name": "_city_name_dim", "zone_key": "_zone_key_from_city"}
        ),
        left_on=city_name_col,
        right_on="_city_name_dim",
        how="left",
    ).drop(columns=["_city_name_dim"], errors="ignore")

    missing_city = df["city_key"].isna().sum()
    if missing_city > 0:
        logger.warning("{} rows with unresolved city_key — defaulting to Santo Domingo", missing_city)
        default_city_key = cities_df[cities_df["city_name"] == "Santo Domingo"]["city_key"].iloc[0]
        df["city_key"] = df["city_key"].fillna(default_city_key)

    # ── 3. Zone Key ──────────────────────────────────────────────
    if "_zone_key_from_city" in df.columns:
        df["zone_key"] = df.get("zone_key", df["_zone_key_from_city"]).fillna(df["_zone_key_from_city"])
        df.drop(columns=["_zone_key_from_city"], inplace=True, errors="ignore")

    if "zone_key" not in df.columns or df["zone_key"].isna().any():
        zones_df = resolver.get_zone_keys()
        zone_code_col = "zone_code_clean" if "zone_code_clean" in df.columns else "zone_code"
        if zone_code_col in df.columns:
            df = df.merge(
                zones_df.rename(columns={"zone_key": "_zone_key2"}),
                left_on=zone_code_col,
                right_on="zone_code",
                how="left",
            )
            df["zone_key"] = df.get("zone_key", pd.Series(dtype="Int64")).fillna(df["_zone_key2"])
            df.drop(columns=["_zone_key2", "zone_code"], errors="ignore", inplace=True)
        default_zone = zones_df[zones_df["zone_code"] == "CAP"]["zone_key"].iloc[0]
        df["zone_key"] = df["zone_key"].fillna(default_zone)

    # ── 4. Product Key + Laboratory Key ──────────────────────────
    products_df = resolver.get_product_keys()
    product_id_col = "sap_product_id" if "sap_product_id" in df.columns else "product_id"

    if product_id_col in df.columns:
        df = df.merge(
            products_df[["product_key", "product_id", "laboratory_key"]],
            left_on=product_id_col,
            right_on="product_id",
            how="left",
        ).drop(columns=["product_id"], errors="ignore")

    if "product_key" not in df.columns or df["product_key"].isna().any():
        logger.warning("{} rows with unresolved product_key", df.get("product_key", pd.Series(dtype="Int64")).isna().sum())
        df["product_key"] = df.get("product_key", pd.Series(dtype="Int64")).fillna(1)

    if "laboratory_key" not in df.columns or df["laboratory_key"].isna().any():
        df["laboratory_key"] = df.get("laboratory_key", pd.Series(dtype="Int64")).fillna(1)

    # ── 5. Distributor Key ───────────────────────────────────────
    distributors_df = resolver.get_distributor_keys()
    dist_code_col = next((c for c in ["distributor_code", "source_system"] if c in df.columns), None)
    if dist_code_col:
        df = df.merge(
            distributors_df[["distributor_key", "distributor_code"]],
            left_on=dist_code_col,
            right_on="distributor_code",
            how="left",
        ).drop(columns=["distributor_code"], errors="ignore")
    default_dist = distributors_df[distributors_df["distributor_code"] == "INT"]["distributor_key"].iloc[0]
    df["distributor_key"] = df.get("distributor_key", pd.Series(dtype="Int64")).fillna(default_dist)

    # ── 6. Client Key ────────────────────────────────────────────
    clients_df = resolver.get_client_keys()
    client_id_col = next((c for c in ["client_id"] if c in df.columns), None)
    if client_id_col:
        df = df.merge(
            clients_df[["client_key", "client_id"]],
            on="client_id",
            how="left",
        )
    # Clients from distributors may not have client_id; use name matching
    if "client_key" not in df.columns or df["client_key"].isna().any():
        default_client = clients_df.iloc[0]["client_key"]
        df["client_key"] = df.get("client_key", pd.Series(dtype="Int64")).fillna(default_client)

    # ── 7. Salesperson Key ───────────────────────────────────────
    sp_df = resolver.get_salesperson_keys()
    sp_id_col = next((c for c in ["salesperson_id"] if c in df.columns), None)
    if sp_id_col:
        df = df.merge(
            sp_df[["salesperson_key", "salesperson_id"]],
            on="salesperson_id",
            how="left",
        )
    default_sp = sp_df.iloc[0]["salesperson_key"]
    df["salesperson_key"] = df.get("salesperson_key", pd.Series(dtype="Int64")).fillna(default_sp)

    # ── 8. Derived Metrics ───────────────────────────────────────
    if "margin_amount" not in df.columns or df["margin_amount"].isna().all():
        if "net_amount" in df.columns and "cost_amount" in df.columns:
            df["margin_amount"] = (df["net_amount"] - df["cost_amount"]).round(2)

    if "margin_pct" not in df.columns or df["margin_pct"].isna().all():
        if "margin_amount" in df.columns and "net_amount" in df.columns:
            df["margin_pct"] = (
                df["margin_amount"] / df["net_amount"].replace(0, float("nan")) * 100
            ).round(3)

    if "gross_amount" not in df.columns:
        df["gross_amount"] = df.get("net_amount", 0)
    if "discount_pct" not in df.columns:
        df["discount_pct"] = 0.0
    if "discount_amount" not in df.columns:
        df["discount_amount"] = 0.0
    if "invoice_line" not in df.columns:
        df["invoice_line"] = 1

    # ── 9. Source Record ID ──────────────────────────────────────
    if "source_record_id" not in df.columns:
        df["source_record_id"] = (
            df.get("invoice_number", "").astype(str) + "-" +
            df.get("invoice_line", 1).astype(str)
        )

    df["source_system"] = source_system
    df["load_timestamp"] = datetime.now(tz=timezone.utc)
    df["is_deleted"] = False

    # ── 10. Select only fact_sales columns ───────────────────────
    fact_cols = [
        "date_key", "product_key", "client_key", "city_key", "zone_key",
        "distributor_key", "laboratory_key", "salesperson_key",
        "invoice_number", "invoice_line",
        "quantity", "unit_price", "gross_amount",
        "discount_pct", "discount_amount", "net_amount",
        "cost_amount", "margin_amount", "margin_pct",
        "source_system", "source_record_id",
        "load_timestamp", "is_deleted",
    ]
    available_cols = [c for c in fact_cols if c in df.columns]
    fact_df = df[available_cols].copy()

    # Final check: drop rows missing critical keys
    critical = ["date_key", "product_key", "client_key", "city_key"]
    missing_critical = fact_df[critical].isna().any(axis=1)
    if missing_critical.any():
        rejected_critical = fact_df[missing_critical].copy()
        rejected_critical["rejection_reason"] = "missing_critical_key"
        rejected_rows.append(rejected_critical)
        fact_df = fact_df[~missing_critical]

    rejected_df = pd.concat(rejected_rows, ignore_index=True) if rejected_rows else pd.DataFrame()

    logger.info(
        "Normalization complete: {} fact rows ready, {} rejected",
        len(fact_df), len(rejected_df),
    )
    return fact_df, rejected_df
