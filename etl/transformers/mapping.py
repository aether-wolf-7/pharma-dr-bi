"""
Pharma DR · Product Code Mapping Module
=========================================
Maps distributor-specific product codes → SAP master product IDs.
Uses CSV lookup table with fuzzy-match fallback for unknown codes.

Unmapped products are written to audit.unmapped_products for manual review.
"""

from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
from loguru import logger
from rapidfuzz import fuzz, process as rfuzz_process


# ── Mapping Table Loader ──────────────────────────────────────

_mapping_cache: Optional[pd.DataFrame] = None


def load_product_mapping(mapping_path: str = "data/mappings/product_mapping.csv") -> pd.DataFrame:
    """Load product code mapping table (cached after first load)."""
    global _mapping_cache
    if _mapping_cache is not None:
        return _mapping_cache

    path = Path(mapping_path)
    if not path.exists():
        logger.warning("Product mapping file not found: {}. Run generate_data.py first.", path)
        _mapping_cache = pd.DataFrame(
            columns=["distributor_code", "dist_product_code", "dist_product_desc",
                     "sap_product_id", "sap_product_name", "sap_category",
                     "mapping_confidence", "validated_flag"]
        )
        return _mapping_cache

    df = pd.read_csv(path, dtype=str)
    df["mapping_confidence"] = pd.to_numeric(df["mapping_confidence"], errors="coerce").fillna(1.0)
    df["validated_flag"] = df["validated_flag"].str.lower().isin(["true", "1", "yes"])

    # Build lookup index: (distributor_code, dist_product_code) → sap_product_id
    _mapping_cache = df
    logger.info("Product mapping loaded: {} entries", len(df))
    return _mapping_cache


def resolve_product_code(
    distributor_code: str,
    dist_product_code: str,
    dist_product_desc: Optional[str] = None,
    fuzzy_threshold: int = 70,
) -> Tuple[Optional[str], Optional[str], float, bool]:
    """
    Resolve a distributor product code to SAP product ID.

    Returns:
        (sap_product_id, sap_product_name, confidence, is_exact_match)
    """
    mapping_df = load_product_mapping()

    # 1. Exact match: distributor_code + dist_product_code
    exact = mapping_df[
        (mapping_df["distributor_code"] == distributor_code) &
        (mapping_df["dist_product_code"] == str(dist_product_code).strip())
    ]
    if not exact.empty:
        row = exact.iloc[0]
        return row["sap_product_id"], row["sap_product_name"], float(row["mapping_confidence"]), True

    # 2. Fuzzy on product description (if provided)
    if dist_product_desc and len(dist_product_desc.strip()) > 3:
        dist_subset = mapping_df[mapping_df["distributor_code"] == distributor_code].copy()
        if not dist_subset.empty:
            candidates = dist_subset["dist_product_desc"].tolist()
            match = rfuzz_process.extractOne(
                dist_product_desc,
                candidates,
                scorer=fuzz.token_set_ratio,
                score_cutoff=fuzzy_threshold,
            )
            if match:
                matched_desc, score, idx = match
                row = dist_subset.iloc[idx]
                return row["sap_product_id"], row["sap_product_name"], score / 100.0, False

    # 3. No match
    return None, None, 0.0, False


def map_product_codes(
    df: pd.DataFrame,
    distributor_code: str,
    code_col: str = "dist_product_code",
    desc_col: str = "product_name_raw",
    fuzzy_threshold: int = 70,
) -> pd.DataFrame:
    """
    Apply product code mapping to an entire DataFrame.
    Adds columns: sap_product_id, sap_product_name, mapping_confidence, mapping_exact
    """
    mapping_df = load_product_mapping()

    # Build fast dict lookup for this distributor
    dist_mapping = mapping_df[mapping_df["distributor_code"] == distributor_code]
    code_lookup: Dict[str, dict] = {}
    for _, row in dist_mapping.iterrows():
        code_lookup[str(row["dist_product_code"]).strip()] = {
            "sap_product_id":   row["sap_product_id"],
            "sap_product_name": row["sap_product_name"],
            "confidence":       float(row["mapping_confidence"]),
        }

    results = []
    unmapped = []

    for _, row in df.iterrows():
        code = str(row.get(code_col, "")).strip()
        desc = str(row.get(desc_col, "")).strip()

        if code in code_lookup:
            m = code_lookup[code]
            results.append({
                "sap_product_id":      m["sap_product_id"],
                "sap_product_name":    m["sap_product_name"],
                "mapping_confidence":  m["confidence"],
                "mapping_exact":       True,
            })
        else:
            sap_id, sap_name, conf, is_exact = resolve_product_code(
                distributor_code, code, desc, fuzzy_threshold
            )
            if sap_id:
                results.append({
                    "sap_product_id":     sap_id,
                    "sap_product_name":   sap_name,
                    "mapping_confidence": conf,
                    "mapping_exact":      is_exact,
                })
            else:
                results.append({
                    "sap_product_id":     None,
                    "sap_product_name":   desc,
                    "mapping_confidence": 0.0,
                    "mapping_exact":      False,
                })
                unmapped.append({
                    "distributor_code":    distributor_code,
                    "dist_product_code":   code,
                    "product_description": desc,
                })

    mapping_result = pd.DataFrame(results, index=df.index)
    df = pd.concat([df, mapping_result], axis=1)

    if unmapped:
        logger.warning(
            "{} unmapped product codes in {} — writing to audit.unmapped_products",
            len(unmapped), distributor_code,
        )
        _write_unmapped_products(unmapped)

    total = len(df)
    mapped = (df["sap_product_id"].notna()).sum()
    logger.info(
        "Product mapping {}: {}/{} ({:.1f}%) resolved",
        distributor_code, mapped, total, mapped / total * 100 if total else 0,
    )
    return df


def _write_unmapped_products(unmapped: list) -> None:
    """Persist unmapped product codes to PostgreSQL audit table."""
    if not unmapped:
        return
    try:
        import sqlalchemy as sa
        from etl.config.settings import settings

        engine = sa.create_engine(settings.postgres_dsn)
        unmapped_df = pd.DataFrame(unmapped)
        with engine.begin() as conn:
            for _, row in unmapped_df.iterrows():
                conn.execute(
                    sa.text("""
                        INSERT INTO audit.unmapped_products
                            (source_system, distributor_code, product_description, first_seen, occurrence_count)
                        VALUES (:ss, :dc, :pd, NOW(), 1)
                        ON CONFLICT DO NOTHING
                    """),
                    {
                        "ss": row.get("distributor_code", ""),
                        "dc": row.get("dist_product_code", ""),
                        "pd": row.get("product_description", ""),
                    },
                )
    except Exception as exc:
        logger.warning("Could not write unmapped products to DB: {}", exc)
