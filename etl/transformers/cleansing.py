"""
Pharma DR · Data Cleansing Module
===================================
Handles:
  - City name normalization (fuzzy match → canonical DR city names)
  - Product name normalization (fuzzy match → SAP master product names)
  - Client name deduplication (RNC-based + fuzzy name fallback)
  - Numeric field validation (quantity > 0, price > 0, date valid)
  - String standardization (unaccent, strip, title-case)
"""

import re
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from loguru import logger
from rapidfuzz import fuzz, process as rfuzz_process
from unidecode import unidecode


# ── Canonical Reference Lists ─────────────────────────────────

CANONICAL_CITIES: Dict[str, str] = {
    # key: normalized (lowercase, unaccented)   value: official name
    "santo domingo":             "Santo Domingo",
    "sd":                        "Santo Domingo",
    "distrito nacional":         "Santo Domingo",
    "d.n.":                      "Santo Domingo",
    "dn":                        "Santo Domingo",
    "santo domingo este":        "Santo Domingo Este",
    "sde":                       "Santo Domingo Este",
    "santo domingo norte":       "Santo Domingo Norte",
    "sdn":                       "Santo Domingo Norte",
    "santo domingo oeste":       "Santo Domingo Oeste",
    "sdoe":                      "Santo Domingo Oeste",
    "boca chica":                "Boca Chica",
    "santiago":                  "Santiago",
    "stgo":                      "Santiago",
    "la vega":                   "La Vega",
    "vega":                      "La Vega",
    "puerto plata":              "Puerto Plata",
    "pp":                        "Puerto Plata",
    "san francisco de macoris":  "San Francisco de Macorís",
    "san francisco":             "San Francisco de Macorís",
    "sfm":                       "San Francisco de Macorís",
    "moca":                      "Moca",
    "bonao":                     "Bonao",
    "cotui":                     "Cotui",
    "la romana":                 "La Romana",
    "romana":                    "La Romana",
    "san pedro de macoris":      "San Pedro de Macorís",
    "san pedro":                 "San Pedro de Macorís",
    "spm":                       "San Pedro de Macorís",
    "higuey":                    "Higüey",
    "higüey":                    "Higüey",
    "hato mayor":                "Hato Mayor",
    "el seibo":                  "El Seibo",
    "barahona":                  "Barahona",
    "san cristobal":             "San Cristóbal",
    "san cristóbal":             "San Cristóbal",
    "azua":                      "Azua",
    "bani":                      "Bani",
    "bani":                      "Bani",
    "ocoa":                      "Ocoa",
    "monte cristi":              "Monte Cristi",
    "mao":                       "Mao",
    "valverde":                  "Mao",
    "dajabon":                   "Dajabón",
    "dajabón":                   "Dajabón",
    "santiago rodriguez":        "Santiago Rodríguez",
    "nagua":                     "Nagua",
    "samana":                    "Samaná",
    "samaná":                    "Samaná",
    "neiba":                     "Neiba",
    "pedernales":                "Pedernales",
    "guerra":                    "Guerra",
    "los alcarrizos":            "Los Alcarrizos",
}

# Fuzzy-match source for unknown city names
CANONICAL_CITY_LIST: List[str] = sorted(set(CANONICAL_CITIES.values()))

CANONICAL_ZONES: Dict[str, str] = {
    "capital":   "CAP", "cap":    "CAP",
    "norte":     "NOR", "nor":    "NOR",
    "este":      "EST", "est":    "EST",
    "sur":       "SUR",
    "oeste":     "OES", "oes":    "OES",
    "noroeste":  "OES",
    "nordeste":  "NOR",
}

# ── Text Normalization Helpers ────────────────────────────────

def normalize_text(text: str) -> str:
    """Lowercase, strip accents, trim whitespace, remove punctuation."""
    if not isinstance(text, str):
        return ""
    text = text.strip()
    text = unidecode(text).lower()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def title_case_spanish(text: str) -> str:
    """Title-case a string, preserving Spanish prepositions in lowercase."""
    if not isinstance(text, str) or not text.strip():
        return text
    lower_words = {"de", "del", "la", "las", "el", "los", "y", "en", "a", "con", "sin"}
    words = text.strip().lower().split()
    result = []
    for i, word in enumerate(words):
        if i == 0 or word not in lower_words:
            result.append(word.capitalize())
        else:
            result.append(word)
    return " ".join(result)


# ── City Name Cleansing ────────────────────────────────────────

def clean_city_name(
    raw_city: str,
    fuzzy_threshold: int = 75,
) -> Tuple[str, float]:
    """
    Normalize a raw city name to its canonical Dominican Republic form.

    Returns:
        (canonical_name, confidence_score)
        confidence = 1.0 for exact match, <1.0 for fuzzy match
    """
    if not isinstance(raw_city, str) or not raw_city.strip():
        return "Santo Domingo", 0.5  # Default to capital

    normalized = normalize_text(raw_city)

    # 1. Exact lookup
    if normalized in CANONICAL_CITIES:
        return CANONICAL_CITIES[normalized], 1.0

    # 2. Partial exact: check if any key is a substring
    for key, canonical in CANONICAL_CITIES.items():
        if key in normalized or normalized in key:
            return canonical, 0.95

    # 3. Fuzzy match against canonical list
    match = rfuzz_process.extractOne(
        normalized,
        [normalize_text(c) for c in CANONICAL_CITY_LIST],
        scorer=fuzz.token_sort_ratio,
        score_cutoff=fuzzy_threshold,
    )
    if match:
        matched_norm, score, idx = match
        return CANONICAL_CITY_LIST[idx], score / 100.0

    # 4. No match found
    logger.debug("Unknown city: '{}' → defaulting to Santo Domingo", raw_city)
    return "Santo Domingo", 0.3


def clean_city_series(series: pd.Series, fuzzy_threshold: int = 75) -> pd.DataFrame:
    """
    Vectorized city name cleaning for a pandas Series.
    Returns DataFrame with columns: city_canonical, city_confidence.
    """
    results = [clean_city_name(v, fuzzy_threshold) for v in series]
    df = pd.DataFrame(results, columns=["city_canonical", "city_confidence"])
    return df


# ── Zone Code Cleansing ────────────────────────────────────────

def clean_zone_code(raw_zone: str) -> str:
    """Normalize zone code. Falls back to city-based lookup if needed."""
    if not isinstance(raw_zone, str):
        return "CAP"
    norm = normalize_text(raw_zone)
    return CANONICAL_ZONES.get(norm, "CAP")


# ── Product Name Cleansing ────────────────────────────────────

_SAP_PRODUCT_MAP: Optional[Dict[str, str]] = None

def _load_sap_products() -> Dict[str, str]:
    """Lazy-load SAP product names from DB or embedded list."""
    global _SAP_PRODUCT_MAP
    if _SAP_PRODUCT_MAP is not None:
        return _SAP_PRODUCT_MAP

    from pathlib import Path
    mapping_csv = Path("data/mappings/product_mapping.csv")
    if mapping_csv.exists():
        df = pd.read_csv(mapping_csv)
        _SAP_PRODUCT_MAP = dict(
            zip(
                df["sap_product_id"].str.upper(),
                df["sap_product_name"],
            )
        )
    else:
        # Use embedded product list as fallback
        from etl.transformers.normalizer import PRODUCTS_REFERENCE
        _SAP_PRODUCT_MAP = {p["product_id"]: p["product_name"] for p in PRODUCTS_REFERENCE}
    return _SAP_PRODUCT_MAP


def clean_product_name(
    raw_name: str,
    sap_products: Optional[Dict[str, str]] = None,
    fuzzy_threshold: int = 70,
) -> Tuple[str, float, Optional[str]]:
    """
    Normalize a raw product description to canonical SAP product name.

    Returns:
        (canonical_name, confidence, matched_sap_id or None)
    """
    if not isinstance(raw_name, str) or not raw_name.strip():
        return raw_name, 0.0, None

    if sap_products is None:
        sap_products = _load_sap_products()

    normalized = normalize_text(raw_name)
    sap_names_norm = {normalize_text(v): (k, v) for k, v in sap_products.items()}

    # Exact normalized match
    if normalized in sap_names_norm:
        sap_id, canonical = sap_names_norm[normalized]
        return canonical, 1.0, sap_id

    # Fuzzy match
    candidates = list(sap_names_norm.keys())
    match = rfuzz_process.extractOne(
        normalized,
        candidates,
        scorer=fuzz.token_set_ratio,
        score_cutoff=fuzzy_threshold,
    )
    if match:
        matched_norm, score, idx = match
        sap_id, canonical = sap_names_norm[candidates[idx]]
        return canonical, score / 100.0, sap_id

    return raw_name, 0.0, None


# ── Client Name Cleansing ─────────────────────────────────────

def clean_client_name(raw_name: str) -> str:
    """Clean and standardize client/pharmacy names."""
    if not isinstance(raw_name, str):
        return raw_name

    name = raw_name.strip()

    # Remove extra whitespace
    name = re.sub(r"\s+", " ", name)

    # Fix common abbreviations
    replacements = {
        r"\bFcia\.?\b":   "Farmacia",
        r"\bFarm\.?\b":   "Farmacia",
        r"\bDrog\.?\b":   "Droguería",
        r"\bCía\.?\b":    "Compañía",
        r"\bCia\.?\b":    "Compañía",
        r"\bS\.?R\.?L\.?\b": "S.R.L.",
        r"\bS\.?A\.?\b":  "S.A.",
        r"\bHosp\.?\b":   "Hospital",
        r"\bClin\.?\b":   "Clínica",
        r"\bClinica\b":   "Clínica",
    }
    for pattern, replacement in replacements.items():
        name = re.sub(pattern, replacement, name, flags=re.IGNORECASE)

    return name.strip()


def clean_rnc(raw_rnc: str) -> Optional[str]:
    """Validate and normalize Dominican RNC (tax ID)."""
    if not isinstance(raw_rnc, str) or not raw_rnc.strip():
        return None
    # Keep only digits and dashes
    cleaned = re.sub(r"[^\d\-]", "", raw_rnc.strip())
    # RNC format: 1-01-12345-6 (11 digits with dashes) or 9 digits plain
    digits_only = re.sub(r"\D", "", cleaned)
    if len(digits_only) in (9, 11):
        return cleaned
    return None


# ── Numeric Field Validation ──────────────────────────────────

def validate_numeric_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply business rules to numeric columns:
      - quantity must be > 0
      - unit_price must be > 0
      - net_amount must be > 0
      - discount_pct must be 0–100
      - margin_pct must be -100 to 200
    """
    issues = []

    if "quantity" in df.columns:
        invalid = df["quantity"] <= 0
        issues.append(f"{invalid.sum()} rows with quantity ≤ 0")
        df = df[~invalid]

    if "unit_price" in df.columns:
        invalid = df["unit_price"] <= 0
        issues.append(f"{invalid.sum()} rows with unit_price ≤ 0")
        df = df[~invalid]

    if "net_amount" in df.columns:
        invalid = df["net_amount"] < 0
        issues.append(f"{invalid.sum()} rows with net_amount < 0")
        df.loc[invalid, "net_amount"] = df.loc[invalid, "gross_amount"] if "gross_amount" in df else np.nan

    if "discount_pct" in df.columns:
        df["discount_pct"] = df["discount_pct"].clip(0, 100)

    if "margin_pct" in df.columns:
        df["margin_pct"] = df["margin_pct"].clip(-100, 200)

    for issue in issues:
        if not issue.startswith("0 "):
            logger.warning("Numeric validation: {}", issue)

    return df


# ── Date Validation ───────────────────────────────────────────

def validate_dates(
    df: pd.DataFrame,
    date_col: str = "sale_date",
    min_date: str = "2015-01-01",
    max_date: Optional[str] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split DataFrame into valid and rejected based on date range.
    Returns (valid_df, rejected_df).
    """
    import pandas as pd
    from datetime import date

    min_dt = pd.Timestamp(min_date)
    max_dt = pd.Timestamp(max_date) if max_date else pd.Timestamp("today")

    if date_col not in df.columns:
        return df, pd.DataFrame()

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    valid_mask = (
        df[date_col].notna() &
        (df[date_col] >= min_dt) &
        (df[date_col] <= max_dt)
    )
    valid_df    = df[valid_mask].copy()
    rejected_df = df[~valid_mask].copy()
    rejected_df["rejection_reason"] = "invalid_date"

    if len(rejected_df) > 0:
        logger.warning("{} rows rejected: invalid/out-of-range dates", len(rejected_df))

    return valid_df, rejected_df


# ── Deduplication ─────────────────────────────────────────────

def deduplicate_invoices(
    df: pd.DataFrame,
    key_cols: List[str] = ["source_system", "invoice_number", "invoice_line"],
) -> Tuple[pd.DataFrame, int]:
    """
    Remove duplicate invoice lines based on composite key.
    Keeps the last occurrence (most recent load).
    Returns (deduplicated_df, n_duplicates_removed).
    """
    present_keys = [c for c in key_cols if c in df.columns]
    if not present_keys:
        return df, 0

    n_before = len(df)
    df = df.drop_duplicates(subset=present_keys, keep="last")
    n_removed = n_before - len(df)
    if n_removed > 0:
        logger.info("Deduplication: {} duplicate rows removed", n_removed)
    return df, n_removed


# ── Full Cleansing Pipeline ────────────────────────────────────

def cleanse_dataframe(
    df: pd.DataFrame,
    source_system: str = "UNKNOWN",
    fuzzy_city_threshold: int = 75,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Run full cleansing pipeline on a raw extracted DataFrame.

    Steps:
      1. Date validation → split valid/rejected
      2. Numeric validation
      3. City name normalization
      4. Client name cleaning
      5. Deduplication
      6. Zone code inference

    Returns: (clean_df, rejected_df)
    """
    logger.info("Cleansing {} rows from {}", len(df), source_system)

    # Step 1: Date validation
    date_col = next((c for c in ["sale_date", "full_date"] if c in df.columns), None)
    if date_col:
        df, rejected = validate_dates(df, date_col=date_col)
    else:
        rejected = pd.DataFrame()

    # Step 2: Numeric validation
    df = validate_numeric_fields(df)

    # Step 3: City normalization
    city_col = next((c for c in ["city_name_raw", "city_name", "CiudadNombre"] if c in df.columns), None)
    if city_col:
        city_results = clean_city_series(df[city_col], fuzzy_city_threshold)
        df["city_name_clean"]      = city_results["city_canonical"]
        df["city_match_confidence"] = city_results["city_confidence"]
    else:
        df["city_name_clean"] = "Santo Domingo"
        df["city_match_confidence"] = 0.5

    # Step 4: Client name cleaning
    client_col = next((c for c in ["client_name_raw", "client_name", "ClienteNombre"] if c in df.columns), None)
    if client_col:
        df["client_name_clean"] = df[client_col].apply(clean_client_name)
    
    # Step 5: Zone code inference from city
    zone_col = next((c for c in ["zone_code_raw", "zone_code"] if c in df.columns), None)
    if zone_col:
        df["zone_code_clean"] = df[zone_col].apply(clean_zone_code)
    else:
        # Map city → zone
        city_zone_map = {
            "Santo Domingo": "CAP", "Santo Domingo Este": "CAP",
            "Santo Domingo Norte": "CAP", "Santo Domingo Oeste": "CAP",
            "Boca Chica": "CAP", "Guerra": "CAP", "Los Alcarrizos": "CAP",
            "Santiago": "NOR", "La Vega": "NOR", "Puerto Plata": "NOR",
            "San Francisco de Macorís": "NOR", "Moca": "NOR",
            "Bonao": "NOR", "Cotui": "NOR", "Nagua": "NOR", "Samaná": "NOR",
            "La Romana": "EST", "San Pedro de Macorís": "EST",
            "Higüey": "EST", "Hato Mayor": "EST", "El Seibo": "EST",
            "Barahona": "SUR", "San Cristóbal": "SUR", "Azua": "SUR",
            "Bani": "SUR", "Ocoa": "SUR", "Neiba": "SUR", "Pedernales": "SUR",
            "Monte Cristi": "OES", "Mao": "OES", "Dajabón": "OES",
            "Santiago Rodríguez": "OES",
        }
        df["zone_code_clean"] = df["city_name_clean"].map(city_zone_map).fillna("CAP")

    # Step 6: Deduplication
    df, _ = deduplicate_invoices(df)

    logger.info("Cleansing complete: {} clean rows, {} rejected", len(df), len(rejected))
    return df, rejected
