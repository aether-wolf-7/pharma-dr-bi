"""
Pharma DR · Unit Tests — Cleansing Module
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
import pandas as pd
from etl.transformers.cleansing import (
    clean_city_name,
    clean_zone_code,
    clean_client_name,
    clean_rnc,
    normalize_text,
    deduplicate_invoices,
    validate_numeric_fields,
    validate_dates,
)


class TestCityNormalization:
    def test_exact_match(self):
        name, conf = clean_city_name("Santo Domingo")
        assert name == "Santo Domingo"
        assert conf == 1.0

    def test_case_insensitive(self):
        name, conf = clean_city_name("SANTO DOMINGO")
        assert name == "Santo Domingo"

    def test_abbreviation(self):
        name, conf = clean_city_name("SD")
        assert name == "Santo Domingo"

    def test_accent_insensitive(self):
        name, conf = clean_city_name("San Cristobal")
        assert name == "San Cristóbal"

    def test_fuzzy_match(self):
        name, conf = clean_city_name("Sntiago")   # typo
        assert name == "Santiago"
        assert conf > 0.7

    def test_sfm_abbreviation(self):
        name, conf = clean_city_name("SFM")
        assert name == "San Francisco de Macorís"

    def test_unknown_city_defaults(self):
        name, conf = clean_city_name("Ciudad Desconocida XYZ")
        assert isinstance(name, str)
        assert conf < 0.5


class TestZoneNormalization:
    def test_capital(self):
        assert clean_zone_code("Capital") == "CAP"
        assert clean_zone_code("CAP") == "CAP"

    def test_north(self):
        assert clean_zone_code("Norte") == "NOR"

    def test_east(self):
        assert clean_zone_code("Este") == "EST"

    def test_unknown_defaults_to_cap(self):
        assert clean_zone_code("") == "CAP"
        assert clean_zone_code(None) == "CAP"


class TestClientCleansing:
    def test_abbreviation_expansion(self):
        result = clean_client_name("Fcia. San Juan")
        assert "Farmacia" in result

    def test_whitespace_normalization(self):
        result = clean_client_name("  Farmacia   Central  ")
        assert result == "Farmacia Central"

    def test_company_suffix(self):
        result = clean_client_name("Distribuidora S.R.L.")
        assert "S.R.L." in result

    def test_empty_string(self):
        result = clean_client_name("")
        assert result == ""


class TestRncValidation:
    def test_valid_rnc(self):
        result = clean_rnc("1-01-12345-6")
        assert result == "1-01-12345-6"

    def test_with_spaces(self):
        result = clean_rnc("1 01 12345 6")
        assert result is not None

    def test_too_short(self):
        result = clean_rnc("12345")
        assert result is None

    def test_none_input(self):
        result = clean_rnc(None)
        assert result is None


class TestNumericValidation:
    def test_removes_negative_quantity(self):
        df = pd.DataFrame({"quantity": [5, -1, 10, 0], "net_amount": [100, 20, 200, 0]})
        result = validate_numeric_fields(df)
        assert (result["quantity"] > 0).all()

    def test_clips_discount_pct(self):
        df = pd.DataFrame({"discount_pct": [0, 50, 150, -5]})
        result = validate_numeric_fields(df)
        assert result["discount_pct"].max() <= 100
        assert result["discount_pct"].min() >= 0


class TestDateValidation:
    def test_valid_dates_pass(self):
        df = pd.DataFrame({"sale_date": ["2023-01-15", "2024-06-30"]})
        valid, rejected = validate_dates(df, "sale_date")
        assert len(valid) == 2
        assert len(rejected) == 0

    def test_future_dates_rejected(self):
        df = pd.DataFrame({"sale_date": ["2023-01-15", "2099-01-01"]})
        valid, rejected = validate_dates(df, "sale_date")
        assert len(rejected) == 1

    def test_invalid_dates_rejected(self):
        df = pd.DataFrame({"sale_date": ["2023-01-15", "not-a-date", None]})
        valid, rejected = validate_dates(df, "sale_date")
        assert len(valid) == 1


class TestDeduplication:
    def test_removes_duplicates(self):
        df = pd.DataFrame({
            "source_system":   ["SAP_HANA", "SAP_HANA", "SAP_HANA"],
            "invoice_number":  ["FAC-001",  "FAC-001",  "FAC-002"],
            "invoice_line":    [1, 1, 1],
            "net_amount":      [100, 100, 200],
        })
        result, n_removed = deduplicate_invoices(df)
        assert n_removed == 1
        assert len(result) == 2

    def test_no_duplicates(self):
        df = pd.DataFrame({
            "source_system":  ["SAP", "SAP"],
            "invoice_number": ["FAC-001", "FAC-002"],
            "invoice_line":   [1, 1],
        })
        result, n_removed = deduplicate_invoices(df)
        assert n_removed == 0
        assert len(result) == 2
