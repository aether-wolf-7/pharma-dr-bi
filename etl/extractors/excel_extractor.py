"""
Pharma DR · Excel Distributor Feed Extractor
==============================================
Adaptive parser for 6 different Excel formats from external distributors.
Each distributor has a unique format profile that determines parsing logic.

Format Registry:
  DIST_A — Standard row-per-line (.xlsx), headers row 1
  DIST_B — Multi-sheet pivot workbook (.xlsx), use "Detalle" sheet
  DIST_C — Company header in rows 1-3, data headers row 4, data from row 5
  DIST_D — Multi-sheet by zone (Zona_CAP, Zona_NOR, etc.)
  DIST_E — Metadata block rows 1-5, headers row 6, data from row 7
  DIST_F — City+Zone combined column, comma decimals, DD/MM/YYYY dates
"""

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger

from etl.config.settings import settings


# ── Format Profile Registry ───────────────────────────────────

FORMAT_PROFILES: Dict[str, dict] = {
    "DIST_A": {
        "name": "Distribuidora Ramos",
        "file_pattern": "DIST_A_*",
        "sheet_name": "Ventas",
        "header_row": 0,       # 0-indexed
        "skip_rows": None,
        "column_map": {
            "Fecha":         "sale_date",
            "Num_Factura":   "invoice_number",
            "Cod_Producto":  "dist_product_code",
            "Descripcion":   "product_name_raw",
            "Cantidad":      "quantity",
            "Precio_Unit":   "unit_price",
            "Descuento_Pct": "discount_pct",
            "Total_Neto":    "net_amount",
            "Cliente":       "client_name_raw",
            "Ciudad":        "city_name_raw",
            "Zona":          "zone_code_raw",
        },
        "date_col":   "sale_date",
        "date_fmt":   None,       # pandas auto-parse
        "decimal_sep": ".",
        "multi_sheet": False,
        "city_zone_combined": False,
    },
    "DIST_B": {
        "name": "MediFar Dominicana",
        "file_pattern": "DIST_B_*",
        "sheet_name": "Detalle",
        "header_row": 0,
        "skip_rows": None,
        "column_map": {
            "FECHA":    "sale_date",
            "FACTURA":  "invoice_number",
            "COD_MF":   "dist_product_code",
            "PRODUCTO": "product_name_raw",
            "CLIENTE":  "client_name_raw",
            "CANT":     "quantity",
            "PRECIO":   "unit_price",
            "IMPORTE":  "net_amount",
            "PROV":     "city_name_raw",
        },
        "date_col": "sale_date",
        "date_fmt": None,
        "decimal_sep": ".",
        "multi_sheet": False,
        "city_zone_combined": False,
    },
    "DIST_C": {
        "name": "Farmacorp",
        "file_pattern": "DIST_C_*",
        "sheet_name": "Hoja1",
        "header_row": 3,       # Headers at row index 3 (0-based = row 4 in Excel)
        "skip_rows": [0, 1, 2],
        "column_map": {
            "fecha":             "sale_date",
            "numero_doc":        "invoice_number",
            "codigo_fc":         "dist_product_code",
            "descripcion_prod":  "product_name_raw",
            "qty":               "quantity",
            "precio_venta":      "unit_price",
            "total":             "net_amount",
            "nombre_cliente":    "client_name_raw",
            "ciudad_cliente":    "city_name_raw",
            "descuento":         "discount_pct_raw",
        },
        "date_col": "sale_date",
        "date_fmt": None,
        "decimal_sep": ".",
        "multi_sheet": False,
        "city_zone_combined": False,
    },
    "DIST_D": {
        "name": "AlphaFarma Group",
        "file_pattern": "DIST_D_*",
        "sheet_name": None,    # Multi-sheet: read all Zona_* sheets
        "header_row": 0,
        "skip_rows": None,
        "column_map": {
            "Fecha":       "sale_date",
            "Factura":     "invoice_number",
            "Codigo":      "dist_product_code",
            "Producto":    "product_name_raw",
            "Categoria":   "category_raw",
            "Cliente":     "client_name_raw",
            "TipoCliente": "client_type_raw",
            "Ciudad":      "city_name_raw",
            "Cantidad":    "quantity",
            "PVP":         "unit_price",
            "Neto":        "net_amount",
        },
        "date_col": "sale_date",
        "date_fmt": None,
        "decimal_sep": ".",
        "multi_sheet": True,
        "zone_sheet_prefix": "Zona_",
        "city_zone_combined": False,
    },
    "DIST_E": {
        "name": "BioPharma Distribution",
        "file_pattern": "DIST_E_*",
        "sheet_name": "ReporteVentas",
        "header_row": 5,       # Headers at row index 5 (row 6 in Excel)
        "skip_rows": [0, 1, 2, 3, 4],
        "column_map": {
            "Fecha_Venta":            "sale_date",
            "ID_Factura":             "invoice_number",
            "CodBP":                  "dist_product_code",
            "Nombre_Medicamento":     "product_name_raw",
            "Unidades":               "quantity",
            "Precio":                 "unit_price",
            "Descto_Pct":             "discount_pct",
            "Importe_Neto":           "net_amount",
            "Razon_Social":           "client_name_raw",
            "Municipio":              "city_name_raw",
            "Lab":                    "lab_code_raw",
            "Categoria_Terapeutica":  "category_raw",
        },
        "date_col": "sale_date",
        "date_fmt": None,
        "decimal_sep": ".",
        "multi_sheet": False,
        "city_zone_combined": False,
    },
    "DIST_F": {
        "name": "MedDist Nacional",
        "file_pattern": "DIST_F_*",
        "sheet_name": "Ventas_MedDist",
        "header_row": 0,
        "skip_rows": None,
        "column_map": {
            "Fecha":         "sale_date",
            "Referencia":    "invoice_number",
            "CodMD":         "dist_product_code",
            "Medicamento":   "product_name_raw",
            "ClienteNombre": "client_name_raw",
            "Ciudad_Zona":   "city_zone_combined",  # Special: split on " / "
            "Unidades":      "quantity",
            "Precio_Unit":   "unit_price",
            "Importe":       "net_amount",
            "Margen_Aprox":  "margin_pct_raw",
        },
        "date_col": "sale_date",
        "date_fmt": "%d/%m/%Y",
        "decimal_sep": ",",
        "multi_sheet": False,
        "city_zone_combined": True,
        "city_zone_col": "city_zone_combined",
        "city_zone_sep": " / ",
    },
}


class ExcelExtractor:
    """
    Adaptive Excel extractor for 6 distributor formats.

    Usage:
        extractor = ExcelExtractor("DIST_A")
        for df in extractor.extract(file_path):
            process(df)
    """

    def __init__(self, distributor_code: str):
        if distributor_code not in FORMAT_PROFILES:
            raise ValueError(f"Unknown distributor: {distributor_code}. Known: {list(FORMAT_PROFILES)}")
        self.distributor_code = distributor_code
        self.profile = FORMAT_PROFILES[distributor_code]
        self.rejected_rows: List[dict] = []

    def extract(self, file_path: str | Path) -> pd.DataFrame:
        """
        Parse Excel file according to distributor format profile.
        Returns normalized DataFrame ready for transformation layer.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Excel file not found: {path}")

        logger.info("Extracting {} ({}) from: {}", self.distributor_code, self.profile["name"], path.name)

        if self.profile.get("multi_sheet"):
            return self._read_multi_sheet(path)
        else:
            return self._read_single_sheet(path)

    def _read_single_sheet(self, path: Path) -> pd.DataFrame:
        """Read a single-sheet (or named sheet) Excel file."""
        profile = self.profile

        read_kwargs = {
            "sheet_name": profile.get("sheet_name", 0),
            "header":     profile.get("header_row", 0),
        }
        if profile.get("skip_rows"):
            # skiprows does not work well with header=N; read raw and slice
            raw = pd.read_excel(path, sheet_name=profile.get("sheet_name", 0),
                                header=None, dtype=str)
            header_idx = profile["header_row"]
            raw.columns = raw.iloc[header_idx]
            df = raw.iloc[header_idx + 1:].copy().reset_index(drop=True)
        else:
            df = pd.read_excel(path, **read_kwargs, dtype=str)

        return self._normalize(df, path.name)

    def _read_multi_sheet(self, path: Path) -> pd.DataFrame:
        """Read all sheets matching zone_sheet_prefix and concatenate."""
        xl = pd.ExcelFile(path)
        prefix = self.profile.get("zone_sheet_prefix", "")
        zone_sheets = [s for s in xl.sheet_names if s.startswith(prefix)]

        if not zone_sheets:
            logger.warning("No zone sheets found in {}", path.name)
            return pd.DataFrame()

        frames = []
        for sheet_name in zone_sheets:
            zone_code = sheet_name.replace(prefix, "")
            df_sheet = pd.read_excel(path, sheet_name=sheet_name, header=0, dtype=str)
            df_sheet["_sheet_zone"] = zone_code
            frames.append(df_sheet)

        df = pd.concat(frames, ignore_index=True)
        return self._normalize(df, path.name)

    def _normalize(self, df: pd.DataFrame, file_name: str) -> pd.DataFrame:
        """Apply column mapping, type coercion, and initial cleaning."""
        profile = self.profile

        # Rename columns using profile map
        col_map = profile.get("column_map", {})
        df.columns = [str(c).strip() for c in df.columns]
        df = df.rename(columns=col_map)

        # Drop rows where all values are null
        df = df.dropna(how="all").reset_index(drop=True)

        # Remove completely empty invoices
        if "invoice_number" in df.columns:
            df = df[df["invoice_number"].notna() & (df["invoice_number"].astype(str).str.strip() != "")]

        # ── Date Parsing ────────────────────────────────────────
        date_col = profile.get("date_col", "sale_date")
        if date_col in df.columns:
            date_fmt = profile.get("date_fmt")
            try:
                if date_fmt:
                    df[date_col] = pd.to_datetime(df[date_col], format=date_fmt, errors="coerce")
                else:
                    df[date_col] = pd.to_datetime(df[date_col], infer_datetime_format=True, errors="coerce")
            except Exception as exc:
                logger.warning("Date parse issue in {}: {}", file_name, exc)
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

        # ── Decimal Separator Fix (DIST_F uses commas) ──────────
        decimal_sep = profile.get("decimal_sep", ".")
        numeric_cols = ["quantity", "unit_price", "net_amount", "gross_amount",
                        "discount_pct", "margin_pct"]
        for col in numeric_cols:
            if col in df.columns:
                if decimal_sep == ",":
                    df[col] = (
                        df[col].astype(str)
                        .str.replace(r"\.", "", regex=True)   # remove thousand sep dot
                        .str.replace(",", ".", regex=False)   # swap comma → dot
                        .str.replace(r"[^\d.\-]", "", regex=True)  # strip non-numeric
                    )
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # ── Discount Pct: strip "%" signs ───────────────────────
        for dcol in ["discount_pct", "discount_pct_raw", "margin_pct_raw"]:
            if dcol in df.columns:
                df[dcol] = (
                    df[dcol].astype(str)
                    .str.replace("%", "", regex=False)
                    .str.replace(",", ".", regex=False)
                    .pipe(pd.to_numeric, errors="coerce")
                    .fillna(0)
                )
                if dcol == "discount_pct_raw":
                    df = df.rename(columns={dcol: "discount_pct"})

        # ── City+Zone Combined Split (DIST_F) ───────────────────
        if profile.get("city_zone_combined") and "city_zone_combined" in df.columns:
            sep = profile.get("city_zone_sep", " / ")
            split = df["city_zone_combined"].astype(str).str.split(sep, n=1, expand=True)
            df["city_name_raw"] = split[0].str.strip()
            df["zone_code_raw"] = split[1].str.strip() if split.shape[1] > 1 else None
            df.drop(columns=["city_zone_combined"], inplace=True)

        # ── Sheet Zone Injection (DIST_D) ────────────────────────
        if "_sheet_zone" in df.columns and "zone_code_raw" not in df.columns:
            df["zone_code_raw"] = df["_sheet_zone"]
            df.drop(columns=["_sheet_zone"], inplace=True)

        # ── Add ETL Metadata ─────────────────────────────────────
        df["distributor_code"] = self.distributor_code
        df["source_system"]    = self.distributor_code
        df["source_file"]      = file_name
        df["extracted_at"]     = datetime.now(tz=timezone.utc)

        # ── Compute missing gross/net if possible ────────────────
        if "net_amount" not in df.columns or df["net_amount"].isna().all():
            if "quantity" in df.columns and "unit_price" in df.columns:
                disc = df.get("discount_pct", pd.Series(0, index=df.index)).fillna(0)
                df["gross_amount"] = (df["quantity"] * df["unit_price"]).round(2)
                df["discount_amount"] = (df["gross_amount"] * disc / 100).round(2)
                df["net_amount"] = (df["gross_amount"] - df["discount_amount"]).round(2)

        # ── Rejection Tracking ───────────────────────────────────
        invalid_dates = df[date_col].isna().sum() if date_col in df.columns else 0
        invalid_amounts = df["net_amount"].isna().sum() if "net_amount" in df.columns else 0
        if invalid_dates > 0:
            logger.warning("{} rows with invalid dates in {}", invalid_dates, file_name)
        if invalid_amounts > 0:
            logger.warning("{} rows with invalid amounts in {}", invalid_amounts, file_name)

        logger.info(
            "{} | {} | {} rows extracted ({} rejected)",
            self.distributor_code, file_name, len(df),
            df[date_col].isna().sum() if date_col in df.columns else 0,
        )
        return df

    @staticmethod
    def scan_landing_directory(
        landing_path: Optional[str] = None,
    ) -> Dict[str, List[Path]]:
        """
        Scan landing directory and group files by distributor code.
        Returns: {"DIST_A": [Path, ...], "DIST_B": [...], ...}
        """
        landing = Path(landing_path or settings.excel_landing_path)
        if not landing.exists():
            logger.warning("Excel landing directory not found: {}", landing)
            return {}

        import fnmatch
        result: Dict[str, List[Path]] = {}
        for dist_code, profile in FORMAT_PROFILES.items():
            pattern = profile["file_pattern"]
            matches = [
                f for f in landing.iterdir()
                if f.is_file() and f.suffix in (".xlsx", ".xls")
                and fnmatch.fnmatch(f.name, pattern)
            ]
            if matches:
                result[dist_code] = sorted(matches)
                logger.debug("{}: {} file(s) found", dist_code, len(matches))

        return result

    @staticmethod
    def move_to_processed(file_path: Path) -> None:
        """Move processed file to processed directory."""
        import shutil
        dest_dir = Path(settings.excel_processed_path)
        dest_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = dest_dir / f"{ts}_{file_path.name}"
        shutil.move(str(file_path), str(dest))
        logger.info("Moved to processed: {}", dest.name)

    @staticmethod
    def move_to_rejected(file_path: Path, reason: str) -> None:
        """Move rejected file to rejected directory with reason log."""
        import shutil
        dest_dir = Path(settings.excel_rejected_path)
        dest_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = dest_dir / f"{ts}_{file_path.name}"
        shutil.move(str(file_path), str(dest))
        reason_file = dest_dir / f"{ts}_{file_path.stem}_rejection_reason.txt"
        reason_file.write_text(reason, encoding="utf-8")
        logger.warning("Moved to rejected: {} — {}", dest.name, reason)
