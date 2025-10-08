from __future__ import annotations
from threading import local
import os, io, zipfile
from datetime import datetime, timedelta
import pandas as pd
import requests
import re


DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
RAW_DIR = os.path.join(DATA_DIR, 'raw')
PROC_DIR = os.path.join(DATA_DIR, 'processed')
os.makedirs(RAW_DIR, exist_ok=True); os.makedirs(PROC_DIR, exist_ok=True)

DEFAULT_CONFIG = {
    "year": 2024,
    "days_back": 60,
    "csv_url": "https://aire.cdmx.gob.mx/descargas/Opendata/anuales_horarios/contaminantes_{year}.csv"
}


def run_etl(cfg: dict | None = None):
    """
    Punto de entrada del pipeline ETL.
    Mezcla la config por defecto con la que pases y ejecuta la extracción + transform.
    """
    cfg = {**DEFAULT_CONFIG, **(cfg or {})}
    return extract_cdmx_csv(cfg)


EXPECTED_HEADER = {"date", "id_station", "id_parameter", "value", "unit"}

def _sniff_header_and_sep(path: str) -> tuple[int, str]:
    """
    Busca la PRIMERA línea que contenga TODAS las columnas esperadas
    y detecta el separador (',', ';' o '\\t'). Devuelve (header_row, sep).
    """
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f):
            low = line.strip().lower()

            for sep in [",", ";", "\t"]:
                parts = [p.strip().strip('"').strip() for p in low.split(sep)]
                if EXPECTED_HEADER.issubset(set(parts)):
                    return i, sep

            # fallback por espacios múltiples
            parts = re.split(r"\s+", low)
            if EXPECTED_HEADER.issubset(set(parts)):
                return i, r"\s+"

    return -1, ","  # no encontrada

def extract_cdmx_csv(cfg: dict) -> pd.DataFrame:
    """
    Lee el archivo contaminantes_2024.csv (descargado de SEDEMA),
    limpia los metadatos iniciales y genera un resumen semanal.
    """
    local = os.path.join(RAW_DIR, f"contaminantes_{cfg['year']}.csv")
    if not os.path.exists(local):
        raise FileNotFoundError(f"No existe {local}. Descárgalo primero en data/raw/")
    
   # --- 1) detectar fila de cabecera real + separador ---
    header_row, sep = _sniff_header_and_sep(local)
    if header_row < 0:
        # ayuda para depurar si no encontró la cabecera
        with open(local, "r", encoding="utf-8", errors="ignore") as f:
            preview = [next(f, "").strip() for _ in range(8)]
        raise RuntimeError(
            "No pude localizar la cabecera con columnas "
            "['date','id_station','id_parameter','value','unit'].\n"
            "Primeras líneas del archivo:\n- " + "\n- ".join(preview)
        )

    # --- 2) leer desde la cabecera real ---
    df = pd.read_csv(
        local,
        engine="python",
        sep=sep,
        skiprows=header_row,  # salta metadatos
        header=0,             # usa esa línea como header
        on_bad_lines="skip"
    )
    df.columns = [c.strip().lower() for c in df.columns]

    expected = ["date", "id_station", "id_parameter", "value", "unit"]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise KeyError(f"Columnas faltantes: {missing}")

    # --- 3) limpiar y renombrar ---
    df = df.rename(columns={
        "date": "fecha_hora",
        "id_station": "estacion",
        "id_parameter": "contaminante",
        "value": "valor",
        "unit": "unidad"
    })

    # --- 4) parsear fecha y valor ---
    df["fecha_hora"] = pd.to_datetime(df["fecha_hora"], errors="coerce")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")
    df = df.dropna(subset=["fecha_hora", "valor"])

    # --- 5) ventana temporal ---
    to_date = df["fecha_hora"].max().normalize()
    from_date = to_date - timedelta(days=cfg.get("days_back", 60))
    df = df[(df["fecha_hora"] >= from_date) & (df["fecha_hora"] <= to_date)]

    # --- 6) semana ISO + agregación ---
    df["year_week"] = df["fecha_hora"].dt.strftime("%G-W%V")
    weekly = (
        df.groupby(["year_week", "contaminante"], as_index=False)
          .agg(valor_promedio=("valor", "mean"),
               valor_min=("valor", "min"),
               valor_max=("valor", "max"),
               mediciones=("valor", "count"))
          .sort_values(["year_week", "contaminante"])
    )

    # --- 7) guardar ---
    out = os.path.join(PROC_DIR, f"cdmx_air_weekly_{cfg['year']}.csv")
    weekly.to_csv(out, index=False)
    print(f"✅ Archivo procesado guardado en {out}")
    return weekly