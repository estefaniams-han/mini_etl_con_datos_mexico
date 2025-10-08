from __future__ import annotations
import os
from datetime import date, timedelta
import pandas as pd


from etl import run_etl
from visualize import line_by_week, bar_top_variation


REPORTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'reports')
os.makedirs(REPORTS_DIR, exist_ok=True)

def summarize(df: pd.DataFrame) -> str:
    """Genera un resumen estadístico del DataFrame."""
    if df.empty:
        return "- No se encontraron datos para los filtros."
    
    latest_week = df["year_week"].max()
    slice_w = df[df["year_week"] == latest_week]

    msg = []
    msg.append(f"- Semana analizada: ***{latest_week}*** ({int(slice_w['mediciones'].sum())} mediciones)")

    # Top 3 semanas-producto por "spread" en la semana (max-min)
    slice_w = slice_w.assign(spread=lambda x: x["valor_max"] - x["valor_min"])
    top_spread = (slice_w.sort_values("spread", ascending=False)
                  .head(3)[["contaminante", "valor_promedio", "valor_min", "valor_max", "spread"]])

    if not top_spread.empty:
        lines = [
            f" - {r.contaminante}: prom {r.valor_promedio:.2f}, "
            f"min {r.valor_min:.2f}, max {r.valor_max:.2f} (Δ {r.spread:.2f})"
            for r in top_spread.itertuples(index=False)
        ]
        msg.append(". Mayor variación intrasemanal (top 3):\n" + "\n".join(lines))
    return "\n".join(msg)

def build_report(df: pd.DataFrame, contaminant_focus: str="PM2.5") -> None:
    """Genera un reporte en Markdown con visualizaciones y estadísticas."""
    img1 = line_by_week(df, contaminant_focus)
    img2 = bar_top_variation(df, topn=5)

    today = date.today()
    last_monday = today - timedelta(days=today.weekday())
    name = f"report_{last_monday.isoformat()}.md"
    path = os.path.join(REPORTS_DIR, name)

    summary = summarize(df)

    md = f"""# DataPulse - Reporte Semanal de Calidad del Aire en CDMX - {last_monday.isoformat()}

**Fuente:** SEDEMA CDMX - datos horarios de contaminantes (CVS anual).
**Filtro de contaminante:** {contaminant_focus}

## Resumen
{summary}

## Tendencial semanal - {contaminant_focus}
{f'![Línea]({os.path.basename(img1)})' if img1 else '_Sin datos para graficar_.'}

## Top variación de contaminantes (ventana analizada)
{f'![Barras]({os.path.basename(img2)})' if img2 else '_Sin datos para graficar_.'}


---
_Generado automáticamente por `src/main.py`._
"""
    
    with open(path, 'w', encoding='utf-8') as f:
        f.write(md)
    print(f"Reporte generado: {path}")
    

if __name__ == "__main__":
    df_weekly = run_etl({
        "year": 2024,
        "days_back": 60,
    })

    build_report(df_weekly, contaminant_focus="PM2.5")
