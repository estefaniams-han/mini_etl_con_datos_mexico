import os
import matplotlib.pyplot as plt
import pandas as pd


REPORTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'reports')
os.makedirs(REPORTS_DIR, exist_ok=True)


def _week_order_index(series: pd.Series) -> pd.Series:
    """
    Convierte etiquetas ISO 'YYYY-Www' a un índice numérico ordenado 1..N
    Asume que vienen formadas como '2024-W01', '2024-W02', ..., '2024-W52'
    """
    ordered_weeks = pd.Index(sorted(series.unique()))
    codes = series.map({w: i + 1 for i, w in enumerate(ordered_weeks)})
    return codes



def line_by_week(df, contaminante: str) -> str:
    """
    Línea del valor promedio semanal para un contaminante (p. ej., 'PM2.5').
    Devuelve la ruta del PNG o "" si no hay datos.
    """
    if df.empty: return ""

    sub = df[df["contaminante"].str.contains(contaminante, case=False, na=False)].copy()
    if sub.empty: return ""

    # orden cronológico
    sub["week_order"] = _week_order_index(sub["year_week"])
    sub = sub.sort_values("week_order")

    plt.figure()
    plt.plot(sub["week_order"], sub["valor_promedio"], marker ='o')
    plt.title(f"{contaminante} promedio semanal (CDMX)")
    plt.xlabel("Semana (orden cronológico)")
    plt.ylabel("Concentración promedio")
    plt.xticks(sub["week_order"], sub["year_week"], rotation=45, ha='right')
    plt.tight_layout()


    out = os.path.join(REPORTS_DIR, f"line_{contaminante.replace('.', '').lower()}.png")
    plt.savefig(out, dpi=150)
    plt.close()
    return out


def bar_top_variation(df, topn=5) -> str:
    """
    Barras con los contaminantes de mayor variación total en la ventana analizada.
    Variación = (máximo de 'valor_max') - (mínimo de 'valor_min') a lo largo de todas las semanas.
    Devuelve la ruta del PNG o "" si no hay datos.
    """
    if df.empty: return ""


    agg_max = df.groupby("contaminante", as_index=False)["valor_max"].max().rename(columns={"valor_max": "max_global"})
    agg_min = df.groupby("contaminante", as_index=False)["valor_min"].min().rename(columns={"valor_min": "min_global"})
    agg = pd.merge(agg_max, agg_min, on="contaminante", how="inner")
    agg["variacion"] = agg["max_global"] - agg["min_global"]
    agg = agg.sort_values("variacion", ascending=False).head(topn)

    if agg.empty:
        return ""

    plt.figure()
    plt.bar(agg["contaminante"], agg["variacion"])
    plt.title(f"Top {topn} contaminantes por variación en la ventana")
    plt.xlabel("Contaminante")
    plt.ylabel("Δ (max_global - min_global)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    out = os.path.join(REPORTS_DIR, "bar_top_variation.png")
    plt.savefig(out, dpi=150)
    plt.close()
    return out

