# 🐍 DataPulse: Mexico Air Quality Mini-ETL

**DataPulse** is a hands-on data engineering project built to practice real ETL workflows using open environmental datasets from Mexico. It extracts hourly air quality measurements from the official SEDEMA dataset, transforms and aggregates them into weekly summaries, and generates automatic reports with plots and key insights.

---

## 🌎 Project Overview

This project demonstrates how to design a lightweight, reproducible ETL pipeline in **Python** — focused on data collection, cleaning, transformation, and visualization. It uses **real data** published by SEDEMA (Mexico City Air Quality Monitoring System – SIMAT).

Each execution:

1. Loads the official CSV dataset (hourly pollutants data).
2. Cleans metadata headers and inconsistent columns.
3. Aggregates all records by ISO week and pollutant.
4. Generates a weekly Markdown report with charts.

> 💡 The goal: automate real-world data processing and reporting using open data from Mexico.

---

## ⚙️ Tech Stack

| Category    | Tools                                                                                                               |
| ----------- | ------------------------------------------------------------------------------------------------------------------- |
| Language    | Python 3.12                                                                                                         |
| Libraries   | `pandas`, `matplotlib`, `requests`, `certifi`                                                                       |
| Data Source | [SEDEMA Air Quality – Hourly Pollutants (Open Data)](https://aire.cdmx.gob.mx/descargas/Opendata/anuales_horarios/) |
| Output      | CSV summaries + Markdown reports with plots                                                                         |

---

## 📂 Repository Structure

```
datapulse/
 ├── src/
 │   ├── main.py          # Orchestrates the ETL & report generation
 │   ├── etl.py           # Extraction, cleaning & weekly aggregation
 │   └── visualize.py     # Plotting utilities (matplotlib)
 ├── data/
 │   ├── raw/             # Raw downloaded datasets
 │   └── processed/       # Cleaned and aggregated CSV outputs
 ├── reports/             # Weekly Markdown reports + charts
 ├── .gitignore
 ├── README.md
 └── requirements.txt
```

---

## 🚀 How to Run

1. **Clone the repo:**

   ```bash
   git clone https://github.com/yourusername/mini_etl_con_datos_mexico.git
   cd mini_etl_con_datos_mexico
   ```

2. **Set up a virtual environment:**

   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   ```

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Run the ETL:**

   ```bash
   python src/main.py
   ```

   The script will process the latest dataset (or your local copy) and create:

   * A cleaned weekly summary under `data/processed/`
   * A Markdown report with plots under `reports/`

---

## 📊 Example Output

```csv
year_week,contaminante,valor_promedio,valor_min,valor_max,mediciones
2024-W10,PM10,46.2,3.0,180.0,2200
2024-W10,PM2.5,22.5,1.0,74.0,2300
2024-W10,O3,27.1,0.0,117.0,4500
```

Each row represents one pollutant’s average concentration for an ISO week. Markdown reports include weekly summaries and matplotlib visualizations.

---

## 💡 Key Learnings

* Building modular ETL pipelines (extract → transform → load → visualize)
* Handling messy real-world CSVs (metadata headers, encoding, separators)
* Time-based aggregation with Pandas (daily/weekly summaries)
* Automating simple analytical reports


---

## 👩‍💻 Author

**Estefanía Marmolejo Sandoval** – *AI Engineer & Data Enthusiast*
Passionate about applied AI, data engineering, and open-source analytics.
💼 [LinkedIn](https://www.linkedin.com/in/estefania-marmolejo/) • 🐙 [GitHub](https://github.com/estefaniams-han)

---

## 📜 License

This project is open-source under the [MIT License](LICENSE).

> ⚠️ *Data courtesy of SEDEMA (Mexico City’s Air Quality Monitoring System – SIMAT). Processed for educational purposes only.*


