# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1: Sourcing and Building the Database
# MAGIC
# MAGIC **Learning Objectives:** By the end of this notebook you will be able to:
# MAGIC 1. Load a versioned CSV snapshot of official statistics from GitHub
# MAGIC 2. Understand how the Eurostat API describes a statistical series (dataset plus filters)
# MAGIC 3. Save raw data as a Delta table, then reshape it with SQL into an analysis table
# MAGIC 4. Store the "rules" of an analysis (who governed when) as a table of its own
# MAGIC
# MAGIC **Series Overview:** This is the first of two notebooks on Spain's economy by prime minister. A viral social media post compared Aznar, Zapatero, Rajoy and Sánchez on GDP, the minimum wage, house prices, jobs and debt. Every number in it was true. In Notebook 2 we test whether its conclusions survive once growth is annualized, adjusted for inflation, measured per person, and compared with the rest of the euro area.
# MAGIC
# MAGIC The same analysis exists as a Jupyter notebook and as an Excel workbook. The last section of Notebook 2 compares the three.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Background: where official statistics come from
# MAGIC
# MAGIC **Eurostat** is the statistical office of the European Union. It publishes harmonized data for every member state, so Spain's unemployment rate is measured the same way as Germany's. Eurostat offers a free **API**: a web address that returns data instead of a web page.
# MAGIC
# MAGIC Each series is identified by a **dataset code** and a set of **filters**. For example:
# MAGIC
# MAGIC | Piece | Meaning |
# MAGIC |---|---|
# MAGIC | `nama_10_gdp` | National accounts: annual GDP and its components |
# MAGIC | `geo=ES` | Spain (`EA20` is the euro area, `PT` Portugal, `IT` Italy) |
# MAGIC | `unit=CLV20_MEUR` | Chain-linked volumes in millions of 2020 euros, i.e. **real** GDP |
# MAGIC | `unit=CP_MEUR` | Current prices in millions of euros, i.e. **nominal** GDP |
# MAGIC | `na_item=B1GQ` | Gross domestic product at market prices |
# MAGIC
# MAGIC Databricks Free Edition only allows internet access to a list of trusted sites, and Eurostat may not be on it. So, as in the Fantasy Premier League series, we load a **snapshot** saved as a CSV file on GitHub. Part 5 shows how to try the live Eurostat API, and what to do if your workspace blocks it.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: Setting Up

# COMMAND ----------

import pandas as pd

print("pandas version:", pd.__version__)
print("Spark version: ", spark.version)

# COMMAND ----------

# Where the snapshot lives. If you fork this repository, change the user name.
SNAPSHOT_URL = "https://raw.githubusercontent.com/ericvd-ucb/Spain-Economy-Databricks/main/data/eurostat_spain_snapshot.csv"

# The twelve series we use. Each row: short code, Eurostat dataset, filters, description.
SERIES = [
    ("rgdp",   "nama_10_gdp",    "geo=ES&unit=CLV20_MEUR&na_item=B1GQ",          "Real GDP, Spain (EUR m, 2020 prices)"),
    ("ngdp",   "nama_10_gdp",    "geo=ES&unit=CP_MEUR&na_item=B1GQ",             "Nominal GDP, Spain (EUR m)"),
    ("eagdp",  "nama_10_gdp",    "geo=EA20&unit=CLV20_MEUR&na_item=B1GQ",        "Real GDP, euro area (EUR m, 2020 prices)"),
    ("gdppc",  "nama_10_pc",     "geo=ES&unit=CLV20_EUR_HAB&na_item=B1GQ",       "Real GDP per person, Spain (EUR, 2020 prices)"),
    ("emp",    "nama_10_pe",     "geo=ES&unit=THS_PER&na_item=EMP_DC",           "Employment, Spain (thousands)"),
    ("une",    "lfsa_urgan",     "geo=ES&age=Y15-74&sex=T&citizen=TOTAL&unit=PC","Unemployment rate, Spain (%)"),
    ("eaune",  "une_rt_a",       "geo=EA20&age=Y15-74&sex=T&unit=PC_ACT",        "Unemployment rate, euro area (%)"),
    ("debt",   "gov_10dd_edpt1", "geo=ES&na_item=GD&sector=S13&unit=PC_GDP",     "Government debt, Spain (% of GDP)"),
    ("hicp",   "prc_hicp_aind",  "geo=ES&coicop=CP00&unit=INX_A_AVG",            "Consumer prices (HICP), Spain, 2015=100"),
    ("eahicp", "prc_hicp_aind",  "geo=EA20&coicop=CP00&unit=INX_A_AVG",          "Consumer prices (HICP), euro area, 2015=100"),
    ("hpi",    "prc_hpi_a",      "geo=ES&purchase=TOTAL&unit=I15_A_AVG",         "House price index, Spain, 2015=100"),
    ("mw",     "earn_mw_cur",    "geo=ES&currency=EUR",                          "Minimum wage, Spain (EUR per month, half-yearly)"),
]
series_catalog = pd.DataFrame(SERIES, columns=["series", "dataset", "filters", "description"])
series_catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: Peeking at the Raw Data
# MAGIC
# MAGIC The snapshot is in **long** format: one row per series per period. This is how data usually arrives from an API, and it is easy to store because adding a new series only adds rows, never columns.

# COMMAND ----------

raw = pd.read_csv(SNAPSHOT_URL, dtype={"period": str})
print(f"{len(raw):,} rows, columns: {list(raw.columns)}")
raw.head(10)

# COMMAND ----------

# How many observations per series, and what period each covers
raw.groupby("series")["period"].agg(["count", "min", "max"])

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that the minimum wage (`mw`) has periods like `2019-S1` and `2019-S2`. Eurostat reports it twice a year, for January and July. Every other series is annual. We deal with that difference in Part 4.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Saving the Raw Table (the "bronze" layer)
# MAGIC
# MAGIC Data engineers often keep data in layers:
# MAGIC
# MAGIC | Layer | Our table | What it holds |
# MAGIC |---|---|---|
# MAGIC | Bronze | `es_eurostat_raw` | Data exactly as it arrived, plus where and when it came from |
# MAGIC | Silver | `es_annual` | Cleaned and reshaped: one row per year, one column per series |
# MAGIC | Reference | `es_terms` | The prime ministers and the years of each term |
# MAGIC
# MAGIC Keeping the raw layer means we can always rebuild the clean one, and we can check any number back to its source. All table names start with `es_` so they do not collide with tables from other courses in your workspace.

# COMMAND ----------

raw_out = raw.merge(series_catalog[["series", "filters", "description"]], on="series", how="left")
raw_out["source"] = "github_snapshot"
raw_out["loaded_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None)

(spark.createDataFrame(raw_out)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("es_eurostat_raw"))

print("Saved es_eurostat_raw:", spark.table("es_eurostat_raw").count(), "rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4: Reshaping with SQL (the "silver" layer)
# MAGIC
# MAGIC Now we turn the long table into a **wide** one: one row per year, one column per series. The pattern `MAX(CASE WHEN series = 'x' THEN value END)` picks out each series in turn; `GROUP BY year` collapses them into one row.
# MAGIC
# MAGIC The minimum wage needs one extra step. We average the January and July values, but only when both exist, so a year with only one observation does not look complete. We then add three derived columns:
# MAGIC
# MAGIC - **Real minimum wage**: the minimum wage divided by the price index, in 2015 euros
# MAGIC - **Real house prices**: the house price index divided by the consumer price index
# MAGIC - **GDP indexes**: real GDP for Spain and the euro area, both set to 100 in 1995

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE es_annual AS
# MAGIC WITH wide AS (
# MAGIC   SELECT
# MAGIC     CAST(substr(period, 1, 4) AS INT)                     AS year,
# MAGIC     MAX(CASE WHEN series = 'rgdp'   THEN value END)       AS real_gdp,
# MAGIC     MAX(CASE WHEN series = 'ngdp'   THEN value END)       AS nominal_gdp,
# MAGIC     MAX(CASE WHEN series = 'eagdp'  THEN value END)       AS ea_real_gdp,
# MAGIC     MAX(CASE WHEN series = 'gdppc'  THEN value END)       AS real_gdp_per_person,
# MAGIC     MAX(CASE WHEN series = 'emp'    THEN value END)       AS employment_thousands,
# MAGIC     MAX(CASE WHEN series = 'une'    THEN value END)       AS unemployment_rate,
# MAGIC     MAX(CASE WHEN series = 'eaune'  THEN value END)       AS ea_unemployment_rate,
# MAGIC     MAX(CASE WHEN series = 'debt'   THEN value END)       AS debt_pct_gdp,
# MAGIC     MAX(CASE WHEN series = 'hicp'   THEN value END)       AS hicp,
# MAGIC     MAX(CASE WHEN series = 'eahicp' THEN value END)       AS ea_hicp,
# MAGIC     MAX(CASE WHEN series = 'hpi'    THEN value END)       AS house_price_index,
# MAGIC     CASE WHEN COUNT(CASE WHEN series = 'mw' THEN 1 END) = 2
# MAGIC          THEN AVG(CASE WHEN series = 'mw' THEN value END) END AS min_wage
# MAGIC   FROM es_eurostat_raw
# MAGIC   WHERE CAST(substr(period, 1, 4) AS INT) BETWEEN 1995 AND 2026
# MAGIC   GROUP BY CAST(substr(period, 1, 4) AS INT)
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   min_wage / hicp * 100                                          AS real_min_wage,
# MAGIC   house_price_index / hicp * 100                                 AS real_house_price_index,
# MAGIC   real_gdp    / FIRST_VALUE(real_gdp)    OVER (ORDER BY year) * 100 AS gdp_index,
# MAGIC   ea_real_gdp / FIRST_VALUE(ea_real_gdp) OVER (ORDER BY year) * 100 AS ea_gdp_index
# MAGIC FROM wide
# MAGIC ORDER BY year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, real_gdp, nominal_gdp, unemployment_rate, min_wage, real_min_wage, house_price_index
# MAGIC FROM es_annual
# MAGIC ORDER BY year

# COMMAND ----------

# MAGIC %md
# MAGIC **Check your understanding.** Real GDP and nominal GDP are exactly equal in one year. Which year, and why? (Hint: look at the unit code `CLV20_MEUR`.)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5: The Terms Table
# MAGIC
# MAGIC Which years "belong" to which prime minister is a choice, not a fact, so we store it as data where everyone can see it and change it. By default a term runs from the year a prime minister took office to the year he left. Pedro Sánchez is still in office, so his term ends in the latest year with data. Notebook 2 lets you change the baseline and see which conclusions survive.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE es_terms AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'José María Aznar',             'PP',   'May 1996 to Apr 2004', 1996, 2004),
# MAGIC   (2, 'José Luis Rodríguez Zapatero', 'PSOE', 'Apr 2004 to Dec 2011', 2004, 2011),
# MAGIC   (3, 'Mariano Rajoy',                'PP',   'Dec 2011 to Jun 2018', 2011, 2018),
# MAGIC   (4, 'Pedro Sánchez',                'PSOE', 'Jun 2018 to present',  2018, 2025)
# MAGIC AS t(term_order, prime_minister, party, in_office, start_year, end_year);
# MAGIC
# MAGIC SELECT * FROM es_terms ORDER BY term_order

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 6 (optional): Try the Live Eurostat API
# MAGIC
# MAGIC The snapshot was taken on 22 November 2026. To get the newest numbers, the cell below asks Eurostat directly. Eurostat returns **JSON-stat**: the numbers sit in a list numbered 0, 1, 2, and a separate index says which year each number belongs to. The function joins the two.
# MAGIC
# MAGIC If your workspace blocks Eurostat, you will see a connection error. That is the Free Edition network restriction, not a mistake in your code. You can keep using the snapshot, or run this same cell on your own computer (or in Colab), save the result as a CSV, and upload it to Databricks.

# COMMAND ----------

import requests

EUROSTAT = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"

def fetch_eurostat(dataset, filters):
    """Download one Eurostat series and return a DataFrame with columns period and value."""
    r = requests.get(f"{EUROSTAT}{dataset}?{filters}", timeout=30)
    r.raise_for_status()
    j = r.json()
    positions = j["dimension"]["time"]["category"]["index"]   # e.g. {"1995": 0, "1996": 1, ...}
    values = j["value"]                                       # e.g. {"0": 763631.0, ...}
    rows = [(period, values.get(str(pos))) for period, pos in positions.items()]
    return pd.DataFrame(rows, columns=["period", "value"]).dropna()

try:
    test = fetch_eurostat("nama_10_gdp", "geo=ES&unit=CLV20_MEUR&na_item=B1GQ")
    print("Eurostat is reachable. Latest real GDP observations:")
    display(test.tail(5))
    EUROSTAT_OK = True
except Exception as e:
    print("Could not reach Eurostat from this workspace:", type(e).__name__)
    print("Keep using the GitHub snapshot.")
    EUROSTAT_OK = False

# COMMAND ----------

# MAGIC %md
# MAGIC If the test worked, set `USE_LIVE = True` and run the next cell. It downloads all twelve series, overwrites `es_eurostat_raw` with `source = 'eurostat_api'`, and you can then re-run Part 4 to rebuild `es_annual`.

# COMMAND ----------

USE_LIVE = False

if USE_LIVE and EUROSTAT_OK:
    frames = []
    for code, dataset, filters, description in SERIES:
        df = fetch_eurostat(dataset, filters)
        df["series"], df["dataset"], df["filters"], df["description"] = code, dataset, filters, description
        frames.append(df)
    live = pd.concat(frames, ignore_index=True)
    live["source"] = "eurostat_api"
    live["loaded_at"] = pd.Timestamp.now(tz="UTC").tz_localize(None)
    live = live[["series", "dataset", "period", "value", "filters", "description", "source", "loaded_at"]]
    (spark.createDataFrame(live).write.mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable("es_eurostat_raw"))
    print(f"Replaced es_eurostat_raw with {len(live):,} live rows. Now re-run Part 4.")
else:
    print("Using the GitHub snapshot (USE_LIVE is False or Eurostat is unreachable).")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 7: Checking the Database

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES LIKE 'es_*'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Delta keeps a history of every write. Try: SELECT * FROM es_eurostat_raw VERSION AS OF 0
# MAGIC DESCRIBE HISTORY es_eurostat_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC You have:
# MAGIC 1. Loaded a snapshot of twelve Eurostat series from GitHub
# MAGIC 2. Saved it unchanged as the bronze table `es_eurostat_raw`
# MAGIC 3. Reshaped it with SQL into `es_annual`, one row per year, including inflation-adjusted columns
# MAGIC 4. Recorded the prime ministers' terms as the reference table `es_terms`
# MAGIC
# MAGIC **Next:** open Notebook 2 to compare the prime ministers.
