# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2: Comparing Prime Ministers, Fairly
# MAGIC
# MAGIC **Learning Objectives:** By the end of this notebook you will be able to:
# MAGIC 1. Join a reference table (terms) to a data table (years) to get start and end values
# MAGIC 2. Reproduce a viral "who did best" table in SQL, then stress-test it
# MAGIC 3. Explain why annualizing, adjusting for inflation, measuring per person and benchmarking change the answer
# MAGIC 4. Use a widget to test how sensitive a conclusion is to one analytical choice
# MAGIC 5. Draw time series with each prime minister's term shaded
# MAGIC
# MAGIC **Before you start:** run Notebook 1 once. This notebook reads the tables `es_annual` and `es_terms`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: One Analytical Choice, as a Widget
# MAGIC
# MAGIC When does a prime minister's record start? Aznar took office in May 1996, Rajoy in December 2011. With annual data we must pick a year to measure from:
# MAGIC
# MAGIC - **Year took office**: compare 1996 with 2004 for Aznar. Simple, but it credits him with nothing that happened in 1996.
# MAGIC - **Year before taking office**: compare 1995 with 2004. It credits him with all of 1996, including months before he governed.
# MAGIC
# MAGIC Neither is "right". Use the dropdown at the top of the notebook to switch, then re-run the cells below.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.dropdown(
    "baseline",
    "Year took office",
    ["Year took office", "Year before taking office"],
    "Baseline year",
)

# COMMAND ----------

baseline = dbutils.widgets.get("baseline")
offset = 1 if baseline == "Year before taking office" else 0

# For each term, attach the full row of es_annual for the start year (s) and the end year (e).
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW term_endpoints AS
WITH t AS (
  SELECT *,
         start_year - {offset}                AS base_year,
         end_year - (start_year - {offset})   AS n_years
  FROM es_terms
)
SELECT t.term_order, t.prime_minister, t.party,
       concat(t.base_year, '-', t.end_year) AS years,
       t.n_years,
       struct(s.*) AS s,
       struct(e.*) AS e
FROM t
JOIN es_annual s ON s.year = t.base_year
JOIN es_annual e ON e.year = t.end_year
""")
print(f"Baseline: {baseline}")
display(spark.sql("SELECT prime_minister, years, n_years FROM term_endpoints ORDER BY term_order"))

# COMMAND ----------

# MAGIC %md
# MAGIC `struct(s.*)` packs every column of the start year into one column called `s`, so we can write `s.real_gdp` and `e.real_gdp` instead of listing thirty columns twice. This is a handy Spark SQL trick for "before and after" comparisons.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: The Article View
# MAGIC
# MAGIC This is the kind of table the viral post showed: **cumulative** changes over each term, mostly in **nominal** euros. Every number is correct. Before reading on, write down which prime minister looks best.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   prime_minister,
# MAGIC   party,
# MAGIC   years,
# MAGIC   ROUND(100 * (e.nominal_gdp / s.nominal_gdp - 1), 1)             AS nominal_gdp_growth_pct,
# MAGIC   ROUND(100 * (e.min_wage / s.min_wage - 1), 1)                   AS min_wage_increase_pct,
# MAGIC   ROUND(100 * (e.house_price_index / s.house_price_index - 1), 1) AS house_price_change_pct,
# MAGIC   ROUND(e.employment_thousands - s.employment_thousands, 0)       AS jobs_created_thousands,
# MAGIC   ROUND(e.unemployment_rate - s.unemployment_rate, 1)             AS unemployment_change_pp,
# MAGIC   ROUND(e.debt_pct_gdp - s.debt_pct_gdp, 1)                       AS debt_change_pp
# MAGIC FROM term_endpoints
# MAGIC ORDER BY term_order

# COMMAND ----------

# MAGIC %md
# MAGIC Empty cells mean the series does not reach back that far: Eurostat's minimum wage data start in 1999 and house prices in 2006.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: The Stress Test
# MAGIC
# MAGIC Four corrections, each a line of SQL:
# MAGIC
# MAGIC | Problem with the article view | Fix | Formula |
# MAGIC |---|---|---|
# MAGIC | Terms have different lengths | **Annualize** | `POWER(end / start, 1 / years) - 1` |
# MAGIC | Prices rose, so euros are worth less | Use **real** (inflation-adjusted) series | real GDP, real minimum wage |
# MAGIC | Population grew | Measure **per person** | real GDP per person |
# MAGIC | Everyone rode the same European cycle | **Benchmark** against the euro area | Spain minus euro area |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW stress_test AS
# MAGIC SELECT
# MAGIC   term_order,
# MAGIC   prime_minister,
# MAGIC   years,
# MAGIC   ROUND(100 * (POWER(e.real_gdp            / s.real_gdp,            1.0 / n_years) - 1), 2) AS real_gdp_growth,
# MAGIC   ROUND(100 * (POWER(e.ea_real_gdp         / s.ea_real_gdp,         1.0 / n_years) - 1), 2) AS ea_real_gdp_growth,
# MAGIC   ROUND(100 * (POWER(e.real_gdp_per_person / s.real_gdp_per_person, 1.0 / n_years) - 1), 2) AS real_gdp_per_person_growth,
# MAGIC   ROUND(100 * (POWER(e.nominal_gdp         / s.nominal_gdp,         1.0 / n_years) - 1), 2) AS nominal_gdp_growth,
# MAGIC   ROUND(100 * (POWER(e.hicp                / s.hicp,                1.0 / n_years) - 1), 2) AS inflation,
# MAGIC   ROUND(100 * (POWER(e.min_wage            / s.min_wage,            1.0 / n_years) - 1), 2) AS min_wage_nominal,
# MAGIC   ROUND(100 * (POWER(e.real_min_wage       / s.real_min_wage,       1.0 / n_years) - 1), 2) AS min_wage_real,
# MAGIC   ROUND(100 * (POWER(e.house_price_index   / s.house_price_index,   1.0 / n_years) - 1), 2) AS house_prices_nominal,
# MAGIC   ROUND(100 * (POWER(e.real_house_price_index / s.real_house_price_index, 1.0 / n_years) - 1), 2) AS house_prices_real,
# MAGIC   ROUND(100 * (POWER(e.employment_thousands / s.employment_thousands, 1.0 / n_years) - 1), 2) AS employment_growth,
# MAGIC   s.unemployment_rate AS unemployment_at_start,
# MAGIC   e.unemployment_rate AS unemployment_at_end,
# MAGIC   s.debt_pct_gdp      AS debt_at_start,
# MAGIC   e.debt_pct_gdp      AS debt_at_end
# MAGIC FROM term_endpoints;
# MAGIC
# MAGIC SELECT * EXCEPT (term_order),
# MAGIC        ROUND(real_gdp_growth - ea_real_gdp_growth, 2) AS spain_minus_euro_area
# MAGIC FROM stress_test
# MAGIC ORDER BY term_order

# COMMAND ----------

# MAGIC %md
# MAGIC All growth figures are **percent per year**. Read across a row before comparing down a column. High nominal growth with high inflation, or a big fall in unemployment from a very high starting point, tells a different story than the headline.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4: Charts with Each Term Shaded
# MAGIC
# MAGIC Tables compress a term into two numbers. Charts show what happened in between: a crisis in the middle of a term, or a recovery that started before a new prime minister arrived. Each chart shades the terms from `es_terms`: blue for PP, red for PSOE.

# COMMAND ----------

import plotly.graph_objects as go
from plotly.subplots import make_subplots

annual = spark.table("es_annual").orderBy("year").toPandas()
terms = spark.table("es_terms").orderBy("term_order").toPandas()
PARTY_COLOUR = {"PP": "rgba(31, 119, 180, 0.12)", "PSOE": "rgba(214, 39, 40, 0.12)"}

def shade_terms(fig, rows=4):
    """Add one shaded band per prime minister to every panel."""
    for r in range(1, rows + 1):
        for _, t in terms.iterrows():
            fig.add_vrect(x0=t.start_year, x1=t.end_year, fillcolor=PARTY_COLOUR[t.party],
                          line_width=0, layer="below", row=r, col=1)
    for _, t in terms.iterrows():
        fig.add_annotation(x=(t.start_year + t.end_year) / 2, y=1.02, xref="x", yref="paper",
                           text=t.prime_minister.split()[-1], showarrow=False, font=dict(size=11))
    return fig

panels = [
    ("Real GDP, 1995 = 100",              [("gdp_index", "Spain"), ("ea_gdp_index", "Euro area")]),
    ("Unemployment rate, %",              [("unemployment_rate", "Spain"), ("ea_unemployment_rate", "Euro area")]),
    ("Minimum wage, EUR per month",       [("min_wage", "Nominal"), ("real_min_wage", "Real (2015 prices)")]),
    ("House prices, 2015 = 100",          [("house_price_index", "Nominal"), ("real_house_price_index", "Real")]),
]
fig = make_subplots(rows=4, cols=1, shared_xaxes=True, subplot_titles=[p[0] for p in panels], vertical_spacing=0.06)
for i, (title, lines) in enumerate(panels, start=1):
    for (col, label), colour in zip(lines, ["#1F3864", "#C55A11"]):
        fig.add_trace(go.Scatter(x=annual.year, y=annual[col], name=label, mode="lines",
                                 line=dict(color=colour, width=2), legendgroup=label,
                                 showlegend=(i == 1 or i == 3)), row=i, col=1)
shade_terms(fig)
fig.update_layout(height=1100, template="plotly_white", title="Spain's economy by prime minister",
                  legend=dict(orientation="h", y=-0.05))
fig.show()

# COMMAND ----------

stress = spark.table("stress_test").orderBy("term_order").toPandas()
fig = go.Figure([
    go.Bar(name="Spain", x=stress.prime_minister, y=stress.real_gdp_growth, marker_color="#1F3864"),
    go.Bar(name="Euro area", x=stress.prime_minister, y=stress.ea_real_gdp_growth, marker_color="#C55A11"),
])
fig.update_layout(barmode="group", template="plotly_white", yaxis_title="% per year",
                  title=f"Annualized real GDP growth by term (baseline: {baseline})")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5: Discussion Questions
# MAGIC
# MAGIC 1. Compare your answer from Part 2 with the stress test. Whose ranking changes most once growth is annualized and adjusted for inflation? Why?
# MAGIC 2. How much of each term's growth was specific to Spain, and how much was the European cycle (the 2008 to 2013 crisis, the 2020 pandemic, the 2022 energy shock)?
# MAGIC 3. Aznar started with unemployment at 22.2% and Rajoy at 21.4%. Is a big fall from a high level the same achievement as a small fall from a low one?
# MAGIC 4. Switch the baseline widget and re-run. Which conclusions survive? What does that tell you about chosen endpoints in political arguments?
# MAGIC 5. The minimum wage rose fastest under Sánchez in nominal terms. How much of that survives in real terms, given inflation in 2022 and 2023?
# MAGIC 6. Is debt as a share of GDP a fair score for a prime minister who governed through a recession or a pandemic? What would you add?
# MAGIC 7. Write a one-paragraph comparison you would be willing to defend, using only numbers from the stress test.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 6 (optional): Ask a Model to Summarize, Then Check It
# MAGIC
# MAGIC `ai_query()` sends a prompt to a model hosted in Databricks. Here we give it the stress test table and ask for a neutral summary. Your job is to **check every number it uses** against the table. If the model name is not available in your workspace, open **Serving** in the left sidebar and pick one that is.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ai_query(
# MAGIC   'databricks-meta-llama-3-3-70b-instruct',
# MAGIC   concat(
# MAGIC     'You are an economist writing for students. Using only the data below, write a neutral 120-word ',
# MAGIC     'comparison of the four Spanish prime ministers. Growth figures are percent per year. ',
# MAGIC     'Mention the euro-area benchmark and inflation. Do not use any number that is not in the data. Data: ',
# MAGIC     to_json(collect_list(struct(*)))
# MAGIC   )
# MAGIC ) AS summary
# MAGIC FROM stress_test

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 7: The Same Pipeline, Three Ways
# MAGIC
# MAGIC This analysis also exists as a Jupyter notebook and an Excel workbook. Each step has an equivalent in all three:
# MAGIC
# MAGIC | Step | Excel | Jupyter (Polars) | Databricks |
# MAGIC |---|---|---|---|
# MAGIC | Get the data | Power Query (Get Data) | `requests` plus the Eurostat API | CSV snapshot from GitHub, or the API (Notebook 1) |
# MAGIC | Keep the raw data | "Live" sheet | DataFrame in memory | Delta table `es_eurostat_raw` |
# MAGIC | Reshape long to wide | `SUMIFS` on the Data sheet | `pivot` | `MAX(CASE WHEN ...)` in SQL |
# MAGIC | Who governed when | Terms sheet | Python dictionary | Table `es_terms` |
# MAGIC | Start and end values | `INDEX` / `MATCH` | `join` | `JOIN` with `struct(s.*)` |
# MAGIC | Change an assumption | Edit a yellow cell | Edit the code | Widget |
# MAGIC | Refresh | Data > Refresh All | Re-run the notebook | Re-run Notebook 1 |
# MAGIC
# MAGIC **Discussion:** which tool makes each step most visible? Which would you trust if a classmate handed it to you? Which scales to 27 countries?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC You have:
# MAGIC 1. Joined terms to years to get start and end values for every series
# MAGIC 2. Reproduced the viral comparison, then annualized, deflated, measured per person and benchmarked it
# MAGIC 3. Tested how one choice (the baseline year) changes the conclusions
# MAGIC 4. Drawn the data with each term shaded, so the story between the endpoints is visible
