# Spain's Economy by Prime Minister, on Databricks

A two-notebook series that tests a viral comparison of Spanish prime ministers (Aznar, Zapatero, Rajoy, Sánchez) against official Eurostat data. Students build a small database, reproduce the viral table in SQL, and then check whether its conclusions survive once growth is annualized, adjusted for inflation, measured per person, and compared with the euro area. Everything runs in a free Databricks account.

[![Get Databricks Free Edition](https://img.shields.io/badge/Databricks-Get%20Free%20Edition-FF3621?logo=databricks&logoColor=white)](https://www.databricks.com/learn/free-edition)

## The Notebooks

| # | Notebook | What you learn |
|---|---|---|
| 1 | [Sourcing and Building the Database](01_Sourcing_and_Building_the_Database.py) | Load an Eurostat snapshot from GitHub, save it as a Delta table, reshape it with SQL, and (optionally) call the live Eurostat API |
| 2 | [Comparing Prime Ministers](02_Comparing_Prime_Ministers.py) | Join terms to years, annualize and deflate in SQL, test a choice with a widget, draw charts with each term shaded, and check an `ai_query()` summary |

**Run Notebook 1 first.** It builds three tables (`es_eurostat_raw`, `es_annual`, `es_terms`) that Notebook 2 reads. All names start with `es_`, so they sit alongside the Fantasy Premier League tables without clashing.

The same analysis is also in this repository as an Excel workbook ([excel/spain_economy_by_term.xlsx](excel/spain_economy_by_term.xlsx)), which pulls the same twelve series with Power Query. Part 7 of Notebook 2 compares the Excel, Jupyter and Databricks versions step by step.

## Getting Started

### Step 1: Create a free Databricks account

1. Go to **[Databricks Free Edition](https://www.databricks.com/learn/free-edition)** and click **Sign up**.
2. Choose **Free Edition**, not the Free Trial. Free Edition does not expire and doesn't ask for a credit card.
3. Sign in with a Google or Microsoft account, or use your email address.

### Step 2: Copy this repository into your workspace

1. In the left sidebar, click **Workspace**, then open your home folder.
2. Click **Create** in the upper right and choose **Git folder**.
3. In **Git repository URL**, paste:
   ```
   https://github.com/ericvd-ucb/Spain-Economy-Databricks.git
   ```
4. Click **Create Git folder**. This is a public repository, so you don't need to link a GitHub account.

### Step 3: Run the notebooks

1. Open **01_Sourcing_and_Building_the_Database**, click **Connect**, and choose **Serverless**.
2. Click **Run all**. At the end, `SHOW TABLES` lists the three `es_` tables.
3. Open **02_Comparing_Prime_Ministers** and run it. Change the **Baseline year** dropdown at the top and re-run to see which conclusions survive.

## Data

The notebooks load `data/eurostat_spain_snapshot.csv` from this repository: 366 observations of twelve Eurostat series, retrieved on 22 November 2026. National accounts for 2024 and 2025 are provisional.

| Code | Eurostat dataset | Series |
|---|---|---|
| `rgdp`, `ngdp`, `eagdp` | `nama_10_gdp` | Real and nominal GDP, Spain; real GDP, euro area |
| `gdppc` | `nama_10_pc` | Real GDP per person, Spain |
| `emp` | `nama_10_pe` | Employment, Spain |
| `une`, `eaune` | `lfsa_urgan`, `une_rt_a` | Unemployment rate, Spain and euro area |
| `debt` | `gov_10dd_edpt1` | Government debt, % of GDP |
| `hicp`, `eahicp` | `prc_hicp_aind` | Consumer price index, Spain and euro area |
| `hpi` | `prc_hpi_a` | House price index, Spain |
| `mw` | `earn_mw_cur` | Statutory minimum wage, EUR per month, January and July |

### Why a snapshot instead of the live API?

Databricks Free Edition allows internet access only to a set of trusted domains, and Eurostat may not be one of them. GitHub's raw file server is reachable, so the snapshot always works. Part 6 of Notebook 1 tests whether your workspace can reach Eurostat and, if so, refreshes the data live.

To update the snapshot, run that Part 6 code anywhere with internet access (your laptop, Colab, a paid Databricks workspace), save the result as `data/eurostat_spain_snapshot.csv` with columns `series, dataset, period, value`, and commit it.

## Troubleshooting

**"Table or view not found: es_annual."** Notebook 1 hasn't been run in this workspace yet, or it stopped before it finished. Run it from the top again.

**Part 6 says it could not reach Eurostat.** That is the Free Edition network restriction, not a bug. Keep using the snapshot.

**`ai_query()` fails in Notebook 2.** Free Edition supports only some hosted models. Open **Serving** in the left sidebar, pick a model from the list, and replace the model name in the cell.

## License

This project is released under the [BSD 3-Clause License](LICENSE). Eurostat data are reused under Eurostat's copyright and reuse policy (source: Eurostat) and are not covered by this license.

## Further Reading

- [Eurostat API: getting started](https://wikis.ec.europa.eu/display/EUROSTATHELP/API+-+Getting+started+with+statistics+API)
- [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [Git folders in Databricks](https://docs.databricks.com/aws/en/repos/)
- [Delta Lake on Databricks](https://docs.databricks.com/en/delta/index.html)
