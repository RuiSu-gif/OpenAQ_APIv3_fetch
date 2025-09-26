# OpenAQ Hourly Fetch Workflow

This directory contains a Jupyter notebook that automates downloading hourly PM2.5 and ozone concentrations from the OpenAQ v3 API for any set of countries.

## Notebook
- `openaq_hourly_fetch.ipynb` – main workflow. The first cell exposes configuration knobs and the second cell implements the data collection pipeline.

## Prerequisites
- Python 3.10 or newer with the packages `pandas`, `requests`, `tqdm`, and `openaq` installed.
- An OpenAQ v3 API key stored in the environment variable `OPENAQ_API_KEY` before launching the notebook session. The key is never written to disk inside the notebook.
- A CSV file containing the list of countries or regions you want to process.

## Configuration Highlights
- `COUNTRY_CSV_PATH` points to the CSV file with country metadata.
- `COUNTRY_CSV_COLUMNS` maps the required logical column names (`country_name`, `iso3`, and optionally `fasst_region`) to the actual column headers in your CSV. Set an entry to `None` if the column doesn’t exist.
- Use `INCLUDE_COUNTRIES` or `EXCLUDE_COUNTRIES` to scope the run using ISO2, ISO3, or country names.
- Adjust the `DATETIME_FROM` and `DATETIME_TO` window as needed; timestamps are interpreted in UTC.

## Running the Notebook
1. Open the notebook in JupyterLab, VS Code, or any compatible environment.
2. Edit the configuration cell to match your CSV path and column names.
3. Run the configuration cell followed by the implementation cell; finally call `main()` if it is not executed automatically.
4. Output files are written to the `output/` subdirectory next to the notebook, with separate CSV files per country and pollutant.

## Tips
- If you work behind a corporate proxy, set `DISABLE_ENV_PROXIES = False` in the configuration cell so that the environment proxy variables remain active.
- Hourly downloads are chunked and include retry logic tuned for the OpenAQ rate limits. You can relax or tighten these values in the configuration section.
