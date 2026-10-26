# BLS metadata and sample pipeline

One Python pipeline processes five catalogs for the BLS group assignment:

| Catalog | Official subject |
| --- | --- |
| LN | Labor Force Statistics from the Current Population Survey |
| CW | Consumer Price Index for Urban Wage Earners and Clerical Workers |
| PC | Producer Price Index industry data |
| SM | State and Area Employment, Hours, and Earnings |
| LE | Current Population Survey Earnings |

LE is earnings, not labor productivity. These choices exclude Darshit's AP, BD,
FA, JT, and WS. Other teammates' reservations still need to be checked in the
group's assignment list; this program does not reserve catalogs for you.

## Install and run

Use Python 3.10 or newer from this project directory:

```sh
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python src/catalog_pipeline.py --catalogs ln
```

The first command processes LN only. Once comfortable with its outputs, run all five:

```sh
python src/catalog_pipeline.py --catalogs ln cw pc sm le --share-dir samples
```

Defaults: five sampled series, six observations each, at most 20,000 observation
rows read per catalog. No API key is needed. The pipeline downloads metadata and
streams just the beginning of one observation file, closing the connection early.
It does not download or retain complete observation histories.

Completed files are cached. To rebuild from the saved raw files without network:

```sh
python src/catalog_pipeline.py --catalogs ln cw pc sm le --offline --share-dir samples
```

Use `--refresh` to replace cached inputs with fresh downloads. Use it when changing
sampling limits if the cached prefix is too short. `--data-dir` changes the raw and
processed destination; `--sample-series`, `--observations`, and `--max-lines`
change sampling limits. Do not combine `--offline` with `--refresh`.

## Files to inspect

For each catalog, for example LN:

| Path | Contents |
| --- | --- |
| `data/raw/ln/` | Original series file, documentation, classifications, footnotes, and bounded observation prefix |
| `data/raw/ln/*.source.json` | Source URL, retrieval timestamp, byte count, SHA-256, and whether the file is partial |
| `data/processed/ln/metadata.json` | Every metadata row, original fields plus classification descriptions |
| `data/processed/ln/samples.json` | Sample series, their enriched metadata, and actual observations |
| `data/processed/ln/summary.json` | Counts, duplicates, completeness, lookup matches, sampling issues, and source receipts |
| `data/processed/ln/lookups.json` | The classification tables used for enrichment |
| `samples/ln/` | Small copies of samples and summary, suitable for GitHub, when `--share-dir samples` is used |

Open `samples/ln/samples.json` first to see how a series's codes, descriptions,
and observations connect. Read [docs/validation_report.md](docs/validation_report.md)
for the measured results from the primary BLS source.

`data/` is ignored by Git. LN's expanded metadata exceeds GitHub's ordinary
100 MiB file limit, so keep full metadata local; the code, configuration, tests,
small samples, and reports are the reviewable deliverables. Full raw inputs and
expanded metadata can be regenerated with the commands above.

## How the pipeline works

1. Download `.series`, `.txt`, `.footnote`, and configured classification tables.
2. Read tab-delimited fields as text and trim padding, preserving leading zeros.
3. Join classifications with explicitly configured keys. PC products use
   **both `industry_code` and `product_code`**. The seasonal lookup's
   `seasonal_code` joins to the series file's `seasonal` field.
4. Preserve every metadata row. Unknown or blank descriptions become JSON `null`;
   counts and unmatched code combinations are reported. Duplicate lookup keys
   stop processing rather than multiplying rows or choosing an arbitrary label.
5. Stream a bounded prefix of one official observation file; save that prefix
   separately as `observations.sample.tsv` and mark it partial in its receipt.
6. Check sampled IDs, duplicates, years, period codes, and metadata coverage.
   Preserve exact observation values in `raw_value`; numeric `value` is null
   for nonnumeric or nonfinite data. Keep original footnote codes.
7. Write strict JSON and a validation report. Exit nonzero if any catalog fails.

Configurations are in [config/catalogs.json](config/catalogs.json). The program
does not try to decode opaque series IDs into classifications. Original series
IDs and fields remain intact. SM does not supply a `series_title` field; its
state, area, industry, and measure descriptions provide context without an
invented title. Frequency is described through published periodicity lookups
where available; each observation also has a `period_kind` derived from its
official period code. No artificial dates are assigned to quarterly or annual data.

## Interpretation and limitations

- Samples are the **first observations encountered**, not random selections or
  the most recent releases. A file named `Current` can still contain older years.
- Match rates measure code-to-lookup coverage, not an independent verification
  of BLS methodology or each economic value. No API cross-check was performed.
- Metadata missing counts describe empty fields, including fields such as
  footnotes that may legitimately be empty. Missing descriptions remain visible.
- Footnote codes are retained; their definitions remain in raw `.footnote` files.
- A series is validated for identifier uniqueness, catalog prefix, and valid
  beginning/ending coverage. Observation checks cover the downloaded prefix only.
- Files are fetched sequentially and cached individually, not as a synchronized
  BLS snapshot. Consult retrieval receipts and coverage fields when refreshing.
- A failed catalog writes `last_failure.json`; previous successful exports may
  still exist. Check the exit status and failure file before using old outputs.
- BLS may reject some clients with HTTP 403. The configured research User-Agent
  worked for this run. If downloads fail, supply the exact primary-source files
  in `data/raw/<catalog>/` and use `--offline`; a real observation prefix is still
  required. Inputs without receipts are explicitly marked unverified local files.
- An alternate `downloadt.bls.gov` host was investigated and had inconsistent
  lookup contents. Those experimental files were discarded; delivered results
  use the primary `download.bls.gov` host.

## Tests and implementation reference

```sh
python -m unittest discover -s tests
```

Five tests use synthetic edge cases for composite product keys, ambiguous lookups,
leading zeros, missing values, trailing tabs, seasonal key names, invalid and
duplicate observations, quarterly periods, and bounded streaming.

The assignment entry point is `src/catalog_pipeline.py`. The earlier CPI pilot,
its tests, and the CPI/CES reference scripts and outputs were removed during
project cleanup; the five-catalog pipeline does not depend on them.

Darshit's [sample script](https://github.com/shah-darshit/MacroCorpusPipeline/blob/main/scripts/sample_catalog.py)
informed the metadata/sample/summary organization and the working research
User-Agent. This implementation uses catalog-specific joins and additional
validation; it is not a certification of his entire repository.

Official source: <https://download.bls.gov/pub/time.series/>.
