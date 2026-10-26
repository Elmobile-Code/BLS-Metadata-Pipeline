# Five-catalog validation report

Validated on October 26, 2026 using primary-source files from `https://download.bls.gov/pub/time.series/`.
All five online runs and the subsequent offline rebuild completed successfully.

| Catalog | Metadata records | Unique IDs | Lookup tables | Lookup match rate | Sample series | Observations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| LN | 68,626 | 68,626 | 35 | 100% | 5 | 30 |
| CW | 7,868 | 7,868 | 5 | 100% | 5 | 30 |
| PC | 4,510 | 4,510 | 3 | 100% | 5 | 30 |
| SM | 22,927 | 22,927 | 6 | 100% | 5 | 30 |
| LE | 11,062 | 11,062 | 17 | 100% | 5 | 30 |

Total: **114,993 metadata records**, 25 sampled series, 150 observations.

## Checks performed

- Original series IDs and metadata fields preserved for every row; no duplicate IDs.
- All configured classification keys matched a populated description; no unmatched or ambiguous lookup keys.
- PC products joined using both industry and product codes.
- All series have recognized beginning/ending periods and valid coverage ordering.
- Sample observations have matching, unambiguous metadata; no duplicate observation keys, invalid periods, or out-of-coverage observations.
- All output JSON parses without NaN or Infinity.
- Every delivered input has a primary-source URL, retrieval time, size, and SHA-256 receipt. Observation prefixes are explicitly marked partial.
- Five assignment-specific synthetic unit tests passed after cleanup. Two earlier CPI pilot tests were removed with that unrelated pilot.
- The offline command reproduced all five catalogs successfully from cached inputs.

## Sample coverage

These are bounded prefixes, not representative random samples or latest-release samples.
Five series per catalog have six observations each. First and last periods across the exported samples:

| Catalog | Earliest sample | Latest sample |
| --- | --- | --- |
| LN | 1948 M01 | 1949 Q02 |
| CW | 1997 M01 | 1997 M06 |
| PC | 1998 M01 | 1998 M07 |
| SM | 2006 M01 | 2006 M06 |
| LE | 1995 Q01 | 1996 Q02 |

## Limits and delivery

Classification coverage verifies joins against published lookup tables; it does not independently establish economic accuracy. Observation checks apply only to the bounded sample. No BLS API cross-check or forecasting was performed. Footnote codes are retained; definitions remain in the raw footnote files. Metadata fields absent from a catalog (such as SM series titles) are not fabricated. Files were fetched sequentially, not as an atomic snapshot.

An alternate-host experiment exposed inconsistent lookup contents and was excluded. Every result above uses the primary `download.bls.gov` host.

Full enriched metadata is local under `data/processed/<catalog>/metadata.json`. LN metadata is approximately 147 MB and should not be committed as an ordinary GitHub file. Small samples and complete validation summaries are under `samples/<catalog>/`, with per-input source receipts embedded in each summary. Full raw inputs and expanded metadata can be regenerated using the pipeline.

## Reproduce

```sh
python3 -m pip install -r requirements.txt
python3 src/catalog_pipeline.py --catalogs ln cw pc sm le --share-dir samples
python3 src/catalog_pipeline.py --catalogs ln cw pc sm le --offline --share-dir samples
python3 -m unittest discover -s tests
```

See [README.md](../README.md) for setup, caching, failure handling, and output interpretation.
