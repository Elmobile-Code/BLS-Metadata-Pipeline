"""Enrich BLS catalog metadata and stream a bounded observation sample."""
from __future__ import annotations

import argparse
import csv
from collections import Counter
from datetime import datetime, timezone
import hashlib
import io
import json
import math
from pathlib import Path
import re

import pandas as pd
import requests

PROJECT = Path(__file__).resolve().parents[1]
BASE_URL = 'https://download.bls.gov/pub/time.series'
CONFIG = PROJECT / 'config' / 'catalogs.json'
USER_AGENT = ('Mozilla/5.0 (Macintosh; BLS-Corpus-Research/1.0; +https://www.bls.gov/data/) '
              'bls-corpus/0.1')


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.part')
    try:
        temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False,
                                        allow_nan=False) + '\n', encoding='utf-8')
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def receipt(path, url, **extra):
    write_json(path.with_name(path.name + '.source.json'), {
        'url': url, 'retrieved_at': datetime.now(timezone.utc).isoformat(),
        'bytes': path.stat().st_size,
        'sha256': hashlib.sha256(path.read_bytes()).hexdigest(), **extra})


def download(session, url, path, offline=False, refresh=False):
    if path.exists() and not refresh:
        return
    if offline:
        raise FileNotFoundError(f'Offline input missing: {path}')
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.part')
    try:
        with session.get(url, timeout=(15, 60), stream=True) as response:
            response.raise_for_status()
            with temporary.open('wb') as target:
                for chunk in response.iter_content(65536):
                    target.write(chunk)
        temporary.replace(path)
        receipt(path, url, partial=False)
    finally:
        temporary.unlink(missing_ok=True)


def read_table(path):
    # Codes stay strings: zeros and BLS missing-value markers must survive.
    try:
        text = path.read_text(encoding='utf-8-sig')
    except UnicodeDecodeError:
        text = path.read_text(encoding='latin-1')
    reader = csv.reader(io.StringIO(text), delimiter='\t')
    header = [c.strip() for c in next(reader)]
    while header and not header[-1]:
        header.pop()
    if not all(header) or len(header) != len(set(header)):
        raise ValueError(f'Duplicate columns in {path}')
    rows = []
    for number, cells in enumerate(reader, 2):
        cells = [c.strip() for c in cells]
        if not any(cells):
            continue
        if any(cells[len(header):]):
            raise ValueError(f'{path}:{number}: extra nonempty fields')
        rows.append((cells + [''] * len(header))[:len(header)])
    return pd.DataFrame(rows, columns=header, dtype=str)


def enrich(series, lookups):
    """Preserve every series row, and reject ambiguous classification joins."""
    result = series.copy()
    stats = []
    for spec, table in lookups:
        keys = spec['keys']
        table = table.rename(columns=dict(zip(spec.get('lookup_keys', keys), keys)))
        missing = set(keys) - (set(series.columns) & set(table.columns))
        if missing:
            raise ValueError(f"{spec['file']}: missing join columns {sorted(missing)}")
        label = spec.get('description')
        if not label:
            candidates = [c for c in table if c not in keys and
                          c.endswith(('_name', '_text'))]
            if len(candidates) != 1:
                raise ValueError(f"{spec['file']}: configure description column explicitly")
            label = candidates[0]
        if label not in table:
            raise ValueError(f"{spec['file']}: missing description column {label}")
        if label in result:
            raise ValueError(f'Refusing to overwrite original metadata column {label}')
        duplicates = table.duplicated(keys, keep=False)
        if duplicates.any():
            raise ValueError(f"{spec['file']}: duplicate lookup keys: "
                             f"{table.loc[duplicates, keys].head().to_dict('records')}")
        joined = result.merge(table[keys + [label]], on=keys, how='left',
                              sort=False, validate='many_to_one', indicator=True)
        blank = joined[keys].eq('').any(axis=1)
        matched = joined['_merge'].eq('both') & ~blank
        unmatched = ~matched & ~blank
        described = matched & joined[label].fillna('').ne('')
        joined.loc[~described, label] = None
        stats.append({
            'file': spec['file'], 'keys': keys, 'description_column': label,
            'lookup_keys': spec.get('lookup_keys', keys),
            'matched': int(matched.sum()), 'unmatched': int(unmatched.sum()),
            'blank_codes': int(blank.sum()),
            'matched_without_description': int((matched & ~described).sum()),
            'match_rate': float(matched.sum() / (~blank).sum()) if (~blank).any() else None,
            'unmatched_codes': joined.loc[unmatched, keys].drop_duplicates().to_dict('records')})
        result = joined.drop(columns='_merge')
    return result, stats


def period_kind(period):
    if period in {'M13', 'Q05', 'S03', 'A01'}:
        return 'annual_average'
    if re.fullmatch(r'M(0[1-9]|1[0-2])', period):
        return 'monthly'
    if re.fullmatch(r'Q0[1-4]', period):
        return 'quarterly'
    if re.fullmatch(r'S0[12]', period):
        return 'semiannual'
    return None


def sample_prefix(session, url, path, series_limit, obs_limit, max_lines,
                  offline=False, refresh=False):
    """Save only complete lines from the beginning of a data file; close early."""
    if path.exists() and not refresh:
        return
    if offline:
        raise FileNotFoundError(f'Offline sample missing: {path}')
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.part')
    counts = Counter()
    try:
        with session.get(url, timeout=(15, 60), stream=True) as response:
            response.raise_for_status()
            with temporary.open('wb') as target:
                for number, line in enumerate(response.iter_lines(chunk_size=8192)):
                    target.write(line + b'\n')
                    if number == 0:
                        header = [v.strip() for v in line.decode('latin-1').split('\t')]
                        if not {'series_id', 'year', 'period', 'value'} <= set(header):
                            raise ValueError('Unexpected observation header')
                        continue
                    if line.strip():
                        cells = line.decode('latin-1').split('\t')
                        sid = cells[header.index('series_id')].strip()
                        if sid and (sid in counts or len(counts) < series_limit):
                            counts[sid] += 1
                    if (len(counts) >= series_limit and min(counts.values()) >= obs_limit
                            or number >= max_lines):
                        break
        temporary.replace(path)
        receipt(path, url, partial=True, max_lines=max_lines,
                sample_series_limit=series_limit, observations_per_series=obs_limit,
                note='Bounded prefix, not the complete BLS data file; earliest rows, not random.')
    finally:
        temporary.unlink(missing_ok=True)


def attach_samples(table, metadata, series_limit, obs_limit):
    counts = Counter(metadata['series_id'])
    by_id = {r['series_id']: r for r in metadata.to_dict('records') if counts[r['series_id']] == 1}
    collected = {}
    issues = {'unknown_series_ids': [], 'ambiguous_series_ids': [],
              'invalid_observations': [], 'duplicate_observations': [],
              'nonnumeric_values': 0}
    seen = set()
    for row in table.to_dict('records'):
        sid = row['series_id']
        if sid not in counts or counts[sid] != 1:
            key = 'unknown_series_ids' if sid not in counts else 'ambiguous_series_ids'
            if sid not in issues[key]:
                issues[key].append(sid)
            continue
        key = (sid, row['year'], row['period'])
        if key in seen:
            issues['duplicate_observations'].append(list(key))
            continue
        seen.add(key)
        kind = period_kind(row['period'])
        if not re.fullmatch(r'\d{4}', row['year']) or not kind:
            issues['invalid_observations'].append(row)
            continue
        meta = by_id[sid]
        point = (int(row['year']), row['period'])
        if (point < (int(meta['begin_year']), meta['begin_period']) or
                point > (int(meta['end_year']), meta['end_period'])):
            issues['invalid_observations'].append({'reason': 'outside metadata coverage', **row})
            continue
        if sid not in collected:
            if len(collected) >= series_limit:
                continue
            collected[sid] = {'series_id': sid, 'metadata': meta, 'observations': []}
        if len(collected[sid]['observations']) >= obs_limit:
            continue
        try:
            value = float(row['value'].replace(',', ''))
            if not math.isfinite(value):
                value = None
        except ValueError:
            value = None
        issues['nonnumeric_values'] += value is None
        collected[sid]['observations'].append({**row, 'raw_value': row['value'],
                                              'value': value, 'period_kind': kind})
    return list(collected.values()), issues


def run(catalog, config, args):
    raw = args.data_dir / 'raw' / catalog
    out = args.data_dir / 'processed' / catalog
    base = f'{args.base_url.rstrip("/")}/{catalog}'
    errors, warnings, sources = [], [], []
    with requests.Session() as session:
        session.headers.update({'User-Agent': USER_AGENT, 'Accept-Encoding': 'identity'})
        names = [f'{catalog}.series', f'{catalog}.txt', f'{catalog}.footnote']
        for name in names:
            download(session, f'{base}/{name}', raw / name, args.offline, args.refresh)
        series = read_table(raw / names[0])
        required = {'series_id', 'begin_year', 'begin_period', 'end_year', 'end_period'}
        if not required <= set(series):
            raise ValueError(f'Missing required series columns: {required - set(series)}')
        tables = []
        for spec in config['lookups']:
            try:
                download(session, f"{base}/{spec['file']}", raw / spec['file'],
                         args.offline, args.refresh)
                tables.append((spec, read_table(raw / spec['file'])))
            except (requests.RequestException, FileNotFoundError) as exc:
                if not spec.get('optional'):
                    raise
                warnings.append(f"Optional lookup unavailable: {spec['file']}: {exc}")
        enriched, joins = enrich(series, tables)
        ids = series['series_id']
        duplicates = ids[ids.duplicated(keep=False)].value_counts().to_dict()
        if duplicates or ids.eq('').any():
            errors.append('Duplicate or blank series identifiers; ambiguous series are not sampled.')
        invalid_ids = ids[~ids.str.startswith(catalog.upper()) | ids.str.contains(r'\s')].tolist()
        if invalid_ids:
            errors.append('Series identifiers have an unexpected catalog prefix or whitespace.')
        invalid_coverage = []
        for row in series[['series_id', 'begin_year', 'begin_period', 'end_year', 'end_period']].to_dict('records'):
            if (not re.fullmatch(r'\d{4}', row['begin_year']) or
                    not re.fullmatch(r'\d{4}', row['end_year']) or
                    not period_kind(row['begin_period']) or not period_kind(row['end_period']) or
                    (row['begin_year'], row['begin_period']) > (row['end_year'], row['end_period'])):
                invalid_coverage.append(row)
        if invalid_coverage:
            errors.append('Invalid series beginning/ending coverage; see invalid_series_coverage.')
        unmapped = [c for c in series if (c.endswith('_code') or c == 'seasonal')
                    and c not in {k for spec, _ in tables for k in spec['keys']}]
        if unmapped:
            warnings.append(f'Classification fields without a lookup: {unmapped}')
        for join in joins:
            if join['unmatched'] or join['matched_without_description']:
                warnings.append(f"Incomplete descriptions in {join['file']}; see lookup_matches.")
        sample_path = raw / 'observations.sample.tsv'
        samples = []
        sample_issues = {}
        try:
            sample_prefix(session, f"{base}/{config['data_file']}", sample_path,
                          args.sample_series, args.observations, args.max_lines,
                          args.offline, args.refresh)
            samples, sample_issues = attach_samples(read_table(sample_path), enriched,
                                                    args.sample_series, args.observations)
            for key in ['unknown_series_ids', 'ambiguous_series_ids',
                        'invalid_observations', 'duplicate_observations']:
                if sample_issues[key]:
                    errors.append(f'Observation validation issue: {key}')
        except (requests.RequestException, FileNotFoundError, ValueError) as exc:
            errors.append(f'Sample retrieval/validation failed: {exc}')
        if not samples:
            errors.append('No valid sample observations obtained.')
        elif len(samples) < args.sample_series:
            warnings.append('Bounded prefix contains fewer series than requested.')
        if any(len(s['observations']) < args.observations for s in samples):
            warnings.append('Some sampled series have fewer observations than requested.')
    for path in sorted(raw.glob('*')):
        if path.name.endswith(('.source.json', '.part')):
            continue
        source = path.with_name(path.name + '.source.json')
        sources.append({'file': path.name, **(json.loads(source.read_text()) if source.exists()
                        else {'provenance': 'Pre-existing local input; retrieval time/URL unverified',
                              'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})})
    if args.base_url.rstrip('/') != BASE_URL:
        warnings.append('Alternate source host used. It may lag the primary BLS directory; '
                        'metadata coverage and sample periods are reported, not assumed current.')
    report = {
        'catalog': catalog.upper(), 'title': config['title'],
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'status': 'failed' if errors else 'passed_with_warnings' if warnings else 'passed',
        'series_count': len(series), 'unique_series_ids': int(ids[ids.ne('')].nunique()),
        'duplicate_series_ids': duplicates, 'missing_series_ids': int(ids.eq('').sum()),
        'invalid_series_identifiers': invalid_ids, 'invalid_series_coverage': invalid_coverage,
        'original_fields': list(series.columns),
        'missing_metadata': {c: int(series[c].eq('').sum()) for c in series},
        'lookup_matches': joins, 'unmapped_classification_fields': unmapped,
        'metadata_end_year_counts': series['end_year'].value_counts().to_dict(),
        'sample_series': len(samples),
        'sample_observations': sum(len(s['observations']) for s in samples),
        'sample_validation': sample_issues,
        'sampling': {'method': 'bounded prefix of official data file',
                     'data_file': config['data_file'], 'max_lines': args.max_lines,
                     'note': 'First observations encountered; not random or necessarily recent.'},
        'errors': errors, 'warnings': warnings, 'sources': sources}
    write_json(out / 'metadata.json', enriched.to_dict('records'))
    write_json(out / 'samples.json', samples)
    write_json(out / 'lookups.json', {spec['file']: table.to_dict('records') for spec, table in tables})
    write_json(out / 'summary.json', report)
    if args.share_dir:
        write_json(args.share_dir / catalog / 'samples.json', samples)
        write_json(args.share_dir / catalog / 'summary.json', report)
    (out / 'last_failure.json').unlink(missing_ok=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--catalogs', nargs='+', type=str.lower, default=['ln'],
                        choices=['ln', 'cw', 'pc', 'sm', 'le'])
    parser.add_argument('--data-dir', type=Path, default=PROJECT / 'data')
    parser.add_argument('--share-dir', type=Path, help='Also export small samples and reports here')
    parser.add_argument('--base-url', default=BASE_URL)
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--refresh', action='store_true')
    parser.add_argument('--sample-series', type=int, default=5)
    parser.add_argument('--observations', type=int, default=6)
    parser.add_argument('--max-lines', type=int, default=20000)
    args = parser.parse_args()
    if args.offline and args.refresh:
        parser.error('--offline and --refresh cannot be combined')
    if min(args.sample_series, args.observations, args.max_lines) < 1:
        parser.error('Sampling limits must be positive')
    configs = json.loads(CONFIG.read_text())
    failed = False
    for catalog in args.catalogs:
        try:
            report = run(catalog, configs[catalog], args)
            print(f"{catalog.upper()}: {report['status']}, {report['series_count']} series, "
                  f"{report['sample_observations']} observations", flush=True)
            failed |= bool(report['errors'])
        except (requests.RequestException, OSError, ValueError, KeyError) as exc:
            failed = True
            report = {'catalog': catalog.upper(), 'status': 'failed', 'errors': [str(exc)]}
            write_json(args.data_dir / 'processed' / catalog / 'last_failure.json', report)
            print(f'{catalog.upper()}: failed: {exc}', flush=True)
    raise SystemExit(1 if failed else 0)


if __name__ == '__main__':
    main()
