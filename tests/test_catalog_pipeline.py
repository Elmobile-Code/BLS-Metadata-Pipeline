"""Synthetic edge cases; live-data results are reported separately."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from catalog_pipeline import attach_samples, enrich, read_table, sample_prefix, write_json


class CatalogTests(unittest.TestCase):
    def test_composite_product_keys_preserve_unmatched_rows(self):
        series = pd.DataFrame([
            {'series_id': 'a', 'industry_code': '01', 'product_code': '00'},
            {'series_id': 'b', 'industry_code': '02', 'product_code': '00'},
            {'series_id': 'c', 'industry_code': '03', 'product_code': '00'}])
        lookup = pd.DataFrame([
            {'industry_code': '01', 'product_code': '00', 'product_name': 'First'},
            {'industry_code': '02', 'product_code': '00', 'product_name': 'Second'}])
        spec = {'file': 'pc.product', 'keys': ['industry_code', 'product_code'],
                'description': 'product_name'}
        result, stats = enrich(series, [(spec, lookup)])
        self.assertEqual(result.series_id.tolist(), ['a', 'b', 'c'])
        self.assertEqual(result.product_name.tolist(), ['First', 'Second', None])
        self.assertEqual(stats[0]['unmatched'], 1)
        with self.assertRaisesRegex(ValueError, 'duplicate lookup keys'):
            enrich(series, [(spec, pd.concat([lookup, lookup.iloc[:1]]))])

    def test_codes_and_missing_values_survive_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'lookup.tsv'
            path.write_text('code\tlabel\n00\tNA\n01\t\n')
            table = read_table(path)
            self.assertEqual(table.code.tolist(), ['00', '01'])
            self.assertEqual(table.label.tolist(), ['NA', ''])
            write_json(Path(directory) / 'out.json', table.to_dict('records'))
            self.assertEqual(len(json.loads((Path(directory) / 'out.json').read_text())), 2)

    def test_trailing_tabs_and_different_seasonal_key_name(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'seasonal.tsv'
            path.write_text('seasonal_code\tseasonal_text\nS\tSeasonally Adjusted\t\nU\t\t\n')
            lookup = read_table(path)
            result, stats = enrich(pd.DataFrame({'seasonal': ['S', 'U', '']}), [(
                {'file': 'example.seasonal', 'keys': ['seasonal'],
                 'lookup_keys': ['seasonal_code']}, lookup)])
            self.assertEqual(result.seasonal.tolist(), ['S', 'U', ''])
            self.assertEqual(result.seasonal_text.tolist(), ['Seasonally Adjusted', None, None])
            self.assertEqual(stats[0]['blank_codes'], 1)
            self.assertEqual(stats[0]['matched_without_description'], 1)
            path.write_text('code\tlabel\n00\tName\tUnexpected\n')
            with self.assertRaisesRegex(ValueError, 'extra nonempty fields'):
                read_table(path)

    def test_sample_validation_and_quarterly_missing_observation(self):
        meta = pd.DataFrame([
            {'series_id': 'one', 'begin_year': '2020', 'begin_period': 'Q01',
             'end_year': '2025', 'end_period': 'Q04'},
            {'series_id': 'dup', 'begin_year': '2020', 'begin_period': 'Q01',
             'end_year': '2025', 'end_period': 'Q04'},
            {'series_id': 'dup', 'begin_year': '2020', 'begin_period': 'Q01',
             'end_year': '2025', 'end_period': 'Q04'}])
        observations = pd.DataFrame([
            {'series_id': s, 'year': y, 'period': p, 'value': v}
            for s, y, p, v in [('one', '2024', 'Q01', '-'),
                               ('one', '2024', 'Q01', '3'),
                               ('one', '2024', 'Q02', 'NaN'),
                               ('one', '2019', 'Q01', '3'),
                               ('one', '2024', 'Q09', '3'),
                               ('unknown', '2024', 'Q01', '3'),
                               ('dup', '2024', 'Q01', '3')]])
        samples, issues = attach_samples(observations, meta, 5, 6)
        self.assertEqual(len(samples), 1)
        self.assertEqual(samples[0]['observations'][0]['raw_value'], '-')
        self.assertIsNone(samples[0]['observations'][0]['value'])
        self.assertIsNone(samples[0]['observations'][1]['value'])
        self.assertEqual(samples[0]['observations'][0]['period_kind'], 'quarterly')
        self.assertEqual(len(issues['duplicate_observations']), 1)
        self.assertEqual(len(issues['invalid_observations']), 2)
        self.assertEqual(issues['unknown_series_ids'], ['unknown'])
        self.assertEqual(issues['ambiguous_series_ids'], ['dup'])

    def test_stream_stops_and_closes_without_entire_history(self):
        class Response:
            closed = False
            def __enter__(self): return self
            def __exit__(self, *args): self.closed = True
            def raise_for_status(self): pass
            def iter_lines(self, **kwargs):
                yield b'series_id\tyear\tperiod\tvalue'
                yield b'a\t2024\tM01\t1'
                yield b'b\t2024\tM01\t2'
                raise AssertionError('Read beyond requested sample')
        class Session:
            def get(self, *args, **kwargs): return response
        response = Response()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'sample.tsv'
            sample_prefix(Session(), 'https://example.test/source', path, 2, 1, 20)
            self.assertTrue(response.closed)
            self.assertEqual(len(read_table(path)), 2)
            self.assertTrue(json.loads(path.with_name('sample.tsv.source.json').read_text())['partial'])


if __name__ == '__main__':
    unittest.main()
