import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('benchmark', Path(__file__).with_name('acceptance-benchmark.py'))
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)

class BenchmarkTest(unittest.TestCase):
    def test_nearest_rank_and_tail_are_not_averaged(self):
        self.assertEqual(benchmark.distribution([1] * 19 + [1000])['p95_ms'], 1)
        self.assertEqual(benchmark.distribution([1] * 19 + [1000])['p99_ms'], 1000)
        self.assertEqual(benchmark.distribution([7])['p95_ms'], 7)
        with self.assertRaises(ValueError):
            benchmark.distribution([])

    def test_failure_is_retained_and_invalidates_window(self):
        rows = [{'status': 202, 'latency_ms': 1}, {'status': 503, 'latency_ms': 1000}]
        result = benchmark.summarize(rows, 2)
        self.assertEqual(result['accepted'], 1)
        self.assertEqual(result['errors'], 1)
        self.assertEqual(result['p95_ms'], 1000)
        self.assertEqual(result['accepted_per_second'], .5)

if __name__ == '__main__':
    unittest.main()
