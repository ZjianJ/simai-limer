import sys
import unittest
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'tools'))
from run_random_split_baselines import schedule, oracle, write_csv
from split_capacity import read_capacity, rate_at


class RandomScheduleTest(unittest.TestCase):
    def setUp(self):
        self.links = [dict(link_class='ACCESS', src_type='HOST', src_node=str(i),
                           src_port=str(p), link_id=f'L{i}-{20+i%4+(p-2)*4}')
                      for i in range(16) for p in (2, 3)]

    def test_reproducible_and_order_independent(self):
        a = schedule(self.links, 8, 80, 1000000, 100000000, 42)
        self.assertEqual(a, schedule(list(reversed(self.links)), 8, 80, 1000000, 100000000, 42))
        self.assertNotEqual(a, schedule(self.links, 8, 80, 1000000, 100000000, 43))
        self.assertEqual(len({r['target_link_id'] for r in a}), 8)
        self.assertEqual(len({r['severity'] for r in a}), 8)
        for r in a:
            self.assertTrue(0 <= r['severity'] <= .8)
            self.assertAlmostEqual(r['parameter_after'] + r['severity'], 1)
            self.assertEqual(r['start_time_ns'], 1000000)

    def test_invalid(self):
        for count, maximum, start, end in [(33,80,1,2),(-1,80,1,2),(8,100,1,2),(8,float('nan'),1,2),(8,80,2,2)]:
            with self.assertRaises(ValueError):
                schedule(self.links, count, maximum, start, end, 42)
        with self.assertRaises(ValueError):
            schedule(self.links[:-1], 8, 80, 1, 2, 42)

    def test_oracle_changes_only_at_boundaries(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fields = ('src','dst','rail','start_ns','capacity_bps')
            for name, rate in [('healthy',100),('degraded',25)]:
                write_csv(root/name, fields, [dict(src=0,dst=4,rail=r,start_ns=0,capacity_bps=rate) for r in (0,1)])
            oracle(root/'healthy', root/'degraded', 1000, 10000, root/'oracle')
            points = read_capacity(root/'oracle')[0,4,0]
            self.assertEqual([rate_at(points,t) for t in (0,999,1000,9999,10000)], [100,100,25,25,100])
            self.assertEqual(read_capacity(root/'healthy')[0,4,0], [(0,100)])


if __name__ == '__main__':
    unittest.main()
