import csv
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from run_split_baselines import audit_age_feedback


class AgeAuditTest(unittest.TestCase):
    def fixture(self):
        feedback=[]
        for sport,rail,start,end in [(11,1,0,5),(10,0,0,20),(12,1,5,45)]:
            feedback.append(dict(src=0,dst=4,sport=sport,rail=rail,bytes=100,
                first_tx_ns=start,completion_ns=end,timestamp_ns=end,measured_bps=100*8e9/(end-start)))
        raw=[0,0];records=[]
        for e in feedback:
            raw[e['rail']]=e['measured_bps']
            for rail in (0,1):
                witnesses=[w for w in feedback if w['rail']==rail and w['first_tx_ns']<e['timestamp_ns']<w['completion_ns']]
                w=min(witnesses,key=lambda w:w['bytes']/(e['timestamp_ns']-w['first_tx_ns'])) if witnesses else None
                age=e['timestamp_ns']-w['first_tx_ns'] if w else 0
                cap=100*8e9/age if age else 0
                records.append(dict(timestamp_ns=e['timestamp_ns'],src=0,trigger_dst=4,
                    trigger_sport=e['sport'],trigger_rail=e['rail'],rail=rail,raw_bps=raw[rail],
                    used_bps=min(raw[rail],cap) if age else raw[rail],witness_dst=4 if w else 0,
                    witness_sport=w['sport'] if w else 0,witness_bytes=100 if w else 0,
                    witness_first_tx_ns=w['first_tx_ns'] if w else 0,age_ns=age,cap_bps=cap))
        return feedback,records

    def check(self,feedback,records):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)
            with (p/'chunk_age_feedback.csv').open('w',newline='') as f:
                writer=csv.DictWriter(f,fieldnames=list(records[0]));writer.writeheader();writer.writerows(records)
            return audit_age_feedback(p,feedback)

    def test_valid_and_idle(self):
        feedback,records=self.fixture()
        self.assertLess(records[3]['used_bps'],records[3]['raw_bps'])
        self.assertEqual(records[-2]['used_bps'],records[-2]['raw_bps'])
        self.assertEqual(len(self.check(feedback,records)),6)

    def test_wrong_bound_rejected(self):
        feedback,records=self.fixture();records[3]['cap_bps']*=2
        with self.assertRaises(ValueError):self.check(feedback,records)

    def test_non_ack_trigger_rejected(self):
        feedback,records=self.fixture();records[3]['timestamp_ns']+=1
        with self.assertRaises(ValueError):self.check(feedback,records)


if __name__=='__main__':unittest.main()
