import csv
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

TOOLS = Path(__file__).resolve().parents[1] / "tools"
sys.path.insert(0, str(TOOLS))
from split_capacity import future_bound, read_capacity
from run_split_baselines import audit, audit_feedback, calibration, validate_static_faults


class CapacityTests(unittest.TestCase):
    def test_probe_feedback_requires_real_simultaneous_equal_chunks(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); path=root/'chunk_feedback.csv'
            feedback=[];events=[]
            for src in range(16):
                for rail,elapsed in [(0,20000),(1,80000)]:
                    event=dict(event='ASSIGN',src=str(src),dst=str((src+4)%16),
                               rail=str(rail),bytes='65536',timestamp_ns='100')
                    events.append(event);events.append(dict(event,event='ACK_COMPLETE'))
                    rate=65536*8e9/elapsed
                    feedback.append(dict(policy='B7',src=src,rail=rail,bytes=65536,
                                         first_tx_ns=100,completion_ns=100+elapsed,elapsed_ns=elapsed,
                                         measured_bps=rate,used_bps=rate,rail_samples=1))
            with path.open('w',newline='') as f:
                writer=csv.DictWriter(f,fieldnames=list(feedback[0]));writer.writeheader();writer.writerows(feedback)
            result=audit_feedback(root,events)
            self.assertEqual(len(result['probes']),16)
            self.assertEqual(result['probes'][0]['A_weight'],0.8)
            events[2]['timestamp_ns']='101'
            with self.assertRaises(ValueError): audit_feedback(root,events)
            events[2]['timestamp_ns']='100';events[2]['bytes']='32768'
            with self.assertRaises(ValueError): audit_feedback(root,events)
    def test_static_calibration_rejects_future_change(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/"fault.csv"
            header="fault_type,start_time_ns,end_time_ns,recovery_delay_ns,parameter_after,target_link_id\n"
            path.write_text(header+"service_degradation,0,1000,0,0.25,L0-24\n")
            validate_static_faults(path,1000)
            with self.assertRaises(ValueError): validate_static_faults(path,1001)
            path.write_text(header+"service_degradation,1,1000,0,0.25,L0-24\n")
            with self.assertRaises(ValueError): validate_static_faults(path,1000)
    def test_constant_independent_rails(self):
        series = {(0,4,0): [(0,30e9)], (0,4,1): [(0,100e9)]}
        result = future_bound(series, 0, 4, 130000000)
        self.assertAlmostEqual(result["duration_ns"], 8000000)
        self.assertEqual(result["rail_bytes"], [30000000,100000000])

    def test_future_recovery_and_outage(self):
        series = {(0,4,0): [(0,0),(1000,8e9)], (0,4,1): [(0,0)]}
        result = future_bound(series,0,4,1000)
        self.assertEqual(result["finish_ns"],2000)
        series[0,4,0]=[(0,0)]
        self.assertFalse(future_bound(series,0,4,1000)["reachable"])
        self.assertEqual(future_bound(series,0,4,0)["finish_ns"],0)

    def test_future_bound_never_uses_past_capacity(self):
        series = {(0,4,0): [(0,8e9),(1000,0)], (0,4,1): [(0,0)]}
        self.assertFalse(future_bound(series,0,4,1,1000)["reachable"])

    def test_reject_bad_capacity(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/"capacity.csv"
            for values in ("0,4,0,1,10", "0,4,0,0,nan", "0,4,2,0,10",
                           "0,4,0,0,10\n0,4,0,0,20"):
                path.write_text("src,dst,rail,start_ns,capacity_bps\n"+values+"\n")
                with self.assertRaises(ValueError): read_capacity(path)

    def test_ack_audit_rejects_duplicate_or_incomplete(self):
        with tempfile.TemporaryDirectory() as temp:
            directory=Path(temp)
            (directory/"run_lifecycle.csv").write_text(
                "status,finished_ranks,world_size,actual_ns\nWORKLOAD_COMPLETE,16,16,30\n")
            header="timestamp_ns,event,src,dst,sport,rail,bytes,logical_flow_id,chunk_offset,flow_bytes\n"
            assign="10,ASSIGN,0,4,100,0,1024,1,0,1024\n"
            complete="20,ACK_COMPLETE,0,4,100,0,1024,1,0,1024\n"
            path=directory/"split_events.csv"
            path.write_text(header+assign+complete)
            self.assertTrue(audit(directory)["pass"])
            path.write_text(header+assign)
            self.assertFalse(audit(directory)["pass"])
            path.write_text(header+assign+complete+complete)
            with self.assertRaises(ValueError): audit(directory)
            path.write_text(header+assign+complete+assign.replace("10,", "21,").replace(",1,0,1024",",2,0,1024")+complete.replace("20,", "25,").replace(",1,0,1024",",2,0,1024"))
            self.assertTrue(audit(directory)["pass"]) # legal port reuse after ACK

    def test_calibration_uses_union_not_sum_of_concurrent_durations(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            paths=[]
            for r in (0,1):
                path=root/str(r); path.mkdir(); paths.append(path)
                (path/"split_events.csv").write_text(
                    "timestamp_ns,event,src,dst,sport,rail,bytes\n"
                    f"0,ASSIGN,0,4,10,{r},1000\n0,ASSIGN,0,4,11,{r},1000\n"
                    f"1000,ACK_COMPLETE,0,4,10,{r},1000\n1000,ACK_COMPLETE,0,4,11,{r},1000\n")
            out=root/"capacity.csv"
            calibration(paths,out)
            self.assertEqual(read_capacity(out)[0,4,0],[(0,16e9)])


if __name__ == "__main__": unittest.main()
