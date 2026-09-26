#!/usr/bin/env python3
"""Paired B8/B9 replay of frozen random faults plus a healthy control."""
import argparse
import json
import resource
import shutil
from pathlib import Path
from types import SimpleNamespace
from run_split_baselines import ROOT, run_one, rows, digest
from simulator_runtime_bundle import seal_runtime_bundle


def diagnostics(directory, faults):
    affected={r['target_link_id'] for r in rows(faults)} if faults else set()
    links={(int(r['src_node']),int(r['src_port'])-2):r['link_id']
           for r in rows(directory/'link_map.csv') if r['link_class']=='ACCESS' and r['src_type']=='HOST'}
    assigned=0
    for e in rows(directory/'split_events.csv'):
        if e['event']=='ASSIGN' and int(e['timestamp_ns'])>=1000000:
            src,dst,rail=(int(e[k]) for k in ('src','dst','rail'))
            if links[src,rail] in affected or links[dst,rail] in affected:
                assigned+=int(e['bytes'])
    result={'post_fault_assigned_bytes_on_affected_access_paths':assigned}
    path=directory/'chunk_age_feedback.csv'
    if path.exists():
        data=rows(path)
        limited=[r for r in data if float(r['used_bps'])<float(r['raw_bps'])*(1-1e-5)]
        cross=[r for r in limited if r['rail']!=r['trigger_rail']]
        result.update(age_snapshot_rows=len(data),limited_rows=len(limited),cross_rail_limited_rows=len(cross),
                      first_post_fault_cross_rail_limit_ns=min((int(r['timestamp_ns']) for r in cross
                          if int(r['timestamp_ns'])>=1000000),default=None))
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary',type=Path,required=True)
    p.add_argument('--topology',type=Path,required=True)
    p.add_argument('--frozen',type=Path,required=True)
    p.add_argument('--out',type=Path,required=True)
    opt=p.parse_args()
    root=opt.out.resolve();root.mkdir(parents=True,exist_ok=False)
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))
    bundle=seal_runtime_bundle(root,opt.binary.resolve())
    args=SimpleNamespace(binary=bundle.executable,bundle=bundle,topology=opt.topology.resolve(),
        workload=ROOT/'limer/configs/microAllReduce_16rank_split_64mib.txt',
        config=ROOT/'limer/configs/SimAI.baseline.conf',threads=1,chunk_bytes=65536,
        max_active=8,sample_us=1000,horizon_ns=100000000,timeout=180)
    original=json.loads((opt.frozen/'summary.json').read_text())
    report={'definition':'B9 ACK-triggered minimum censored per-chunk completion-rate bound; no timer, GP or oracle',
            'source_summary_sha256':digest(opt.frozen/'summary.json'),'scenarios':{}}
    def save(): (root/'summary.json').write_text(json.dumps(report,indent=2)+'\n')
    for scenario in ('healthy','42','43','44'):
        args.out=root/scenario;args.out.mkdir();args.faults=None
        if scenario!='healthy':
            args.faults=args.out/'faults.csv'
            shutil.copyfile(opt.frozen/f'seed_{scenario}'/'faults.csv',args.faults)
            assert digest(args.faults)==original['seeds'][scenario]['fault_sha256']
        results={};report['scenarios'][scenario]=results
        for policy in ('B8','B9'):
            result=run_one(args,policy,policy)
            if result['pass']:
                result.update(diagnostics(args.out/policy,args.faults))
                if args.faults:
                    applied=[r for r in rows(args.out/policy/'fault_application_telemetry.csv') if r['transition']=='apply']
                    expected={r['fault_id']:r for r in rows(args.faults)}
                    result['fault_exact']=len(applied)==8 and {r['fault_id'] for r in applied}==set(expected) and all(
                        r['status']=='APPLIED' and int(r['actual_ns'])==1000000
                        and r['target_link_id']==expected[r['fault_id']]['target_link_id']
                        and abs(float(r['parameter_after'])-float(expected[r['fault_id']]['parameter_after']))<1e-10 for r in applied)
                if policy=='B8' and scenario!='healthy':
                    result['matches_previous_B8']=result['workload_finish_ns']==original['seeds'][scenario]['results']['B8']['workload_finish_ns']
            results[policy]=result;save()
            print(scenario,policy,'pass=',result['pass'],'finish_ns=',result.get('workload_finish_ns'),flush=True)
    values=[r for scenario in report['scenarios'].values() for r in scenario.values()]
    report['pass']=all(r['pass'] and r.get('fault_exact',True) and r.get('matches_previous_B8',True) for r in values)
    report['same_payload']=len({sum(r['acked_payload_bytes']) for r in values if r['pass']})==1
    save()
    return 0 if report['pass'] and report['same_payload'] else 1


if __name__=='__main__':
    raise SystemExit(main())
