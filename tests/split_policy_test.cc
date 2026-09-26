#include "limer_split_policy.h"
#include <cassert>
#include <iostream>
using limer::SplitPolicy;
static void calibrate(SplitPolicy& p,double a=30e9,double b=100e9) {
  p.capacity[{0,4,0}]={{0,a}}; p.capacity[{0,4,1}]={{0,b}};
  p.ValidateCapacity();
}
int main() {
  SplitPolicy single("B0"); assert(single.Select(0,4,1000,0)==1);
  SplitPolicy equal("B1");
  for(int i=0;i<101;++i) {int r=equal.Select(0,4,1000,0);equal.Reserve(0,4,r,1000);}
  assert(equal.state[std::make_pair(0u,0u)].assigned==51000);
  SplitPolicy weighted("B2"); calibrate(weighted);
  for(int i=0;i<130;++i) {int r=weighted.Select(0,4,1000,0);weighted.Reserve(0,4,r,1000);}
  assert(weighted.state[std::make_pair(0u,0u)].assigned==30000);
  weighted.capacity[{0,4,0}].push_back({100,0});
  assert(weighted.Rate(0,4,0,200)==30e9); // static policy cannot use future changes
  SplitPolicy current("B3"); calibrate(current);
  current.capacity[{0,4,0}].push_back({100,0});
  assert(current.Rate(0,4,0,99)==30e9);
  assert(current.Select(0,4,1000,100)==1);
  SplitPolicy queued("B4"); calibrate(queued,80e9,40e9);
  queued.Reserve(0,4,0,9000000);queued.Reserve(0,4,1,1000000);
  assert(queued.Select(0,4,1000000,0)==1);
  queued.Complete(0,4,1,1000000);
  bool threw=false;try {queued.Complete(0,4,1,1);}catch(const std::runtime_error&) {threw=true;}
  assert(threw);
  SplitPolicy ewma("B6");
  ewma.capacity[{0,4,0}]={{0,std::numeric_limits<double>::quiet_NaN()}};
  assert(ewma.Rate(0,4,0,0)==100e9); // no oracle read, even if a table exists
  ewma.Observe(0,0,1,80e9,1,true,true);
  ewma.Observe(0,0,2,40e9,1,true,true);
  assert(std::abs(ewma.Rate(0,4,0,2)-72e9)<1);
  ewma.Observe(0,0,3,0,0,false,true); // idle does not become zero capacity
  assert(std::abs(ewma.Rate(0,4,0,3)-72e9)<1);
  ewma.Observe(0,0,4,0,0,false,false);
  assert(ewma.Select(0,4,1000,4)==1);
  ewma.Observe(0,1,4,0,0,false,false);
  assert(ewma.Select(0,4,1000,4)==-1);
  threw=false;try{SplitPolicy bad("B5");}catch(const std::invalid_argument&){threw=true;}
  assert(threw);
  weighted.capacity[{0,4,0}].push_back({0,2});
  threw=false;try{weighted.ValidateCapacity();}catch(const std::invalid_argument&){threw=true;}
  assert(threw);
  for(const auto& name:{"B7","B8","B9"}) {
    SplitPolicy p(name);
    assert(p.Select(0,4,1000,0)==0);p.Reserve(0,4,0,1000);
    assert(p.Select(0,4,1000,0)==1);p.Reserve(0,4,1,1000);
    assert(!p.HasChunkFeedback(0));
    p.ChunkComplete(0,0,1000,20000);
    assert(p.HasChunkFeedback(0));
    assert(p.Select(0,4,1000,20000)==0); // don't block fast rail on slow probe
    p.ChunkComplete(0,1,1000,80000);
    p.chunk_credit.clear();
    int count=0;
    for(int i=0;i<100;++i) count+=p.Select(0,4,1000,100000)==0;
    assert(count==80);
    p.ChunkComplete(0,0,1000,80000);
    assert(p.Rate(0,4,0,100000)==(p.name=="B7" ? 400000000.:100000000.));
    p.state[{0,0}].up=false;
    assert(p.Select(0,4,1000,200000)==1);
  }
  SplitPolicy aging("B9");
  aging.ChunkComplete(0,1,1000,10000);
  aging.ResetAgeCaps(0); // idle rail retains last completed estimate
  assert(aging.Rate(0,4,1,50000)==800000000.);
  aging.AgeSample(0,1,1000,5000); // ordinary incomplete chunk: no penalty
  assert(aging.Rate(0,4,1,50000)==800000000.);
  aging.AgeSample(0,1,1000,20000);
  assert(aging.Rate(0,4,1,50000)==400000000.);
  assert(aging.Rate(0,4,1,100000)==400000000.); // no timer/Select-time decay
  assert(aging.Rate(1,4,1,50000)==0); // no cross-source contamination
  aging.ResetAgeCaps(0);aging.AgeSample(0,1,1000,0);
  assert(aging.Rate(0,4,1,50000)==800000000.);
  aging.AgeSample(0,1,500,20000); // use each witness's actual size
  assert(aging.Rate(0,4,1,50000)==200000000.);
  aging.ResetAgeCaps(0);aging.ChunkComplete(0,1,1000,5000);
  assert(aging.Rate(0,4,1,50000)==1600000000.);
  std::cout<<"split policy behavior tests passed\n";
}
