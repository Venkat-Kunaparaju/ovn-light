/* SPDX-License-Identifier: Apache-2.0 */
'use strict';
const testDialog = $('traffic-test-dialog');
let testTimer, testSequence = 0, testEnabled = false, testRunning = false, testContext = '';

let catalog, testSubmitting = false;
const testProfile = () => catalog?.profiles.find(p=>p.id===$('traffic-test-profile').value);
const testCase = () => catalog?.cases.find(c=>c.id===$('traffic-test-case').value);

function fillTestChoices(id, choices, previous) {
  const el=$(id);
  el.replaceChildren(...choices.map(c=>new Option(c.name,c.id)));
  if(choices.some(c=>c.id===previous)) el.value=previous;
}

function selectTestProfile(previousCase,src,dst) {
  const profile=testProfile();
  if(!profile)return;
  const choices=catalog.cases.filter(c=>!c.service || profile.service);
  const old=catalog.cases.find(c=>c.id===previousCase);
  const fallback=choices.find(c=>c.sameNode===Boolean(old?.sameNode))?.id;
  fillTestChoices('traffic-test-case',choices,choices.some(c=>c.id===previousCase)?previousCase:fallback);
  configureTestMode();
  selectTestNodes(src,dst);
}

function configureTestMode() {
  const profile=testProfile();
  if(!profile)return;
  const iperf=profile.id.startsWith('iperf-');
  const bandwidth=iperf || profile.rdma || profile.id==='netperf-tcp-stream';
  const mode=$('test-mode');
  mode.options[0].textContent=iperf?'Connectivity check':'Measure only';
  if(!bandwidth)mode.value='connectivity';
  $('test-mode-label').hidden=!bandwidth;
  const evaluate=bandwidth && mode.value==='throughput';
  for(const [prefix,enabled] of [['test-min',evaluate],['test-rate',evaluate && iperf && catalog?.runner!=='helm']]) {
    $(prefix+'-label').hidden=!enabled;
    $(prefix).disabled=!enabled;
  }
  $('traffic-test-description').textContent=catalog?.runner==='helm' ? profile.description : evaluate && iperf ?
    'One paced stream for 120 seconds. The upstream evaluator compares the measured throughput with your minimum.' : profile.description;
  updateTestForm();
}

function selectTestNodes(src,dst) {
  const test=testCase(), profile=testProfile();
  if(!test||!profile)return;
  const device=profile.rdma?catalog.rdmaResource:catalog.device;
  const eligible=catalog.nodes.filter(n=>!device || (profile.rdma?n.rdmaDevices:n.devices)>=(test.sameNode?2:1));
  const choices=eligible.map(n=>({id:n.name,name:n.name}));
  fillTestChoices('traffic-test-source',choices,src);
  updateTestDestination(dst);
}

function updateTestDestination(dst) {
  const src=$('traffic-test-source').value;
  const single=testCase()?.sameNode;
  const choices=Array.from($('traffic-test-source').options).filter(n=>single?n.value===src:n.value!==src)
    .map(n=>({id:n.value,name:n.textContent}));
  fillTestChoices('traffic-test-destination',choices,dst);
  $('traffic-test-destination').disabled=Boolean(single);
  updateTestForm();
}

function updateTestForm() {
  const src = $('traffic-test-source').value, dst = $('traffic-test-destination').value;
  const reason = testContext || (!testEnabled ? 'Traffic tests require a live, write-enabled server with the test runner installed.' :
    testRunning ? 'One test is running. Follow it in Traffic test history on Hardware Offload.' :
    testProfile()?.rdma && !catalog.rdmaResource ? 'This test requires an RDMA device resource configured on the runner.' :
    !src || !dst ? 'No eligible node pair for this test case. Check node readiness and device capacity.' : '');
  $('traffic-test-message').textContent = reason;
  const invalidMinimum=!$('test-min').disabled && !($('test-min').valueAsNumber>0);
  $('traffic-test-submit').disabled = Boolean(reason) || invalidMinimum || (!$('test-rate').disabled && !$('test-rate').validity.valid) || testSubmitting || !testCase();
}

function testResult(run, root=$('traffic-test-result')) {
  const expanded=root.dataset.run===run?.id ? Array.from(root.querySelectorAll('details[open][data-flow]'),d=>d.dataset.flow) : [];
  root.replaceChildren();
  if (!run) return;
  root.dataset.run=run.id;
  const profile=catalog?.profiles.find(p=>p.id===(run.test||'iperf-tcp'));
  const scenario=catalog?.cases.find(c=>c.id===(run.case||'POD_TO_POD_DIFF_NODE'));
  root.append(text('h3',run.finished ? 'Completed run' : 'Current test'), text('strong',[run.testName||profile?.name||run.test,run.caseName||scenario?.name||run.case].filter(Boolean).join(' · ')),
    text('p',run.source.node+' → '+run.destination.node+' · '+new Date(run.started).toLocaleString()));
  const traffic = run.traffic;
  if (traffic?.delivered) {
    const stats=[];
    if(traffic.bytes!=null)stats.push(count(traffic.bytes)+' bytes received');
    if(traffic.bps!=null)stats.push((traffic.bps/1e6).toFixed(2)+' Mbit/s');
    if(traffic.transactionsPerSecond!=null)stats.push(traffic.transactionsPerSecond.toFixed(2)+' transactions/s');
    if(traffic.lostPercent!=null)stats.push(traffic.lostPercent.toFixed(2)+'% loss');
    if(traffic.jitterMs!=null)stats.push(traffic.jitterMs.toFixed(3)+' ms jitter');
    root.append(badge(run.test==='iperf-tcp'?'TCP delivery verified':'Traffic delivered','green'),text('p',stats.join(' · ')));
    if((scenario?.service || run.case?.includes('CLUSTER_IP')) && traffic.targetIP)root.append(text('p','Service IP '+traffic.targetIP+' → server '+traffic.destinationIP));
  } else if (traffic) root.append(text('strong','Test failed'),text('p',traffic.error || 'No completed transfer.'));
  else root.append(text('p','Test in progress. Waiting for the runner and collector samples.'));
  if(run.minimumGbps!=null){
    const e=traffic?.evaluation;
    root.append(text('h3','Throughput evaluation'),text('p','Minimum '+run.minimumGbps+' Gbit/s'+(run.senderMbps?' · sender rate '+run.senderMbps+' Mbit/s':'')));
    if(e){
      root.append(badge(e.passed?'Threshold passed':'Below threshold',e.passed?'green':'red'));
      root.append(text('p',[['TX',e.txGbps],['RX',e.rxGbps]].filter(([,v])=>v!=null).map(([label,v])=>label+' '+v+' Gbit/s').join(' · ')));
      if(e.reason)root.append(text('p',e.reason,'note'));
    }else root.append(text('p',run.finished?(traffic?.error || 'No throughput measurement to evaluate.'):'Awaiting measurement.','note'));
  }else if(traffic?.delivered && run.test?.startsWith('iperf-')){
    root.append(text('p',run.testNamespace?'Connectivity check · no throughput threshold.':'Connectivity check · sender rate '+(run.senderMbps||1)+' Mbit/s · no throughput threshold.','note'));
  }
  if(traffic?.offload?.length){
    root.append(text('h3','TFT hardware offload validation'));
    for(const result of traffic.offload){
      root.append(badge((result.role?result.role+' · ':'')+(result.passed?'Passed':'Failed'),result.passed?'green':'red'));
      root.append(text('p',[['RX',result.rx],['TX',result.tx]].map(([direction,counter])=>
        direction+': '+(counter?.packets!=null?counter.packets.toLocaleString()+' packets':counter?'unavailable (counter reset)':'unavailable')).join(' · ')));
      if(result.reason)root.append(text('p',result.reason));
    }
    root.append(text('p','Packet counts are representor counter increases during the test. Before and after values are in the TFT runner log.','note'));
  }
  if(traffic?.log){
    const log=text('details','');
    log.append(text('summary','TFT runner log'),text('pre',traffic.log));
    root.append(log);
  }
  if (run.drop) root.append(text('p','Matching policy drop · '+run.drop));
  for (const n of run.nodes || []) {
    const card = text('section','','record');
    card.append(text('strong',n.node));
    for(const [role,endpoint] of [['Client',run.source],['Server',run.destination]]){
      const ip=endpoint.ip || (role==='Client'?traffic?.sourceIP:traffic?.destinationIP);
      if(endpoint.node===n.node && ip)card.append(text('p', [role,endpoint.pod,ip].filter(Boolean).join(' · ')));
    }
    if (n.state === 'disabled') card.append(text('p','Hardware offload disabled'));
    if(run.test?.startsWith('ib-'))card.append(text('p','RDMA data uses a separate datapath.'));
    else renderTestFlows(card,n,run.finished);
    for (const p of n.paths || []) {
      const detail=text('details','');
      detail.append(text('summary',(offloadLabels[p.status] || p.status)+' · '+(p.tuple.sourceIP === run.source.ip ? 'client → server' : 'server → client')+(p.issue?' · persistent issue':'')),
        text('p',p.summary+' · '+new Date(p.observedAt).toLocaleTimeString()));
      for (const s of p.stages || []) {
        detail.append(text('p','recirc '+s.recirc+' · '+s.inPort+' · '+s.status+' · partial packets '+(s.partialDelta == null?'Δ unavailable':'+'+count(s.partialDelta))+(s.reason?' · '+s.reason:'')));
      }
      for (const c of p.conntrack || []) detail.append(text('p','CT zone '+c.zone+' · '+(c.direction||'direction unknown')+' · '+c.status+' · original '+(c.original||'unknown')+' · reply '+(c.reply||'unknown')+(c.reason?' · '+c.reason:'')));
      for (const warning of p.warnings || []) detail.append(text('p',warning,'note'));
      card.append(detail);
    }
    if (n.limited) card.append(text('p','Additional evidence exceeded the result limit.','note'));
    root.append(card);
  }
  for(const detail of root.querySelectorAll('details[data-flow]'))detail.open=expanded.includes(detail.dataset.flow);
}

async function refreshTest(seq) {
  try {
    const status=await api('traffic-test');
    if (seq!==testSequence || !testDialog.open) return;
    testEnabled=status.enabled;
    testRunning=Boolean(status.run && !status.run.finished);
    updateTestForm();testResult(status.run);
    if (testRunning) testTimer=setTimeout(()=>refreshTest(seq),4000);
  } catch (e) {
    if(seq===testSequence) $('traffic-test-message').textContent=e.message;
  }
}
async function openTrafficTest(src,dst,network) {
  clearTimeout(testTimer);testEnabled=false;testRunning=false;testSubmitting=false;catalog=null;
  for(const id of ['profile','case','source','destination']) $('traffic-test-'+id).replaceChildren();
  $('traffic-test-description').textContent='';
  testContext=network && network!=='default' ? 'This test supports the default network. The selected link uses '+network+'.' : '';
  $('traffic-test-result').replaceChildren();$('traffic-test-submit').disabled=true;
  $('traffic-test-message').textContent='Loading tests and nodes…';testDialog.showModal();
  const seq=++testSequence;
  try {
    const options=await api('traffic-test/options');
    if(seq!==testSequence)return;
    catalog=options;
    fillTestChoices('traffic-test-profile',catalog.profiles,'iperf-tcp');
    $('test-mode').value='connectivity';$('test-rate').value='1';$('test-min').value='';
    selectTestProfile(src && src===dst ? 'POD_TO_POD_SAME_NODE' : 'POD_TO_POD_DIFF_NODE',src,dst);
    await refreshTest(seq);
  }catch(e){if(seq===testSequence)$('traffic-test-message').textContent=e.message}
}
$('offload-run-test').onclick=()=>openTrafficTest($('offload-node').value,'',$('offload-network').value);
$('test-mode').onchange=configureTestMode;
$('test-min').oninput=$('test-rate').oninput=updateTestForm;
$('traffic-test-profile').onchange=()=>selectTestProfile($('traffic-test-case').value,$('traffic-test-source').value,$('traffic-test-destination').value);
$('traffic-test-case').onchange=()=>selectTestNodes($('traffic-test-source').value,$('traffic-test-destination').value);
$('traffic-test-source').onchange=()=>updateTestDestination($('traffic-test-destination').value);
$('traffic-test-destination').onchange=updateTestForm;
testDialog.addEventListener('close',()=>{clearTimeout(testTimer);testSequence++});
$('traffic-test-form').onsubmit=async e=>{
  e.preventDefault();if ($('traffic-test-submit').disabled)return;
  $('traffic-test-submit').disabled=true;
  testSubmitting=true;
  const seq=++testSequence;
  try {
    const request={test:testProfile().id,case:testCase().id,sourceNode:$('traffic-test-source').value,destinationNode:$('traffic-test-destination').value,network:'default'};
    if(!$('test-min').disabled)request.minimumGbps=$('test-min').valueAsNumber;
    if(!$('test-rate').disabled)request.senderMbps=$('test-rate').valueAsNumber;
    const status=await api('traffic-test',request);
    loadTestHistory();
    if(seq!==testSequence)return;
    testSubmitting=false;testRunning=true;updateTestForm();testResult(status.run);refreshTest(seq);
  } catch(err) {
    if(seq===testSequence){testSubmitting=false;updateTestForm();$('traffic-test-message').textContent=err.message}
  }
};

function renderTestFlows(card,node,finished){
  const flows=node.flows||[];
  if(!flows.length){
    card.append(text('p',finished?'No matching datapath flows were captured during this run.':'Awaiting a collector sample for these test pods.','note'));
    return;
  }
  card.append(text('h4','Datapath flows'),text('p','Shared matches include wildcarded fields. Counters cover all packets matching the OVS rule.','note'));
  for(const status of new Set(flows.map(f=>f.status))){
    const group=text('details','');
    group.dataset.flow=node.node+'/'+status;
    const members=flows.filter(f=>f.status===status);
    group.append(text('summary',(offloadLabels[status]||status)+' · '+members.length+' '+(members.length===1?'flow':'flows')));
    for(const f of members){
      const line=text('details','','test-flow');
      line.dataset.flow=group.dataset.flow+'/'+f.id+'/'+f.direction;
      line.append(text('summary','recirc '+f.recirc+' · '+f.inPort+' · '+f.direction),
        text('p',(f.exact?'Exact test tuple':'Shared match on test pod port')+' · '+count(f.packets)+' packets · '+count(f.bytes)+' bytes · '+new Date(f.at).toLocaleTimeString(),'note'));
      if(f.partialPackets)line.append(text('p',count(f.partialPackets)+' partial-offload packets'));
      if(f.raw)line.append(text('pre',f.raw));
      group.append(line);
    }
    card.append(group);
  }
}

let testHistoryRequest=0, testHistoryTimer, selectedTestRun='';
function runOutcome(run){
  if(!run.finished)return ['Running','amber'];
  if(run.traffic?.delivered && run.traffic.evaluation)return run.traffic.evaluation.passed?['Threshold passed','green']:['Below threshold','red'];
  if(run.phase==='failed')return ['Failed','red'];
  return [run.test?.startsWith('iperf-')?'Connectivity verified':'Traffic delivered','green'];
}

async function loadTestHistory(){
  const compact=currentPage==='issues';
  if(!compact && currentPage!=='offload')return;
  const root=$(compact?'issues-test-runs':'offload-test-runs'), seq=++testHistoryRequest;
  try{
    const runs=await api('traffic-test/runs');
    if(seq!==testHistoryRequest)return;
    root.replaceChildren();
    root.classList.toggle('hidden',compact && !runs.length);
    const header=text('div','','section-title');
    header.append(text('h2',compact?'Recent traffic tests':'Traffic test history'));
    if(compact && runs.length>3)header.append(button('View all runs',()=>page('offload')));
    root.append(header);
    if(!runs.length)root.append(text('p','No traffic tests recorded.','note'));
    for(const run of runs.slice(0,compact?3:30)){
      const row=text('article','','record record-top'), label=text('div',''), [name,color]=runOutcome(run);
      const title=run.minimumGbps!=null?'Throughput check':run.test?.startsWith('iperf-')?'Connectivity check':'Traffic test';
      label.append(text('strong',title+' · '+(run.testName||run.test)+(run.minimumGbps!=null?' · minimum '+run.minimumGbps+' Gbit/s':'')),text('p',new Date(run.started).toLocaleString()+' · '+run.source.node+' → '+run.destination.node,'note'));
      row.append(label,badge(name,color),button(run.finished?'View result':'View progress',()=>openTestResult(run.id)));
      root.append(row);
    }
  }catch(e){if(seq===testHistoryRequest)root.replaceChildren(text('p',e.message,'error'))}
}

async function openTestResult(id){
  clearTimeout(testHistoryTimer);
  selectedTestRun=id;
  const dialog=$('run-history-dialog'), root=$('run-history-body');
  if(!dialog.open){root.replaceChildren(text('p','Loading test result…'));dialog.showModal()}
  try{
    const run=await api('traffic-test/runs/'+encodeURIComponent(id));
    if(!dialog.open || selectedTestRun!==id)return;
    testResult(run,root);
    if(!run.finished)testHistoryTimer=setTimeout(()=>openTestResult(id),4000);
    else loadTestHistory();
  }catch(e){if(dialog.open && selectedTestRun===id)root.replaceChildren(text('p',e.message,'error'))}
}
$('run-history-dialog').addEventListener('close',()=>{selectedTestRun='';clearTimeout(testHistoryTimer)});
