/* SPDX-License-Identifier: Apache-2.0 */
'use strict';
const testDialog = $('traffic-test-dialog');
let testTimer, testSequence = 0, testEnabled = false, testRunning = false, testContext = '';

let catalog, testSubmitting = false, testRunID = '';
const testCase = () => catalog?.cases.find(c=>c.id===$('traffic-test-case').value);

function fillTestChoices(id, choices, previous) {
  const el=$(id);
  el.replaceChildren(...choices.map(c=>new Option(c.name,c.id)));
  if(choices.some(c=>c.id===previous)) el.value=previous;
}

function selectTestNodes(src,dst) {
  if(!testCase())return;
  const choices=catalog.nodes.map(n=>({id:n.name,name:n.name}));
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
    !src || !dst ? 'No eligible node pair for this test case. Check node readiness and device capacity.' : '');
  $('traffic-test-message').textContent = reason;
  $('traffic-test-submit').disabled = Boolean(reason) || testSubmitting || !testCase();
}

function testResult(run, root=$('traffic-test-result')) {
  const expanded=root.dataset.run===run?.id ? Array.from(root.querySelectorAll('details[open][data-flow]'),d=>d.dataset.flow) : [];
  root.replaceChildren();
  if (!run) return;
  root.dataset.run=run.id;
  const profile=catalog?.profiles.find(p=>p.id===(run.test||'iperf-tcp'));
  const scenario=catalog?.cases.find(c=>c.id===(run.case||'POD_TO_POD_DIFF_NODE'));
  root.append(text('h3',runOutcome(run)[0]+' run'), text('strong',[run.testName||profile?.name||run.test,run.caseName||scenario?.name||run.case].filter(Boolean).join(' · ')),
    text('p',run.source.node+' → '+run.destination.node+' · '+new Date(run.started).toLocaleString()));
  const traffic = run.traffic;
  if (traffic?.delivered) {
    const stats=[];
    if(traffic.bytes!=null)stats.push(count(traffic.bytes)+' bytes received');
    if(traffic.txGbps!=null)stats.push('TX '+traffic.txGbps.toFixed(3)+' Gbps');
    if(traffic.rxGbps!=null)stats.push('RX '+traffic.rxGbps.toFixed(3)+' Gbps');
    else if(traffic.bps!=null)stats.push((traffic.bps/1e9).toFixed(3)+' Gbps');
    root.append(badge(run.test==='iperf-tcp'?'TCP delivery verified':'Traffic delivered','green'),text('p',stats.join(' · ')));
    if((scenario?.service || run.case?.includes('CLUSTER_IP')) && traffic.targetIP)root.append(text('p','Service IP '+traffic.targetIP+' → server '+traffic.destinationIP));
  } else if (traffic) root.append(text('strong','Test failed'),text('p',traffic.error || 'No completed transfer.'));
  else root.append(text('p',run.phase==='queued'?'Queued. Your test will start automatically.':run.phase==='starting'?'Starting the TFT runner.':'Test in progress. Waiting for the runner and collector samples.'));
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
    const status=testRunID ? {enabled:true,run:await api('traffic-test/runs/'+encodeURIComponent(testRunID))} : await api('traffic-test');
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
  clearTimeout(testTimer);testEnabled=false;testRunning=false;testSubmitting=false;catalog=null;testRunID='';
  for(const id of ['case','source','destination']) $('traffic-test-'+id).replaceChildren();
  $('traffic-test-description').textContent='';
  testContext=network && network!=='default' ? 'This test supports the default network. The selected link uses '+network+'.' : '';
  $('traffic-test-result').replaceChildren();$('traffic-test-submit').disabled=true;
  $('traffic-test-message').textContent='Loading tests and nodes…';testDialog.showModal();
  const seq=++testSequence;
  try {
    const options=await api('traffic-test/options');
    if(seq!==testSequence)return;
    catalog=options;
    fillTestChoices('traffic-test-case',catalog.cases,src && src===dst?'POD_TO_POD_SAME_NODE':'POD_TO_POD_DIFF_NODE');
    $('traffic-test-description').textContent='Measure TCP bitrate for 120 seconds and validate hardware offload.';
    selectTestNodes(src,dst);
    await refreshTest(seq);
  }catch(e){if(seq===testSequence)$('traffic-test-message').textContent=e.message}
}
$('offload-run-test').onclick=()=>openTrafficTest($('offload-node').value,'',$('offload-network').value);
$('traffic-test-case').onchange=()=>selectTestNodes($('traffic-test-source').value,$('traffic-test-destination').value);
$('traffic-test-source').onchange=()=>updateTestDestination($('traffic-test-destination').value);
$('traffic-test-destination').onchange=updateTestForm;
testDialog.addEventListener('close',()=>{clearTimeout(testTimer);testSequence++});
$('traffic-test-form').onsubmit=async e=>{
  e.preventDefault();if ($('traffic-test-submit').disabled)return;
  $('traffic-test-submit').disabled=true;
  testSubmitting=true;
  const seq=++testSequence;
  clearTimeout(testTimer);
  try {
    const request={test:'iperf-tcp',case:testCase().id,sourceNode:$('traffic-test-source').value,destinationNode:$('traffic-test-destination').value,network:'default'};
    const status=await api('traffic-test',request);
    loadTestHistory();
    if(seq!==testSequence)return;
    testRunID=status.run.id;testSubmitting=false;testRunning=true;updateTestForm();testResult(status.run);refreshTest(seq);
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
  if(!run.finished)return [{queued:'Queued',starting:'Starting'}[run.phase]||'Running','amber'];
  if(run.phase==='failed')return ['Failed','red'];
  return ['Completed','green'];
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
    for(const run of (compact?runs.slice(0,3):runs)){
      const row=text('article','','record record-top'), label=text('div',''), [name,color]=runOutcome(run);
      const title='TCP throughput';
      label.append(text('strong',title+' · '+(run.testName||run.test)),text('p',new Date(run.started).toLocaleString()+' · '+run.source.node+' → '+run.destination.node,'note'));
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
