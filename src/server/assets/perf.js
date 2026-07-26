// ── performance / storage scan ─────────────────────────────────────────────
// Admin-only console for the storage health scan: start one over chosen
// buckets, watch it live over its own WebSocket, read the reports it leaves
// behind, and drive repairs from them.
//
// The scan is also a registry task, so it appears in the top-bar task panel and
// can be cancelled from there too; this page is the detailed view — per-phase
// counters while it runs, and the findings afterwards.
//
// Findings are paginated server-side (they live in their own column family and
// can number in the millions), so nothing here ever holds a whole report.

let scanWs=null, scanLive=null, scanReportId=null, scanBuckets=[], scanKindOpen={};

// Label + one-line explanation per finding kind. The *actions* a kind allows
// are never hard-coded here — the server sends them with each finding, because
// the storage layer is the single source of truth for what is repairable how.
const findingMeta={
  orphan_blob:['Orphan blob directories','On disk but no index row references them — an uncommitted publish, or index loss. The client was never told these writes succeeded.'],
  superseded_blob:['Superseded copies','A stale copy left by a crashed overwrite or migration. The index row is authoritative and points elsewhere.'],
  missing_blob:['Missing blobs','The index lists these objects but their bytes are gone — they list, but cannot be read.'],
  unreadable_blob:['Unreadable blob directories','No usable meta.json, so there is no object key to be had — only the path. Nothing can be looked up or resynced for these.'],
  corrupt_object:['Corrupt objects','Parts are missing, shorter than recorded, or do not add up to the declared size.'],
  index_drift:['Index drift','The row and meta.json disagree on size, etag, or last-modified. Listings are describing bytes a download would not return.'],
  empty_fanout:['Empty fanout directories','Empty directories under objects/ — wasted inodes, no data at risk.'],
};
const actionLabels={
  trash_blob:'Move blob dir to trash',
  delete_row:'Delete index row',
  quarantine:'Quarantine (trash bytes + delete row)',
  resync_row:'Resync row from meta.json',
  reclaim_empty_dirs:'Reclaim empty directories',
};
// A repaired finding stays in its report rather than vanishing — the report is
// a record of what was found *and* what was done about it. These are how each
// outcome reads once that has happened.
const stateLabels={repaired:'Repaired',stale:'No longer applicable',failed:'Repair failed'};
const kindOrder=['corrupt_object','missing_blob','unreadable_blob','orphan_blob','superseded_blob','index_drift','empty_fanout'];
// Not everything the scan reports is damage. Empty directories are pure
// housekeeping — nothing is at risk and nothing is unreadable — so they are
// styled as routine rather than alarming, and kept out of the problem count.
const housekeepingKinds=new Set(['empty_fanout']);
const isProblem=kind=>!housekeepingKinds.has(kind);
const countProblems=counts=>Object.entries(counts||{}).filter(([k])=>isProblem(k)).reduce((sum,[,n])=>sum+n,0);

// How many findings one "repair all" click may sweep up. Repairs are real
// filesystem work; an unbounded click on a million findings is not a thing an
// operator should be able to do by accident.
const REPAIR_ALL_CAP=5000;

function fmtAgo(ms){
  if(!ms)return '';
  const secs=Math.max(0,Math.floor((Date.now()-ms)/1000));
  if(secs<60)return 'just now';
  const mins=Math.floor(secs/60);if(mins<60)return `${mins} minute${mins===1?'':'s'} ago`;
  const hours=Math.floor(mins/60);if(hours<24)return `${hours} hour${hours===1?'':'s'} ago`;
  const days=Math.floor(hours/24);if(days<30)return `${days} day${days===1?'':'s'} ago`;
  const months=Math.floor(days/30);if(months<12)return `${months} month${months===1?'':'s'} ago`;
  return `${Math.floor(months/12)} year${Math.floor(months/12)===1?'':'s'} ago`;
}

function initPerf(){loadScanHistory();connectScanWs();}

// ── live progress ──
function connectScanWs(){
  if(!me?.is_admin||scanWs)return;
  let ws;
  try{ws=new WebSocket((location.protocol==='https:'?'wss:':'ws:')+'//'+location.host+'/api/perf/scan/ws');}catch{return;}
  scanWs=ws;
  ws.onmessage=e=>{let env;try{env=JSON.parse(e.data);}catch{return;}handleScanEvent(env);};
  ws.onerror=()=>{try{ws.close();}catch{}};
  // Reconnect while the page is open — a scan can outlive a proxy's idle timeout.
  ws.onclose=()=>{scanWs=null;if(me?.is_admin)setTimeout(connectScanWs,2000);};
}
function handleScanEvent(env){
  if(env.type==='finished'){
    scanLive=null;renderScanLive();
    toast('Scan complete',`${env.findings_total} finding${env.findings_total===1?'':'s'}`,env.status==='completed');
    loadScanHistory();if(env.report_id)openScanReport(env.report_id);
    return;
  }
  if(env.type==='repair_finished'){
    scanLive=null;renderScanLive();
    toast('Repairs finished',`${env.repaired} repaired · ${env.stale} no longer applicable · ${env.failed} failed`,env.failed===0);
    if(scanReportId&&$('reportDlg').open)openScanReport(scanReportId);
    return;
  }
  scanLive=env.type==='idle'?null:env;
  renderScanLive();
}
function renderScanLive(){
  const panel=$('scanLivePanel');if(!panel)return;
  if(!scanLive){panel.classList.add('hidden');$('scanRunBtn').disabled=false;return;}
  panel.classList.remove('hidden');$('scanRunBtn').disabled=true;
  const repair=scanLive.type==='repair';
  $('scanLiveTitle').textContent=repair?'Repairing findings':'Storage scan running';
  const p=scanLive.progress||{};
  if(repair){
    const done=scanLive.repaired||0,total=scanLive.total||0;
    $('scanLiveSub').textContent=`${done} of ${total} finding${total===1?'':'s'} processed`;
    setScanBar(total?done/total*100:0,false);
    $('scanStats').innerHTML=statTile('Processed',`${done} / ${total}`);
  }else{
    const bucketsDone=p.buckets_done||0,bucketsTotal=p.buckets_total||0;
    $('scanLiveSub').textContent=`${p.bucket?`Bucket “${p.bucket}” · `:''}${bucketsDone} of ${bucketsTotal} bucket${bucketsTotal===1?'':'s'} done · started ${fmtAgo(scanLive.started_at_ms)}`;
    const known=p.phase==='disk'&&p.objects_total>0;
    setScanBar(known?Math.min(100,(p.objects_visited||0)/p.objects_total*100):0,!known);
    $('scanStats').innerHTML=[
      statTile('Phase',scanPhaseLabel(p.phase)),
      statTile('Objects visited',known?`${(p.objects_visited||0).toLocaleString()} / ${p.objects_total.toLocaleString()}`:(p.objects_visited||0).toLocaleString()),
      statTile('Directories',(p.dirs_visited||0).toLocaleString()),
      statTile('Parts checked',(p.parts_checked||0).toLocaleString()),
      statTile('Bytes seen',fmtSize(p.bytes_seen||0)),
      statTile('Findings',(p.findings||0).toLocaleString(),(p.findings||0)>0),
    ].join('');
  }
}
function scanPhaseLabel(phase){
  return {starting:'Starting',index:'Reading index',disk:'Scanning disk usage',verify:'Verifying candidates',reconcile:'Reconciling index against disk',usage:'Measuring trash & staging'}[phase]||phase||'—';
}
function setScanBar(pct,indeterminate){
  const bar=$('scanBar');bar.classList.toggle('indet',!!indeterminate);
  $('scanBarFill').style.width=indeterminate?'32%':pct.toFixed(1)+'%';
}
function statTile(label,value,warn){return `<div class="scan-stat${warn?' warn':''}"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`;}

async function cancelScan(){
  if(!scanLive?.task_id)return;
  const btn=$('scanCancelBtn');setBusy(btn,true,'Cancelling…');
  try{await api('POST','/api/tasks/'+enc(scanLive.task_id)+'/cancel');toast('Cancelling','The scan will stop at its next checkpoint');}
  catch(e){toast('Cannot cancel',e.message,false);}
  finally{setBusy(btn,false);btn.textContent='Cancel';}
}

// ── the wizard ──
async function openScanWizard(){
  setInlineError('scanDlgError');
  const host=$('scanBucketList');
  host.innerHTML='<div class="muted" style="padding:10px">Loading buckets…</div>';
  $('scanDlg').showModal();
  try{
    const data=await api('GET','/api/buckets');
    scanBuckets=(data.buckets||[]).map(b=>b.name);
    host.innerHTML=scanBuckets.length
      ? scanBuckets.map(name=>`<label class="scan-bucket"><input type="checkbox" value="${esc(name)}" checked> <span>${esc(name)}</span></label>`).join('')
      : '<div class="muted" style="padding:10px">There are no buckets to scan.</div>';
  }catch(e){host.innerHTML='';setInlineError('scanDlgError',e.message);}
}
function toggleAllScanBuckets(on){document.querySelectorAll('#scanBucketList input[type=checkbox]').forEach(cb=>{cb.checked=on;});}
async function startScan(){
  const buckets=[...document.querySelectorAll('#scanBucketList input[type=checkbox]:checked')].map(cb=>cb.value);
  if(!buckets.length){setInlineError('scanDlgError','Select at least one bucket.');return;}
  const btn=$('scanStartBtn');setBusy(btn,true,'Starting…');
  try{
    await api('POST','/api/perf/scan',{buckets});
    $('scanDlg').close();
    toast('Scan started',`Scanning ${buckets.length} bucket${buckets.length===1?'':'s'}`);
  }catch(e){setInlineError('scanDlgError',e.message);}
  finally{setBusy(btn,false);btn.textContent='Start scan';}
}

// ── history ──
async function loadScanHistory(){
  const host=$('scanHistory');if(!host)return;
  try{
    const data=await api('GET','/api/perf/scans?limit=25');
    const reports=data.reports||[];
    if(!reports.length){
      host.innerHTML=`<div class="empty" style="padding:44px 20px"><div class="empty-icon">${icons.activity}</div><h3>No scans yet</h3><p>A scan walks every object directory to reconcile the catalog against what is actually on disk. Run one to see disk usage per bucket and anything that needs attention.</p></div>`;
      return;
    }
    host.innerHTML=`<table><thead><tr><th style="width:34px"><input type="checkbox" onclick="event.stopPropagation()" onchange="toggleAllReports(this.checked)"></th><th>When</th><th>Buckets</th><th>Objects</th><th>Logical</th><th>On disk</th><th>Findings</th><th></th></tr></thead><tbody>${reports.map(scanRowHtml).join('')}</tbody></table>`;
  }catch(e){host.innerHTML=`<div class="muted" style="padding:18px">${esc(e.message)}</div>`;}
}
function scanRowHtml(report){
  const status=report.status;
  const badge=status==='completed'?'':`<span class="scan-badge ${esc(status)}">${esc(status)}</span> `;
  const problems=countProblems(report.findings);
  const tidy=(report.findings_total||0)-problems;
  const findings=problems
    ? `<span class="scan-badge warn">${problems.toLocaleString()}</span>`
    : tidy
      ? `<span class="scan-badge tidy">${tidy.toLocaleString()} to tidy</span>`
      : `<span class="muted">clean</span>`;
  // The row stays clickable for anyone who tries it, but the View button is
  // what makes opening a report discoverable — a bare clickable row tells
  // nobody it can be clicked.
  return `<tr class="scan-row" onclick="openScanReport('${esc(report.id)}')">
    <td onclick="event.stopPropagation()"><input type="checkbox" class="scan-report-pick" value="${esc(report.id)}"></td>
    <td><strong class="scan-when">${esc(fmtTime(report.started_at_ms))}</strong><div class="muted" style="font-size:11.5px">${badge}${esc(fmtAgo(report.started_at_ms))}${report.actor?' · by '+esc(report.actor):''}</div></td>
    <td>${report.buckets_scanned}</td>
    <td>${(report.objects||0).toLocaleString()}</td>
    <td>${esc(fmtSize(report.logical_bytes||0))}</td>
    <td>${esc(fmtSize(report.disk_bytes||0))}</td>
    <td>${findings}</td>
    <td style="text-align:right;white-space:nowrap">
      <button class="btn small" onclick="event.stopPropagation();openScanReport('${esc(report.id)}')">View</button>
      <button class="row-action danger" title="Delete this report" onclick="event.stopPropagation();confirmDeleteScan('${esc(report.id)}')">${icons.trash}</button>
    </td>
  </tr>`;
}
function toggleAllReports(on){document.querySelectorAll('.scan-report-pick').forEach(cb=>{cb.checked=on;});}
function deleteSelectedReports(){
  const ids=[...document.querySelectorAll('.scan-report-pick:checked')].map(cb=>cb.value);
  if(!ids.length){toast('Nothing selected','Tick the reports you want to delete',false);return;}
  showConfirm(`Delete ${ids.length} report${ids.length===1?'':'s'}`,'Scan history',
    'The scans themselves changed nothing, so deleting their reports is safe — any repair already applied stays applied.',
    async()=>{
      const r=await api('DELETE','/api/perf/scans',{ids});
      if(ids.includes(scanReportId)){scanReportId=null;$('reportDlg').close();}
      toast('Reports deleted',`${r.deleted} removed`);loadScanHistory();
    });
}
function deleteAllReports(){
  showConfirm('Delete the entire scan history','All reports',
    'Every report and all of its findings are removed. A scan that is still running keeps its report. Repairs already applied stay applied.',
    async()=>{
      const r=await api('DELETE','/api/perf/scans',{all:true});
      scanReportId=null;$('reportDlg').close();
      toast('History cleared',`${r.deleted} report${r.deleted===1?'':'s'} removed`);loadScanHistory();
    });
}
function confirmDeleteScan(id){
  showConfirm('Delete scan report','This removes the report and all of its findings.','The scan itself changed nothing, so deleting the report is safe — you can always run another.',async()=>{
    await api('DELETE','/api/perf/scans/'+enc(id));
    if(scanReportId===id){scanReportId=null;$('reportDlg').close();}
    toast('Report deleted','');loadScanHistory();
  });
}

// ── one report ──
async function openScanReport(id){
  scanReportId=id;
  // Which categories have already been fetched is per-report state. Not
  // clearing it meant a kind opened in an earlier report short-circuited its
  // load here, leaving the section expanded but empty — no rows, no buttons.
  scanKindOpen={};
  const host=$('scanReportBody');
  host.innerHTML='<div class="panel"><div class="muted" style="padding:20px">Loading report…</div></div>';
  $('reportDlgTitle').textContent='Scan report';
  $('reportDlgSub').textContent='';
  if(!$('reportDlg').open)$('reportDlg').showModal();
  try{
    const report=await api('GET','/api/perf/scans/'+enc(id));
    $('reportDlgTitle').textContent=`Scan of ${fmtTime(report.started_at_ms)}`;
    $('reportDlgSub').textContent=`${fmtAgo(report.started_at_ms)} · ${report.buckets_scanned} bucket${report.buckets_scanned===1?'':'s'}`
      +`${report.finished_at_ms?' · took '+fmtDur(report.finished_at_ms-report.started_at_ms):''}`
      +`${report.actor?' · by '+report.actor:''}`;
    host.innerHTML=reportHtml(report);
    hydrateIcons(host);
    // Auto-open the first non-empty category: the point of the page is the
    // problems, so make the operator click once fewer to see one.
    const first=kindOrder.find(kind=>(report.findings||{})[kind]);
    if(first)toggleFindings(first);
  }catch(e){host.innerHTML=`<div class="panel"><div class="muted" style="padding:20px">${esc(e.message)}</div></div>`;}
}
function reportHtml(report){
  const states=report.finding_states||{};
  const outstanding=states.open||0;
  const deferred=report.deferred_recent||0;
  const problems=countProblems(report.findings);
  const housekeeping=(report.findings_total||0)-problems;
  // A dir written moments before the scan looked at it may be a publish that
  // has not committed its row yet, so it is not judged. Say so plainly —
  // otherwise "no findings" reads as "nothing to find".
  const deferredNote=deferred?`<div class="scan-note">${deferred} director${deferred===1?'y was':'ies were'} modified moments before the scan reached ${deferred===1?'it':'them'}, so ${deferred===1?'it was':'they were'} not judged — a directory being published right now is indistinguishable from one left behind. Run the scan again in a few seconds to include ${deferred===1?'it':'them'}.</div>`:'';
  const buckets=(report.buckets||[]).map(b=>`<tr>
    <td><strong>${esc(b.bucket)}</strong>${b.error?`<div class="scan-badge failed">${esc(b.error)}</div>`:''}</td>
    <td>${(b.objects_indexed||0).toLocaleString()}</td>
    <td>${esc(fmtSize(b.logical_bytes||0))}</td>
    <td>${esc(fmtSize(b.objects_bytes||0))}</td>
    <td>${esc(fmtSize(b.trash_bytes||0))}</td>
    <td>${esc(fmtSize(b.staging_bytes||0))}</td>
    <td>${esc(fmtSize(b.index_bytes||0))}</td>
    <td>${(b.empty_fanout_dirs||0).toLocaleString()}</td>
    <td>${(b.stale_intents||0).toLocaleString()}</td>
  </tr>`).join('');
  const kinds=kindOrder.filter(kind=>(report.findings||{})[kind]).map(kind=>findingSectionHtml(kind,report.findings[kind])).join('');
  return `<div class="panel">
    <div class="panel-title">
      <div><h3>Summary</h3><p>${report.error?esc(report.error):'What the scan measured across every bucket it covered.'}</p></div>
    </div>
    <div class="scan-summary">
      ${statTile('Objects',(report.objects||0).toLocaleString())}
      ${statTile('Logical size',fmtSize(report.logical_bytes||0))}
      ${statTile('On disk',fmtSize(report.disk_bytes||0))}
      ${statTile('Problems',problems.toLocaleString(),problems>0)}
      ${housekeeping?statTile('To tidy up',housekeeping.toLocaleString()):''}
      ${statTile('Still open',outstanding.toLocaleString(),problems>0&&outstanding>0)}
      ${statTile('Repaired',(states.repaired||0).toLocaleString())}
      ${deferred?statTile('Not yet judged',deferred.toLocaleString(),true):''}
    </div>
    ${deferredNote}
    <div style="overflow-x:auto"><table>
      <thead><tr><th>Bucket</th><th>Objects</th><th>Logical</th><th>Objects dir</th><th>Trash</th><th>Staging</th><th>Index</th><th>Empty dirs</th><th>Stale intents</th></tr></thead>
      <tbody>${buckets}</tbody>
    </table></div>
  </div>
  ${problems?'':`<div class="panel" style="margin-top:16px"><div class="empty" style="padding:40px 20px"><div class="empty-icon">${icons.check}</div><h3>Nothing wrong found</h3><p>Every index row matched a blob directory on disk, every part was present and the right size, and no directory was left unreferenced.${housekeeping?` There ${housekeeping===1?'is':'are'} ${housekeeping} routine tidy-up item${housekeeping===1?'':'s'} below — no data is at risk.`:''}${deferred?` ${deferred} recently-written director${deferred===1?'y was':'ies were'} skipped as too new to judge — rescan shortly to cover ${deferred===1?'it':'them'}.`:''}</p></div></div>`}
  ${kinds}`;
}
function findingSectionHtml(kind,count){
  const [title,blurb]=findingMeta[kind]||[kind,''];
  const badge=isProblem(kind)?'warn':'tidy';
  return `<div class="panel scan-kind" style="margin-top:16px" id="kind_${esc(kind)}">
    <div class="panel-title" onclick="toggleFindings('${esc(kind)}')" style="cursor:pointer">
      <div><h3>${esc(title)} <span class="scan-badge ${badge}">${count.toLocaleString()}</span></h3><p>${esc(blurb)}</p></div>
      <span class="spacer"></span>
      <span class="scan-chevron" data-icon="chevron-down"></span>
    </div>
    <div class="scan-findings hidden" id="findings_${esc(kind)}"></div>
  </div>`;
}
async function toggleFindings(kind){
  const host=$('findings_'+kind);if(!host)return;
  const opening=host.classList.contains('hidden');
  host.classList.toggle('hidden',!opening);
  $('kind_'+kind).classList.toggle('open',opening);
  if(!opening||scanKindOpen[kind])return;
  scanKindOpen[kind]=true;
  host.innerHTML='<div class="muted" style="padding:16px 19px">Loading findings…</div>';
  await loadFindings(kind,null,[]);
}
async function loadFindings(kind,after,accumulated){
  const host=$('findings_'+kind);
  const query=new URLSearchParams({kind,limit:'100'});if(after)query.set('after',after);
  try{
    const data=await api('GET',`/api/perf/scans/${enc(scanReportId)}/findings?`+query);
    const findings=accumulated.concat(data.findings||[]);
    host.dataset.next=data.next||'';
    host.innerHTML=findingsHtml(kind,findings,data.next);
    hydrateIcons(host);
  }catch(e){host.innerHTML=`<div class="muted" style="padding:16px 19px">${esc(e.message)}</div>`;}
}
function findingsHtml(kind,findings,next){
  const rows=findings.map(f=>findingRowHtml(kind,f)).join('');
  const actions=(findings[0]?.actions)||[];
  const openCount=findings.filter(f=>f.state==='open').length;
  // Whole-section repair: the operator picks the action once and it is applied
  // to every open finding of this kind — not just the ones on screen, since
  // repairAll pages through the server. Offered for every kind, because "there
  // are 4,000 of these" is exactly when clicking each one is not an option.
  const bulk=actions.length&&openCount
    ? `<select class="input small scan-action" id="bulk_${esc(kind)}" title="Action to apply to every open finding in this category">
         ${actions.map((a,i)=>`<option value="${esc(a)}">${esc(actionLabels[a]||a)}${i===0?' (recommended)':''}</option>`).join('')}
       </select>
       <button class="btn small primary" onclick="repairAll('${esc(kind)}')">Apply to all open</button>`
    : '';
  return `<div class="scan-findings-head">
      <label class="scan-check"><input type="checkbox" onchange="toggleAllFindings('${esc(kind)}',this.checked)"> Select all on this page</label>
      <button class="btn small" onclick="repairSelected('${esc(kind)}')">Repair selected</button>
      <span class="spacer"></span>
      ${bulk}
    </div>
    <div class="scan-finding-list">${rows}</div>
    ${next?`<div class="scan-more"><button class="btn small" onclick="loadMoreFindings('${esc(kind)}')">Load more</button></div>`:''}`;
}
function findingRowHtml(kind,f){
  const state=f.state||'open';
  const target=f.object_key?`<code>${esc(f.object_key)}</code>`:(f.blob_dir?`<code>${esc(f.blob_dir)}</code>`:'<span class="muted">—</span>');
  const where=f.object_key&&f.blob_dir?`<div class="muted" style="font-size:11px">${esc(f.blob_dir)}</div>`:'';
  // Aggregated findings (empty directories) name no single path, so list the
  // directories they stand for — otherwise there is nothing to go and inspect.
  const paths=Array.isArray(f.data?.paths)?f.data.paths:null;
  const pathList=paths&&paths.length
    ? `<div class="scan-paths">${paths.map(p=>`<code>${esc(p)}</code>`).join('')}${f.data.paths_truncated?`<span class="muted">…and ${(f.count-paths.length).toLocaleString()} more</span>`:''}</div>`
    : '';
  const options=(f.actions||[]).map((a,i)=>`<option value="${esc(a)}">${esc(actionLabels[a]||a)}${i===0?' (recommended)':''}</option>`).join('');
  const controls=state==='open'
    ? `<select class="input small scan-action" data-key="${esc(f.key)}">${options}</select>
       <button class="btn small" onclick="repairOne('${esc(kind)}','${esc(f.key)}',this)">Repair</button>`
    : `<span class="scan-badge ${esc(state)}">${state==='repaired'?icons.check:''}${esc(stateLabels[state]||state)}</span>`;
  return `<div class="scan-finding" data-key="${esc(f.key)}" data-state="${esc(state)}">
    <label class="scan-check">${state==='open'?`<input type="checkbox" class="scan-pick" value="${esc(f.key)}">`:'<span style="width:14px;display:inline-block"></span>'}</label>
    <div style="flex:1;min-width:0">
      <div class="scan-finding-target">${esc(f.bucket)} · ${target}</div>${where}
      <div class="muted" style="font-size:11.5px">${esc(f.detail)}</div>
      ${pathList}
      ${f.outcome?`<div class="scan-outcome">${esc(f.outcome)}${f.repaired_at_ms?` · ${esc(fmtAgo(f.repaired_at_ms))}`:''}</div>`:''}
    </div>
    <div class="scan-finding-size">${f.bytes?esc(fmtSize(f.bytes)):''}</div>
    <div class="scan-finding-actions">${controls}</div>
  </div>`;
}
function toggleAllFindings(kind,on){document.querySelectorAll(`#findings_${CSS.escape(kind)} .scan-pick`).forEach(cb=>{cb.checked=on;});}
async function loadMoreFindings(kind){
  const host=$('findings_'+kind);
  const shown=[...host.querySelectorAll('.scan-finding')].length;
  const next=host.dataset.next;if(!next)return;
  // Re-fetch from the cursor and append; the list is server-ordered so pages
  // concatenate cleanly.
  const query=new URLSearchParams({kind,limit:'100',after:next});
  try{
    const data=await api('GET',`/api/perf/scans/${enc(scanReportId)}/findings?`+query);
    const list=host.querySelector('.scan-finding-list');
    list.insertAdjacentHTML('beforeend',(data.findings||[]).map(f=>findingRowHtml(kind,f)).join(''));
    host.dataset.next=data.next||'';
    if(!data.next)host.querySelector('.scan-more')?.remove();
    hydrateIcons(host);
  }catch(e){toast('Could not load more',e.message,false);}
  void shown;
}

// ── repairs ──
function repairOne(kind,key,btn){
  const select=btn.parentElement.querySelector('.scan-action');
  const action=select?select.value:null;if(!action)return;
  confirmRepair(kind,[{key,action}],actionLabels[action]||action,1);
}
function repairSelected(kind){
  const host=$('findings_'+kind);
  const items=[...host.querySelectorAll('.scan-pick:checked')].map(cb=>{
    const row=cb.closest('.scan-finding');
    const select=row.querySelector('.scan-action');
    return {key:cb.value,action:select?select.value:null};
  }).filter(item=>item.action);
  if(!items.length){toast('Nothing selected','Tick the findings you want to repair',false);return;}
  confirmRepair(kind,items,'the action chosen on each row',items.length);
}
// Sweeps every *open* finding of one kind — paging through the server rather
// than trusting whatever happens to be rendered.
async function repairAll(kind){
  const select=$('bulk_'+kind);
  const action=select?select.value:null;
  if(!action)return;
  const items=[];let after=null;
  try{
    while(items.length<REPAIR_ALL_CAP){
      const query=new URLSearchParams({kind,limit:'500'});if(after)query.set('after',after);
      const data=await api('GET',`/api/perf/scans/${enc(scanReportId)}/findings?`+query);
      for(const f of data.findings||[])if(f.state==='open')items.push({key:f.key,action});
      if(!data.next)break;
      after=data.next;
    }
  }catch(e){toast('Could not collect findings',e.message,false);return;}
  if(!items.length){toast('Nothing to repair','Every finding in this category is already resolved',false);return;}
  const capped=items.length>=REPAIR_ALL_CAP;
  confirmRepair(kind,items,actionLabels[action]||action,items.length,capped);
}
function confirmRepair(kind,items,what,count,capped){
  const [title]=findingMeta[kind]||[kind];
  showConfirm(
    `Repair ${count} finding${count===1?'':'s'}`,
    title,
    `${what} will be applied to ${count} finding${count===1?'':'s'}.`
      +` Each one is re-checked under its object lock first, so anything that no longer applies is skipped rather than acted on.`
      +(capped?` Only the first ${REPAIR_ALL_CAP} are included — run this again afterwards for the rest.`:''),
    async()=>{
      await api('POST',`/api/perf/scans/${enc(scanReportId)}/repair`,{items});
      toast('Repairs started',`${count} finding${count===1?'':'s'} queued`);
    },
    {confirmLabel:'Repair',busyLabel:'Starting…',danger:true}
  );
}
