// ── backup ─────────────────────────────────────────────────────────────────
// Export / import of the global IAM database. Admin-only (the tab and its
// endpoints are gated). Export downloads a binary dump; import uploads one and
// renders a per-family summary so the result is easy to read at a glance.

function fmtBytes(n){
  if(n<1024) return n+' B';
  const u=['KB','MB','GB']; let i=-1;
  do{ n/=1024; i++; }while(n>=1024&&i<u.length-1);
  return n.toFixed(n<10?1:0)+' '+u[i];
}

function resetBackupPanel(){
  stagedImport=null;
  const f=$('importFile'); if(f){ f.value=''; importFileChosen(); }
  const r=$('importResult'); if(r){ r.classList.add('hidden'); r.innerHTML=''; }
  document.querySelectorAll('input[name="importMode"]').forEach((el,i)=>el.checked=(i===0));
}

// ── file picker ────────────────────────────────────────────────────────────
function importFileChosen(){
  const file=$('importFile').files[0];
  cancelImportPreview();
  $('importPickEmpty').classList.toggle('hidden',!!file);
  $('importPickInfo').classList.toggle('hidden',!file);
  if(file){ $('importPickName').textContent=file.name; $('importPickSize').textContent=fmtBytes(file.size); }
}
function clearImportFile(){ $('importFile').value=''; importFileChosen(); }
function importDropFile(e){
  e.preventDefault(); e.currentTarget.classList.remove('drag');
  const file=e.dataTransfer.files&&e.dataTransfer.files[0]; if(!file) return;
  const dt=new DataTransfer(); dt.items.add(file);
  $('importFile').files=dt.files; importFileChosen();
}

async function exportIam(){
  const btn=$('exportBtn'); setBusy(btn,true,'Exporting…');
  try{
    const resp=await fetch('/api/admin/export',{cache:'no-store'});
    if(!resp.ok){ let msg=resp.statusText; try{ msg=(await resp.json()).error||msg; }catch{} throw new Error(msg); }
    const blob=await resp.blob();
    const stamp=new Date().toISOString().replace(/[:.]/g,'-').slice(0,19);
    const name=`rusts3-iam-${stamp}.rs3pb`;
    const url=URL.createObjectURL(blob);
    const a=document.createElement('a'); a.href=url; a.download=name; document.body.append(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
    toast('Export ready',`Downloaded ${name} · ${fmtBytes(blob.size)}`,true);
  }catch(e){ toast('Export failed',e.message,false); }
  finally{ setBusy(btn,false); }
}

// ── staged import ───────────────────────────────────────────────────────────
// Stage 1 ("Review & Import"): the dump is uploaded to a read-only preview
// endpoint that validates the framing and reports per-family counts plus up to
// 10 sample names derived from row keys — secrets live in row values, which
// are never decoded, so none can appear here. Stage 2 ("Confirm import")
// uploads the same bytes to the real import endpoint.
let stagedImport=null;

async function importIam(){
  const input=$('importFile');
  const file=input&&input.files&&input.files[0];
  if(!file){ toast('Choose a file','Select a dump file to review.',false); return; }
  const btn=$('importBtn'); setBusy(btn,true,'Reading dump…');
  const result=$('importResult'); result.classList.add('hidden'); result.innerHTML='';
  try{
    const buf=await file.arrayBuffer();
    const resp=await fetch('/api/admin/import/preview',{
      method:'POST', headers:{'content-type':'application/octet-stream'}, body:buf,
    });
    const data=await resp.json().catch(()=>({}));
    if(!resp.ok) throw new Error(data.error||resp.statusText||'Could not read dump');
    stagedImport=buf;
    renderImportPreview(data);
  }catch(e){
    stagedImport=null;
    result.classList.remove('hidden');
    result.innerHTML=`<div style="padding:12px 14px;border-radius:8px;background:rgba(192,57,43,.1);color:#c0392b;font-weight:600">Could not read dump: ${esc(e.message)}</div>`;
    toast('Could not read dump',e.message,false);
  }finally{ setBusy(btn,false); }
}

const FAMILY_LABELS={
  users:['Users','User'],
  groups:['Groups','Group'],
  access_keys:['Access keys','Access key'],
  web_keys:['Web signing keys','Web signing key of'],
  user_groups:['Group memberships','Membership'],
};

function renderImportPreview(d){
  const r=$('importResult');
  const families=d.families||[];
  const sections=families.map(f=>{
    const [plural,singular]=FAMILY_LABELS[f.family]||[f.family,f.family];
    const samples=f.samples||[];
    const lines=samples.map(name=>`<div class="preview-line">${esc(singular)} <strong>${esc(name)}</strong></div>`).join('')
      +(f.rows>samples.length?'<div class="preview-line muted">…</div>':'');
    return `<details class="preview-sec"${families.length<=2?' open':''}><summary>Loading ${f.rows} ${esc(plural)}</summary><div class="preview-body">${lines||'<div class="preview-line muted">No rows</div>'}</div></details>`;
  }).join('');
  r.classList.remove('hidden');
  r.innerHTML=`
    <div class="preview-card">
      <div class="preview-head"><strong>Review before importing</strong><span class="muted" style="font-size:12.5px">${d.total} row${d.total===1?'':'s'} in the dump. Nothing has been written yet. Secrets are never shown.</span></div>
      ${sections||'<div class="preview-line muted" style="padding:12px 14px">The dump contains no rows.</div>'}
      <div class="preview-actions">
        <span class="muted" id="previewModeNote"></span>
        <button class="btn" onclick="cancelImportPreview()">Cancel</button>
        <button class="btn danger" id="confirmImportBtn" onclick="confirmImport()"><span data-icon="upload"></span> Confirm import</button>
      </div>
    </div>`;
  hydrateIcons(r);
  updatePreviewModeNote();
}

function updatePreviewModeNote(){
  const el=$('previewModeNote'); if(!el) return;
  const mode=(document.querySelector('input[name="importMode"]:checked')||{}).value||'merge';
  el.innerHTML=mode==='replace'
    ?'<span style="color:#c0392b;font-weight:600">Replace mode: the entire IAM database is erased first.</span>'
    :'Merge mode: rows are upserted; existing entries are kept.';
}

function cancelImportPreview(){
  stagedImport=null;
  const r=$('importResult'); if(r){ r.classList.add('hidden'); r.innerHTML=''; }
}

async function confirmImport(){
  if(!stagedImport){ toast('Nothing staged','Choose a dump file and review it first.',false); return; }
  const mode=(document.querySelector('input[name="importMode"]:checked')||{}).value||'merge';
  const btn=$('confirmImportBtn'); setBusy(btn,true,'Importing…');
  try{
    const resp=await fetch('/api/admin/import?mode='+encodeURIComponent(mode),{
      method:'POST', headers:{'content-type':'application/octet-stream'}, body:stagedImport,
    });
    const data=await resp.json().catch(()=>({}));
    if(!resp.ok) throw new Error(data.error||resp.statusText||'Import failed');
    stagedImport=null;
    clearImportFile();
    renderImportResult(data);
    toast('Import complete',`${data.imported} row(s) imported · ${data.mode}`,true);
    // Reflect the restored state in the IAM views.
    if(typeof loadUsers==='function') loadUsers();
    if(typeof loadGroups==='function') loadGroups();
  }catch(e){
    setBusy(btn,false);
    toast('Import failed',e.message,false);
  }
}

function renderImportResult(d){
  const r=$('importResult');
  const label={users:'Users',groups:'Groups',access_keys:'Access keys',web_keys:'Web signing keys',user_groups:'Group memberships'};
  const rows=(d.families||[]).map(f=>`<tr><td>${esc(label[f.family]||f.family)}</td><td style="text-align:right;font-variant-numeric:tabular-nums">${f.rows}</td></tr>`).join('');
  const erased=d.mode==='replace'?` · erased ${d.erased} existing row${d.erased===1?'':'s'} first`:'';
  r.classList.remove('hidden');
  r.innerHTML=`
    <div style="border:1px solid var(--line);border-radius:10px;overflow:hidden">
      <div style="padding:12px 14px;background:var(--surface-2,rgba(0,0,0,.02));border-bottom:1px solid var(--line)">
        <div style="font-weight:700">Import complete</div>
        <div class="muted" style="font-size:12.5px;margin-top:3px">Mode <strong>${esc(d.mode)}</strong>${erased} · imported <strong>${d.imported}</strong> row${d.imported===1?'':'s'} total</div>
      </div>
      <table style="width:100%;border-collapse:collapse;font-size:13px">
        <thead><tr><th style="text-align:left;padding:8px 14px;color:var(--muted);font-weight:600">Family</th><th style="text-align:right;padding:8px 14px;color:var(--muted);font-weight:600">Rows</th></tr></thead>
        <tbody>${rows||'<tr><td colspan="2" class="muted" style="padding:10px 14px">No rows in the dump</td></tr>'}</tbody>
      </table>
    </div>`;
}
