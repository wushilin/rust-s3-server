// ── webhooks & replication ────────────────────────────────────────────────
// Changelog subscriptions (admin only): list with live status, create/edit,
// the one-time secret/token reveal, and the recovery actions for a paused
// subscription (resume, resume from oldest, re-snapshot, skip, set offset).
let subs=[],subEditing=null,subActing=null;
const SUB_MASK='********';
const subStatusBadge=s=>s.status==='active'?'<span class="badge green">Active</span>':s.status==='snapshot'?'<span class="badge green">Snapshot</span>':s.status==='muted'?`<span class="badge amber" title="Until ${esc(fmtTime(s.muted_until_ms))}">Muted ${fmtDur(s.muted_until_ms-Date.now())}</span>`:`<span class="badge" style="color:var(--danger,#c33)" title="${esc(s.paused?.message||'')}">Paused · ${esc((s.paused?.reason||'').replace('_',' '))}</span>`;
function subFilterText(f){const parts=[];if(f.prefix)parts.push(`prefix <code>${esc(f.prefix)}</code>`);if(f.suffix)parts.push(`suffix <code>${esc(f.suffix)}</code>`);if(f.ops&&f.ops.length===1)parts.push(f.ops[0]+'s only');return parts.join(' · ')||'<span class="muted">all changes</span>';}
function subPosition(s){
  if(s.kind==='pull')return s.last_success_ms?`served through seq ${s.offset.toLocaleString()}<div class="muted" style="font-size:11px">readers keep their own position</div>`:'<span class="muted">not read yet</span>';
  if(s.phase==='snapshot')return `Snapshot${s.snapshot_cursor?` at <code>${esc(s.snapshot_cursor)}</code>`:''}`;
  const lag=s.lag??0;let text=`seq ${s.offset.toLocaleString()} / ${(s.head??0).toLocaleString()}`;
  if(lag)text+=`<div class="muted" style="font-size:11px">${lag.toLocaleString()} behind</div>`;
  if(s.expires_at_ms&&s.status!=='paused'){const left=s.expires_at_ms-Date.now();const warn=left<2*86400000;text+=`<div style="font-size:11px;${warn?'color:var(--danger,#c33);font-weight:600':''}" class="${warn?'':'muted'}">oldest pending expires in ${fmtDur(left)}</div>`;}
  return text;
}
function subLast(s){
  if(!s.last_attempt_ms&&!s.last_success_ms)return '<span class="muted">Never</span>';
  const ok=s.last_success_ms&&s.last_success_ms>=s.last_attempt_ms;
  return `${fmtTime(s.last_attempt_ms||s.last_success_ms)}<div class="muted" style="font-size:11px">${ok?'ok':esc(s.last_error||('HTTP '+s.last_status))}${s.consecutive_failures?` · ${s.consecutive_failures} failed`:''}</div>`;
}
async function loadSubs(){
  try{
    const data=await api('GET','/api/subscriptions');subs=data.subscriptions||[];
    $('subCount').textContent=`${subs.length} subscription${subs.length===1?'':'s'}`;
    $('subRows').innerHTML=subs.map((s,i)=>`<tr><td><div class="identity"><span class="identity-icon">${icons[s.kind==='webhook'?'send':'key']}</span><div><strong>${esc(s.name)}</strong><small>${s.kind==='webhook'?esc(s.url):'Replication token (pull)'}</small></div></div></td><td><code>${esc(s.bucket)}</code><div style="font-size:11.5px">${subFilterText(s.filter)}</div></td><td>${subStatusBadge(s)}</td><td>${subPosition(s)}</td><td>${subLast(s)}</td><td class="actions"><button class="row-action" title="Status and actions" onclick="openSubActions(${i})">${icons.activity}</button><button class="row-action" title="Edit" onclick="openSubDialog(${i})">${icons.edit}</button><button class="row-action danger" title="Delete" onclick="confirmDeleteSub(${i})">${icons.trash}</button></td></tr>`).join('');
    const empty=!subs.length;$('subRows').closest('table').classList.toggle('hidden',empty);$('subEmpty').classList.toggle('hidden',!empty);
    if(empty)$('subEmpty').innerHTML=`<div class="empty-icon">${icons.send}</div><h3>No subscriptions</h3><p>Create a webhook to receive bucket changes, or a replication token for a replicator to pull them.</p>`;
  }catch(e){toast('Could not load subscriptions',e.message,false);}
}
function subKindChanged(){const webhook=$('sub_kind').value==='webhook';document.querySelectorAll('#subDlg .sub-webhook').forEach(el=>el.classList.toggle('hidden',!webhook));}
function parseSubHeaders(text){const out={};for(const raw of text.split('\n')){const line=raw.trim();if(!line)continue;const at=line.indexOf(':');if(at<1)throw new Error(`Header line “${line}” needs the form Name: value`);out[line.slice(0,at).trim()]=line.slice(at+1).trim();}return out;}
async function openSubDialog(i){
  subEditing=i===undefined?null:subs[i];const s=subEditing;
  setInlineError('subError');
  try{const data=await api('GET','/api/buckets');$('sub_bucket').innerHTML=(data.buckets||[]).map(b=>`<option>${esc(b.name)}</option>`).join('');}catch{}
  $('sub_title').textContent=s?`Edit ${s.name}`:'New subscription';$('sub_submit').textContent=s?'Save':'Create';
  $('sub_kind').value=s?s.kind:'webhook';$('sub_kind').disabled=!!s;$('sub_bucket').disabled=!!s;
  if(s)$('sub_bucket').innerHTML=`<option>${esc(s.bucket)}</option>`;
  $('sub_name').value=s?.name||'';$('sub_url').value=s?.url||'';
  $('sub_headers').value=s?Object.keys(s.headers||{}).map(k=>`${k}: ${SUB_MASK}`).join('\n'):'';
  $('sub_prefix').value=s?.filter.prefix||'';$('sub_suffix').value=s?.filter.suffix||'';
  $('sub_ops').value=s&&s.filter.ops&&s.filter.ops.length===1?s.filter.ops[0]:'';
  $('sub_batch').value=s?.batch_max_events??'';$('sub_mute').value=s?.mute_secs??'';
  $('sub_snapshot').checked=false;document.querySelectorAll('#subDlg .sub-create').forEach(el=>el.classList.toggle('hidden',!!s));
  subKindChanged();$('subDlg').showModal();setTimeout(()=>$('sub_name').focus(),50);
}
async function saveSub(event){
  event.preventDefault();setInlineError('subError');
  try{
    const kind=$('sub_kind').value,ops=$('sub_ops').value;
    const filter={prefix:$('sub_prefix').value,suffix:$('sub_suffix').value,ops:ops?[ops]:[]};
    const batch=$('sub_batch').value?Number($('sub_batch').value):null,mute=$('sub_mute').value?Number($('sub_mute').value):null;
    const headers=kind==='webhook'?parseSubHeaders($('sub_headers').value):{};
    if(subEditing){
      const body={name:$('sub_name').value,filter,batch_max_events:batch};
      if(kind==='webhook'){body.url=$('sub_url').value;body.headers=headers;body.mute_secs=mute;}
      await api('PUT','/api/subscriptions/'+encodeURIComponent(subEditing.id),body);
      $('subDlg').close();toast('Subscription updated',$('sub_name').value);
    }else{
      const created=await api('POST','/api/subscriptions',{name:$('sub_name').value,bucket:$('sub_bucket').value,kind,url:$('sub_url').value,headers,filter,initial_snapshot:$('sub_snapshot').checked,batch_max_events:batch,mute_secs:kind==='webhook'?mute:null});
      $('subDlg').close();showSubSecret(created);
    }
    await loadSubs();
  }catch(e){setInlineError('subError',e.message);}
}
function showSubSecret(result){
  const s=result.subscription;
  if(result.token){$('ss_title').textContent=`Replication token for ${s.name}`;$('ss_hint').textContent='Replicators send it as “Authorization: Bearer <token>” to GET /_rs3/changelog?after=<seq> and GET /_rs3/changelog/snapshot on the S3 port, and keep their own position.';$('ss_text').textContent=result.token;}
  else{$('ss_title').textContent=`Signing secret for ${s.name}`;$('ss_hint').textContent='Every delivery carries X-RS3-Signature: t=<unix>,v1=<hex HMAC-SHA256(secret, "<t>.<body>")>. Verify it, and reject stale timestamps.';$('ss_text').textContent=result.secret;}
  $('subSecretDlg').showModal();
}
async function copySubSecret(){const ok=await copyText($('ss_text').textContent);toast(ok?'Copied':'Copy failed',ok?'':'Select the text and press Ctrl+C',ok);}
function openSubActions(i){
  subActing=subs[i];renderSubActions();setInlineError('saError');$('sa_offset').value='';$('subActionDlg').showModal();
}
function renderSubActions(){
  const s=subActing;$('sa_name').textContent=s.name;
  $('sa_status').innerHTML=`${esc(s.kind==='webhook'?'Webhook':'Replication token')} on <code>${esc(s.bucket)}</code> · ${subStatusBadge(s)}`;
  const pause=$('sa_pause');pause.classList.toggle('hidden',!s.paused);pause.textContent=s.paused?.message||'';
  $('sa_detail').textContent=[`phase: ${s.phase}`,`offset: ${s.offset}`,`head: ${s.head??'?'}`,`oldest retained: after seq ${s.trimmed_through??'?'}`,`delivered: ${s.delivered_events} events in ${s.delivered_batches} batches`,s.last_error?`last error: ${s.last_error}`:null,s.muted_until_ms>Date.now()?`muted until: ${fmtTime(s.muted_until_ms)}`:null].filter(Boolean).join('\n');
  $('sa_rotate').textContent=s.kind==='webhook'?'Rotate secret':'Rotate token';
  document.querySelectorAll('#subActionDlg .sa-webhook').forEach(el=>el.classList.toggle('hidden',s.kind!=='webhook'));
  if(s.kind==='pull')$('sa_detail').textContent=[`last read: ${s.last_success_ms?fmtTime(s.last_success_ms):'never'}`,`served through: seq ${s.offset}`,`head: ${s.head??'?'}`,'Readers keep their own position (after=). Pause disables the token.'].join('\n');
}
async function subAction(action){
  setInlineError('saError');const body={action};
  if(action==='set_offset'){const v=$('sa_offset').value;if(v===''){setInlineError('saError','Enter an offset');return;}body.offset=Number(v);}
  try{const result=await api('POST','/api/subscriptions/'+encodeURIComponent(subActing.id)+'/action',body);subActing=result.subscription;renderSubActions();toast('Done',action.replace(/_/g,' '));await loadSubs();}
  catch(e){setInlineError('saError',e.message);await loadSubs();const fresh=subs.find(x=>x.id===subActing.id);if(fresh){subActing=fresh;renderSubActions();}}
}
function subRotate(){
  const s=subActing,what=s.kind==='webhook'?'signing secret':'replication token';
  showConfirm(`Rotate ${what}?`,s.name,`The current ${what} stops working immediately.`,async()=>{const result=await api('POST','/api/subscriptions/'+encodeURIComponent(s.id)+'/action',{action:s.kind==='webhook'?'rotate_secret':'rotate_token'});$('subActionDlg').close();showSubSecret(result);},{confirmLabel:'Rotate',busyLabel:'Rotating…'});
}
function confirmDeleteSub(i){const s=subs[i];showConfirm('Delete subscription?',s.name,`Deliveries stop and its position is discarded. ${s.kind==='pull'?'The replication token stops working.':''}`,async()=>{await api('DELETE','/api/subscriptions/'+encodeURIComponent(s.id));toast('Subscription deleted',s.name);await loadSubs();});}
