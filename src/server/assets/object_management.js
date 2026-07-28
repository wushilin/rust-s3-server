// ── objects ───────────────────────────────────────────────────────────────
// The object browser: bucket rail, breadcrumb navigation, per-folder filter
// memory, object listing, the drag-and-drop upload queue with its transfer
// centre, the object details panel, and share-link (presign) creation.
let buckets=[],bucket=null,prefix='',nextAfter=null,objectItems=[],shareKey=null,detailObject=null,filterMemory={};
let selectedKeys=new Set(),visibleItems=[],bulkCancel=false,bulkRunning=false;

async function loadBuckets(preserve=false){
  try{const data=await api('GET','/api/buckets');buckets=data.buckets||[];if(!preserve||!buckets.some(b=>b.name===bucket))bucket=buckets[0]?.name||null;renderBuckets();if(bucket){prefix=preserve?prefix:'';await loadObjects();}else{objectItems=[];renderCrumbs();renderObjects();}}catch(e){toast('Could not load buckets',e.message,false);}
}
function renderBuckets(){
  // The multipart icon is normally hover-only, like the other row actions. A
  // non-zero count pins it visible and stamps the count on its corner: a bucket
  // holding staged parts should announce itself at rest, since nothing else in
  // the rail hints that disk is being held.
  const mpCount=b=>{const n=b.multipart_uploads||0;return n?`<span class="mp-count">${n>99?'99+':n}</span>`:'';};
  $('bucketList').innerHTML=buckets.length?buckets.map((b,i)=>`<div class="bucket-item ${b.name===bucket?'active':''}" onclick="selectBucket(${i})" title="${esc(b.name)}">${icons.database}<span>${esc(b.name)}</span><button class="row-action bucket-act ${b.multipart_uploads?'has-mp':''}" title="${b.multipart_uploads?`${b.multipart_uploads} multipart upload${b.multipart_uploads===1?'':'s'} in flight`:'Ongoing multipart uploads'}" onclick="event.stopPropagation();openMultipartDialog(decodeURIComponent('${enc(b.name)}'))">${icons.layers}${mpCount(b)}</button><button class="row-action danger bucket-act" title="Delete bucket “${esc(b.name)}”" onclick="event.stopPropagation();confirmDeleteBucket(decodeURIComponent('${enc(b.name)}'))">${icons.trash}</button></div>`).join(''):`<div style="padding:20px 10px;text-align:center;color:var(--muted);font-size:12px">No buckets yet</div>`;
}
// Per-folder filter memory: leaving a folder saves its filter, arriving at one
// restores it (empty = a fresh folder resets the filter).
function locationKey(){return (bucket||'')+' '+prefix;}
function saveFilter(){if(bucket)filterMemory[locationKey()]=$('objectSearch').value;}
function restoreFilter(){$('objectSearch').value=bucket?(filterMemory[locationKey()]||''):'';}
function selectBucket(i){saveFilter();bucket=buckets[i].name;prefix='';restoreFilter();objectItems=[];renderBuckets();loadObjects();}
function renderCrumbs(){
  if(!bucket){$('crumbs').innerHTML='<span class="muted">Create a bucket to get started</span>';$('uploadBtn').disabled=true;$('uploadFolderBtn').disabled=true;$('bucketSettingsBtn').disabled=true;$('rebuildBucketBtn').disabled=true;$('bulkDeleteBtn').disabled=true;return;}
  $('uploadBtn').disabled=false;$('uploadFolderBtn').disabled=false;$('bucketSettingsBtn').disabled=false;$('rebuildBucketBtn').disabled=false;$('bulkDeleteBtn').disabled=false;const parts=prefix.split('/').filter(Boolean);let acc='';let html=`<button class="crumb-btn ${parts.length?'':'current'}" onclick="goPrefix('')">${esc(bucket)}</button>`;parts.forEach((part,i)=>{acc+=part+'/';html+=`<span class="crumb-sep">${icons['chevron-right']}</span><button class="crumb-btn ${i===parts.length-1?'current':''}" onclick="goPrefix(decodeURIComponent('${enc(acc)}'))">${esc(part)}</button>`;});$('crumbs').innerHTML=html;
}
function goPrefix(value){saveFilter();prefix=value;restoreFilter();closeDetails();loadObjects();}
async function loadObjects(more=false){
  if(!bucket){renderObjects();return;}if(!more){nextAfter=null;objectItems=[];selectedKeys.clear();}renderCrumbs();
  try{const q=new URLSearchParams({bucket,prefix});if(more&&nextAfter)q.set('after',nextAfter);const data=await api('GET','/api/objects?'+q);objectItems.push(...data.common_prefixes.map(value=>({type:'folder',key:value,name:value.slice(prefix.length).replace(/\/$/,''),size:null,last_modified_ms:null})),...data.entries.map(value=>({...value,type:'object',name:value.key.slice(prefix.length)})));nextAfter=data.is_truncated?data.next_after:null;$('moreWrap').classList.toggle('hidden',!nextAfter);renderObjects();}catch(e){toast('Could not load objects',e.message,false);renderObjects();}
}
function renderObjects(){
  renderCrumbs();const query=$('objectSearch').value.trim().toLowerCase();const visible=objectItems.filter(o=>o.name.toLowerCase().includes(query));visibleItems=visible;$('objectCount').textContent=bucket?`${visible.length}${nextAfter?'+':''} item${visible.length===1?'':'s'}`:'';
  const checkCell=o=>`<td class="row-check"><input type="checkbox" ${selectedKeys.has(o.key)?'checked':''} onclick="toggleSelect(decodeURIComponent('${enc(o.key)}'),this.checked)"></td>`;
  $('objRows').innerHTML=visible.map((o,i)=>o.type==='folder'?`<tr>${checkCell(o)}<td><div class="object-name"><span class="file-icon folder">${icons.folder}</span><button onclick="goPrefix(decodeURIComponent('${enc(o.key)}'))">${esc(o.name)}</button></div></td><td class="hide-mobile muted">—</td><td class="hide-tablet muted">—</td><td class="actions"><button class="row-action" title="Open folder" onclick="goPrefix(decodeURIComponent('${enc(o.key)}'))">${icons['chevron-right']}</button></td></tr>`:`<tr>${checkCell(o)}<td><div class="object-name"><span class="file-icon">${icons.file}</span><button onclick="openDetails(${objectItems.indexOf(o)})" title="${esc(o.name)}">${esc(o.name)}</button></div></td><td class="hide-mobile">${fmtSize(o.size)}</td><td class="hide-tablet muted">${fmtTime(o.last_modified_ms)}</td><td class="actions"><a class="row-action" title="Download" href="${objectUrl(o.key)}" download>${icons.download}</a><button class="row-action" title="Share" onclick="openShare(decodeURIComponent('${enc(o.key)}'))">${icons.share}</button><button class="row-action danger" title="Delete" onclick="confirmDeleteObject(decodeURIComponent('${enc(o.key)}'))">${icons.trash}</button></td></tr>`).join('');
  const selAll=$('selAll'),selCount=visible.filter(o=>selectedKeys.has(o.key)).length;selAll.disabled=!visible.length;selAll.checked=visible.length>0&&selCount===visible.length;selAll.indeterminate=selCount>0&&selCount<visible.length;
  const delBtn=$('deleteSelectedBtn');delBtn.classList.toggle('hidden',!selectedKeys.size);if(selectedKeys.size)delBtn.innerHTML=icons.trash+` Delete ${selectedKeys.size} selected`;
  const empty=!visible.length;$('objectTableWrap').querySelector('table').classList.toggle('hidden',empty);$('objectEmpty').classList.toggle('hidden',!empty);if(empty)$('objectEmpty').innerHTML=`<div class="empty-icon">${icons[bucket?'folder':'database']}</div><h3>${query?'No matching objects':bucket?'This location is empty':'No buckets yet'}</h3><p>${query?'Try a different filter.':bucket?'Upload files by using the button above or dragging them into this window.':'Create your first bucket to begin storing objects.'}</p>${!bucket&&me?.is_admin?'<button class="btn primary" onclick="openBucketDialog()">Create bucket</button>':''}`;
}
function objectUrl(key){return `/api/object?bucket=${encodeURIComponent(bucket)}&key=${encodeURIComponent(key)}`;}
function openBucketDialog(){$('newBucketName').value='';setInlineError('bucketError');$('bucketDlg').showModal();setTimeout(()=>$('newBucketName').focus(),50);}
async function createBucket(event){event.preventDefault();const name=$('newBucketName').value.trim();if(!name){setInlineError('bucketError','Enter a bucket name.');return;}try{await api('POST','/api/buckets',{name});$('bucketDlg').close();toast('Bucket created',name);await loadBuckets();const index=buckets.findIndex(b=>b.name===name);if(index>=0)selectBucket(index);}catch(e){setInlineError('bucketError',e.message);}}
let bucketSettingsTarget=null;
const CORS_SAMPLE=JSON.stringify([{allowed_origins:['https://app.example.com'],allowed_methods:['GET','PUT'],allowed_headers:['content-type','x-amz-*'],expose_headers:['ETag','x-amz-request-id'],max_age_seconds:3600}],null,2);
async function openBucketSettings(){if(!bucket)return;bucketSettingsTarget=bucket;$('bucketSettingsName').textContent=bucket;setInlineError('bucketSettingsError');try{const data=await api('GET','/api/buckets/'+encodeURIComponent(bucket)+'/cors');$('bucketConsoleOrigin').value=data.console_origin||'Not configured';const rules=data.rules||[];$('bucketCorsJson').value=rules.length?JSON.stringify(rules,null,2):'';$('bucketCorsJson').placeholder=CORS_SAMPLE;$('bucketSettingsDlg').showModal();}catch(e){toast('Could not load bucket settings',e.message,false);}}
function validateCorsRules(rules){if(!Array.isArray(rules))throw new Error('CORS rules must be a JSON array.');if(rules.length>100)throw new Error('A bucket can have at most 100 CORS rules.');const methods=new Set(['GET','PUT','POST','DELETE','HEAD']);for(let i=0;i<rules.length;i++){const r=rules[i],at=`Rule ${i+1}`;if(!r||typeof r!=='object'||Array.isArray(r))throw new Error(`${at} must be an object.`);for(const field of ['allowed_origins','allowed_methods','allowed_headers','expose_headers']){if(!Array.isArray(r[field])||r[field].some(v=>typeof v!=='string'||!v.trim()))throw new Error(`${at}: ${field} must be an array of non-empty strings.`);}if(!r.allowed_origins.length)throw new Error(`${at} needs at least one allowed origin.`);if(!r.allowed_methods.length)throw new Error(`${at} needs at least one allowed method.`);if(r.allowed_methods.some(v=>!methods.has(v.toUpperCase())))throw new Error(`${at} has an unsupported method.`);if([...r.allowed_origins,...r.allowed_headers].some(v=>(v.match(/\*/g)||[]).length>1))throw new Error(`${at}: origins and headers may contain at most one wildcard.`);if(r.max_age_seconds!==undefined&&(!Number.isInteger(r.max_age_seconds)||r.max_age_seconds<0||r.max_age_seconds>4294967295))throw new Error(`${at}: max_age_seconds must be an integer from 0 to 4294967295.`);}}
async function saveBucketSettings(){let rules;try{rules=JSON.parse($('bucketCorsJson').value||'[]');validateCorsRules(rules);}catch(e){setInlineError('bucketSettingsError',e.message);return;}try{await api('PUT','/api/buckets/'+encodeURIComponent(bucketSettingsTarget)+'/cors',{rules});$('bucketSettingsDlg').close();toast('Bucket settings saved',bucketSettingsTarget);}catch(e){setInlineError('bucketSettingsError',e.message);}}
function openCorsSample(){$('corsSampleJson').value=CORS_SAMPLE;$('corsSampleDlg').showModal();}
async function copyCorsSample(){const ok=await copyText(CORS_SAMPLE);toast(ok?'Sample copied':'Copy failed',ok?'Paste it into the CORS editor and change the origin.':'Select the sample and press Ctrl+C',ok);}
let deletingBucket=null,deletingObjectCount=0;
function rebuildBucket(){
  if(!bucket)return;const target=bucket;
  showConfirm('Rebuild index?',target,`Rebuilds “${target}” from the objects on disk. While it runs the bucket returns 503 for data operations (the console stays available); progress appears in the tasks panel.`,async()=>{await api('POST','/api/buckets/'+encodeURIComponent(target)+'/rebuild');toast('Rebuild started',target);refreshTasks();},{confirmLabel:'Rebuild index',busyLabel:'Starting…',danger:false});
}
async function confirmDeleteBucket(target){
  if(!target)return;
  try{const stats=await api('GET','/api/buckets/'+encodeURIComponent(target)+'/stats');if(stats.objects===0){showConfirm('Delete empty bucket?',target,'The empty bucket will be permanently deleted.',async()=>{await api('DELETE','/api/buckets/'+encodeURIComponent(target));toast('Bucket deleted',target);await loadBuckets();});return;}deletingBucket=target;deletingObjectCount=Number(stats.objects);$('emptyBucketSubtitle').textContent=`${deletingObjectCount.toLocaleString()} object${deletingObjectCount===1?' is':'s are'} still in “${target}”. Empty the bucket before deleting it?`;$('emptyBucketConfirm').value='';$('emptyBucketConfirm').disabled=false;$('emptyBucketProgress').classList.add('hidden');$('emptyBucketProgressBar').style.width='0';$('emptyBucketPercent').textContent='0%';$('emptyBucketAction').disabled=false;$('emptyBucketCancel').disabled=false;$('emptyBucketClose').disabled=false;setInlineError('emptyBucketError');$('emptyBucketDlg').showModal();setTimeout(()=>$('emptyBucketConfirm').focus(),50);}catch(e){toast('Cannot delete bucket',e.message,false);}
}
async function emptyAndDeleteBucket(event){
  event.preventDefault();if($('emptyBucketConfirm').value!=='CONFIRM'){setInlineError('emptyBucketError','Type CONFIRM exactly to continue.');return;}
  const target=deletingBucket;let deleted=0;setInlineError('emptyBucketError');$('emptyBucketConfirm').disabled=true;$('emptyBucketAction').disabled=true;$('emptyBucketCancel').disabled=true;$('emptyBucketClose').disabled=true;$('emptyBucketProgress').classList.remove('hidden');cancelUploadsForBucket(target);
  try{
    while(true){const data=await api('GET','/api/objects?'+new URLSearchParams({bucket:target,prefix:'',recursive:'true'}));const entries=data.entries||[];if(!entries.length)break;for(let i=0;i<entries.length;i+=6){const batch=entries.slice(i,i+6);await Promise.all(batch.map(entry=>api('DELETE',`/api/object?bucket=${encodeURIComponent(target)}&key=${encodeURIComponent(entry.key)}`)));deleted+=batch.length;const percent=Math.min(99,Math.round(deleted/Math.max(deletingObjectCount,deleted)*100));$('emptyBucketStatus').textContent=`Deleted ${deleted.toLocaleString()} of ${Math.max(deletingObjectCount,deleted).toLocaleString()} objects`;$('emptyBucketPercent').textContent=percent+'%';$('emptyBucketProgressBar').style.width=percent+'%';}}
    $('emptyBucketStatus').textContent='Deleting bucket…';await api('DELETE','/api/buckets/'+encodeURIComponent(target));$('emptyBucketPercent').textContent='100%';$('emptyBucketProgressBar').style.width='100%';$('emptyBucketDlg').close();toast('Bucket emptied and deleted',target);await loadBuckets();
  }catch(e){setInlineError('emptyBucketError',e.message);$('emptyBucketAction').disabled=false;$('emptyBucketCancel').disabled=false;$('emptyBucketClose').disabled=false;}
}

function openDetails(index){const o=objectItems[index];if(!o||o.type!=='object')return;detailObject=o;$('detailList').innerHTML=`<div class="detail-row"><dt>Object name</dt><dd>${esc(o.name)}</dd></div><div class="detail-row"><dt>Full path</dt><dd>${esc(o.key)}</dd></div><div class="detail-row"><dt>Bucket</dt><dd>${esc(bucket)}</dd></div><div class="detail-row"><dt>Size</dt><dd>${fmtSize(o.size)}</dd></div><div class="detail-row"><dt>Modified</dt><dd>${fmtTime(o.last_modified_ms)}</dd></div><div class="detail-row"><dt>ETag</dt><dd><code>${esc(o.etag||'—')}</code></dd></div>`;$('detailDownload').href=objectUrl(o.key);$('detailDownload').setAttribute('download','');$('detailsPanel').classList.add('open');}
function closeDetails(){$('detailsPanel')?.classList.remove('open');detailObject=null;}
function openShareFromDetails(){if(detailObject)openShare(detailObject.key);}
function openShare(key){shareKey=key;$('shareObjectName').textContent=key;$('shareResult').classList.add('hidden');$('sh_url').value='';$('shareAction').innerHTML=icons.link+' Generate link';$('shareAction').onclick=doPresign;setInlineError('shareError');$('shareDlg').showModal();}
async function doPresign(){try{const data=await api('POST','/api/presign',{bucket,key:shareKey,expires_secs:+$('sh_exp').value});$('sh_url').value=data.url;$('shareResult').classList.remove('hidden');$('shareAction').innerHTML=icons.copy+' Copy link';$('shareAction').onclick=copyShare;}catch(e){setInlineError('shareError',e.message);}}
async function copyShare(){const ok=await copyText($('sh_url').value);toast(ok?'Link copied':'Copy failed',ok?'':'Select the link and press Ctrl+C',ok);}
function confirmDeleteObject(key){showConfirm('Delete object?',key,`This permanently deletes “${key}”. This action cannot be undone.`,async()=>{await api('DELETE',objectUrl(key));closeDetails();toast('Object deleted',key);await loadObjects();});}

// ── ongoing multipart uploads (per-bucket rail icon) ───────────────────────
let mpBucket=null;
async function openMultipartDialog(name){mpBucket=name;$('mp_bucket').textContent=name;setInlineError('mpError');$('mpList').innerHTML='<div class="muted" style="padding:20px 0;text-align:center">Loading…</div>';$('mpDlg').showModal();await loadMultipartUploads();}
async function loadMultipartUploads(){
  if(!mpBucket)return;setInlineError('mpError');
  try{
    const data=await api('GET','/api/multipart/list?bucket='+encodeURIComponent(mpBucket));const uploads=data.uploads||[];
    $('mpList').innerHTML=uploads.length?uploads.map(u=>`<div class="mp-row"><span class="file-icon">${icons.layers}</span><div style="flex:1;min-width:0"><strong style="display:block;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${esc(u.key)}">${esc(u.key)}</strong><div class="muted" style="font-size:11.5px">Started ${fmtTime(u.initiated_at_ms)} · id <code>${esc(u.upload_id)}</code></div></div><button class="row-action danger" title="Abort upload" onclick="confirmAbortMultipart(decodeURIComponent('${enc(u.key)}'),decodeURIComponent('${enc(u.upload_id)}'))">${icons.x}</button></div>`).join(''):`<div class="empty" style="padding:26px 10px"><div class="empty-icon">${icons.layers}</div><h3>No multipart uploads in flight</h3><p>Large console uploads and S3 multipart sessions appear here until they complete or abort.</p></div>`;
    setBucketMultipartCount(mpBucket,uploads.length);
  }catch(e){setInlineError('mpError',e.message);$('mpList').innerHTML='';}
}
// Badge upkeep. The dialog and the upload queue know a bucket's count first
// hand, so they set it directly; the poll is the catch-all for uploads started
// by S3 clients outside this console. It re-renders only on a real change, so a
// rail that isn't moving costs nothing but the request.
function setBucketMultipartCount(name,count){const target=buckets.find(b=>b.name===name);if(!target||(target.multipart_uploads||0)===count)return;target.multipart_uploads=count;renderBuckets();}
async function refreshBucketBadges(){
  if(!buckets.length)return;
  try{
    const data=await api('GET','/api/buckets');const counts=new Map((data.buckets||[]).map(b=>[b.name,b.multipart_uploads||0]));
    let changed=false;buckets.forEach(b=>{const n=counts.get(b.name)||0;if((b.multipart_uploads||0)!==n){b.multipart_uploads=n;changed=true;}});
    if(changed)renderBuckets();
  }catch{}
}
let bucketBadgeTimer=null;
function startBucketBadgePolling(){if(bucketBadgeTimer)return;bucketBadgeTimer=setInterval(()=>{if(document.hidden||$('tab_objects').classList.contains('hidden'))return;refreshBucketBadges();},20000);}
function confirmAbortMultipart(key,uploadId){
  const target=mpBucket;
  showConfirm('Abort multipart upload?',key,'All parts staged for this upload will be discarded. A client still uploading will fail its next part.',async()=>{await api('DELETE',`/api/multipart/abort?bucket=${encodeURIComponent(target)}&key=${encodeURIComponent(key)}&upload_id=${encodeURIComponent(uploadId)}`);toast('Upload aborted',key);await loadMultipartUploads();await refreshBucketBadges();},{confirmLabel:'Abort upload',busyLabel:'Aborting…'});
}

// ── bulk delete ───────────────────────────────────────────────────────────
// Two entry points share one engine: "Delete N selected" (checked rows;
// folders delete recursively) and "Bulk Delete" (an explicit key prefix).
// The engine deletes known keys in batches of 6, then drains each prefix by
// re-listing recursively and deleting until a listing comes back empty. A
// round that deletes nothing aborts (permission failures would loop forever).
function toggleSelect(key,checked){if(checked)selectedKeys.add(key);else selectedKeys.delete(key);renderObjects();}
function toggleSelectAll(checked){visibleItems.forEach(o=>{if(checked)selectedKeys.add(o.key);else selectedKeys.delete(o.key);});renderObjects();}
function confirmDeleteSelected(){
  const items=objectItems.filter(o=>selectedKeys.has(o.key));if(!items.length)return;
  const objects=items.filter(o=>o.type==='object').map(o=>o.key),folders=items.filter(o=>o.type==='folder').map(o=>o.key);
  const parts=[];if(objects.length)parts.push(`${objects.length} object${objects.length===1?'':'s'}`);if(folders.length)parts.push(`${folders.length} folder${folders.length===1?'':'s'} and everything inside`);
  showConfirm('Delete selected items?',`${bucket}/${prefix}`,`This permanently deletes ${parts.join(' and ')}. This action cannot be undone.`,()=>{runBulkDelete(objects,folders);});
}
function openBulkDelete(){if(!bucket)return;$('bulkPrefixSubtitle').textContent=`Delete every object under a prefix in “${bucket}”.`;$('bulkPrefixInput').value=prefix;setInlineError('bulkPrefixError');$('bulkPrefixDlg').showModal();setTimeout(()=>$('bulkPrefixInput').focus(),50);}
function startBulkPrefixDelete(event){
  event.preventDefault();const target=$('bulkPrefixInput').value.trim();
  if(!target){setInlineError('bulkPrefixError','Enter a prefix — deleting the whole bucket from here is not allowed.');return;}
  $('bulkPrefixDlg').close();
  showConfirm('Bulk delete by prefix?',`${bucket}/${target}`,`This permanently deletes every object whose key starts with “${target}”. This action cannot be undone.`,()=>{runBulkDelete([],[target]);},{confirmLabel:'Bulk Delete',busyLabel:'Starting…'});
}
function stopBulkDelete(){bulkCancel=true;$('bulkProgressStatus').textContent='Stopping…';}
async function runBulkDelete(objectKeys,prefixes){
  const targetBucket=bucket;bulkCancel=false;bulkRunning=true;let deleted=0,failed=0,discovered=objectKeys.length;
  $('bulkProgressSubtitle').textContent=`Bucket “${targetBucket}”`;$('bulkProgressStatus').textContent='Preparing…';$('bulkProgressCount').textContent='';$('bulkProgressBar').style.width='0';
  setInlineError('bulkProgressError');$('bulkProgressStop').classList.remove('hidden');$('bulkProgressClose').classList.add('hidden');$('bulkProgressDlg').showModal();
  const del=key=>api('DELETE',`/api/object?bucket=${encodeURIComponent(targetBucket)}&key=${encodeURIComponent(key)}`).then(()=>{deleted++;}).catch(()=>{failed++;});
  const paint=label=>{$('bulkProgressStatus').textContent=label;$('bulkProgressCount').textContent=`${(deleted+failed).toLocaleString()} / ${discovered.toLocaleString()}${failed?` (${failed} failed)`:''}`;$('bulkProgressBar').style.width=Math.min(100,discovered?Math.round((deleted+failed)/discovered*100):0)+'%';};
  try{
    for(let i=0;i<objectKeys.length&&!bulkCancel;i+=6){await Promise.all(objectKeys.slice(i,i+6).map(del));paint('Deleting objects…');}
    for(const p of prefixes){
      while(!bulkCancel){
        const data=await api('GET','/api/objects?'+new URLSearchParams({bucket:targetBucket,prefix:p,recursive:'true'}));
        const entries=(data.entries||[]).map(e=>e.key);if(!entries.length)break;
        discovered+=entries.length;const before=deleted;
        for(let i=0;i<entries.length&&!bulkCancel;i+=6){await Promise.all(entries.slice(i,i+6).map(del));paint(`Deleting “${p}”…`);}
        if(deleted===before)throw new Error(`Nothing under “${p}” could be deleted (${failed} failure${failed===1?'':'s'}) — stopping.`);
      }
      if(bulkCancel)break;
    }
    $('bulkProgressDlg').close();
    toast(bulkCancel?'Bulk delete stopped':'Bulk delete finished',`${deleted.toLocaleString()} object${deleted===1?'':'s'} deleted${failed?`, ${failed} failed`:''}`,!failed&&!bulkCancel);
  }catch(e){setInlineError('bulkProgressError',e.message);$('bulkProgressStop').classList.add('hidden');$('bulkProgressClose').classList.remove('hidden');}
  finally{bulkRunning=false;loadObjects();}
}
