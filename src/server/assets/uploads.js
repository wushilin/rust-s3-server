// ── uploads ────────────────────────────────────────────────────────────────
// The upload pipeline: drag-and-drop (files and folders), the folder picker,
// the transfer centre queue, and the two wire strategies — a single PUT for
// small files, and transparent multipart (create → streamed parts → complete)
// for anything larger than MP_THRESHOLD. Parts stream from disk via Blob
// slices; nothing is buffered whole on either side.
const dropZone=$('dropZone');let dragDepth=0;
dropZone.addEventListener('dragenter',e=>{e.preventDefault();dragDepth++;dropZone.classList.add('drop-active');});
dropZone.addEventListener('dragover',e=>e.preventDefault());
dropZone.addEventListener('dragleave',e=>{e.preventDefault();dragDepth=Math.max(0,dragDepth-1);if(!dragDepth)dropZone.classList.remove('drop-active');});
// Folder drops: capture directory entries synchronously (they are only valid
// during the drop event), then walk them asynchronously. Plain file drops keep
// the simple dataTransfer.files path.
dropZone.addEventListener('drop',e=>{e.preventDefault();dragDepth=0;dropZone.classList.remove('drop-active');const items=[...(e.dataTransfer.items||[])];const entries=items.map(item=>item.webkitGetAsEntry&&item.webkitGetAsEntry()).filter(Boolean);if(entries.some(entry=>entry.isDirectory)){Promise.all(entries.map(entry=>walkDropEntry(entry,''))).then(lists=>uploadFiles(lists.flat())).catch(err=>toast('Could not read dropped folder',err?.message||'',false));}else uploadFiles(e.dataTransfer.files);});
function walkDropEntry(entry,dir){
  if(entry.isFile)return new Promise((resolve,reject)=>entry.file(file=>resolve([{file,rel:dir+file.name}]),reject));
  if(!entry.isDirectory)return Promise.resolve([]);
  const reader=entry.createReader();
  // readEntries returns at most ~100 entries per call — drain until empty.
  const readAll=new Promise((resolve,reject)=>{const acc=[];const step=()=>reader.readEntries(batch=>{if(!batch.length)return resolve(acc);acc.push(...batch);step();},reject);step();});
  return readAll.then(children=>Promise.all(children.map(child=>walkDropEntry(child,dir+entry.name+'/'))).then(lists=>lists.flat()));
}
const MP_THRESHOLD=1024*1024*1024;            // files above 1 GiB upload as multipart
const MP_PART_SIZE=256*1024*1024;             // 256 MiB parts (grows for >2.5 TiB files: max 10,000 parts)
const MAX_UI_UPLOAD_BYTES=5*1024*1024*1024*1024; // 5 TiB — the S3 object size ceiling
let uploadTaskId=0,uploadTasks=[],activeUploadCount=0,uploadReloadTimer=null;
// Accepts a FileList/array of File (folder-picker files carry their path in
// webkitRelativePath) or `{file, rel}` pairs from a folder drop; `rel` becomes
// the key suffix, so folder uploads recreate their tree under the current prefix.
function uploadFiles(fileList){const items=[...fileList].map(item=>item instanceof File?{file:item,rel:item.webkitRelativePath||item.name}:item);if(!items.length||!bucket)return;const accepted=items.filter(({file,rel})=>{if(file.size<=MAX_UI_UPLOAD_BYTES)return true;toast('File is too large',`${rel} exceeds the 5 TiB S3 object limit.`,false);return false;});const targetBucket=bucket,targetPrefix=prefix;for(const {file,rel} of accepted)uploadTasks.push({id:++uploadTaskId,file,bucket:targetBucket,key:targetPrefix+rel,status:'queued',progress:0,xhr:null,error:''});$('fileInput').value='';$('folderInput').value='';renderTransfers();processUploadQueue();}
function processUploadQueue(){while(activeUploadCount<3){const task=uploadTasks.find(t=>t.status==='queued');if(!task)break;task.status='uploading';activeUploadCount++;renderTransfers();runUpload(task).then(()=>{task.status='complete';task.progress=100;scheduleUploadRefresh(task);}).catch(e=>{if(task.status!=='cancelled'){task.status='error';task.error=e.message;}}).finally(()=>{task.xhr=null;activeUploadCount--;renderTransfers();processUploadQueue();});}}
function runUpload(task){return task.file.size>MP_THRESHOLD?runMultipartUpload(task):runSinglePut(task);}
function runSinglePut(task){return new Promise((resolve,reject)=>{const xhr=task.xhr=new XMLHttpRequest();xhr.open('PUT',`/api/object?bucket=${encodeURIComponent(task.bucket)}&key=${encodeURIComponent(task.key)}`);xhr.setRequestHeader('content-type',task.file.type||'application/octet-stream');xhr.upload.onprogress=e=>{if(e.lengthComputable){task.progress=e.loaded/e.total*100;renderTransfers();}};xhr.onload=()=>{if(xhr.status>=200&&xhr.status<300)resolve();else{let message=xhr.statusText;try{message=JSON.parse(xhr.responseText).error||message;}catch{}reject(new Error(message||'Upload failed'));}};xhr.onerror=()=>reject(new Error('Network error during upload'));xhr.onabort=()=>reject(new Error('Upload cancelled'));xhr.send(task.file);});}
// Multipart: parts go up sequentially (cancel aborts the in-flight part's XHR,
// which surfaces here and triggers a server-side abort to free staged parts).
async function runMultipartUpload(task){
  const q=`bucket=${encodeURIComponent(task.bucket)}&key=${encodeURIComponent(task.key)}`;
  const created=await api('POST',`/api/multipart/create?${q}&content_type=${encodeURIComponent(task.file.type||'application/octet-stream')}`);
  const uploadId=created.upload_id;
  try{
    const size=task.file.size,partSize=Math.max(MP_PART_SIZE,Math.ceil(size/10000));
    const parts=[];
    for(let number=1,offset=0;offset<size;number++,offset+=partSize){
      if(task.status==='cancelled')throw new Error('Upload cancelled');
      const blob=task.file.slice(offset,Math.min(offset+partSize,size));
      const data=await sendPart(task,q,uploadId,number,blob,offset,size);
      parts.push({part_number:number,etag:data.etag});
    }
    await api('POST','/api/multipart/complete',{bucket:task.bucket,key:task.key,upload_id:uploadId,parts});
  }catch(e){
    api('DELETE',`/api/multipart/abort?${q}&upload_id=${encodeURIComponent(uploadId)}`).catch(()=>{});
    throw e;
  }
}
function sendPart(task,q,uploadId,number,blob,sentSoFar,total){
  return new Promise((resolve,reject)=>{
    const xhr=task.xhr=new XMLHttpRequest();
    xhr.open('PUT',`/api/multipart/part?${q}&upload_id=${encodeURIComponent(uploadId)}&part_number=${number}`);
    xhr.setRequestHeader('content-type','application/octet-stream');
    xhr.upload.onprogress=e=>{if(e.lengthComputable){task.progress=(sentSoFar+e.loaded)/total*100;renderTransfers();}};
    xhr.onload=()=>{if(xhr.status>=200&&xhr.status<300){try{resolve(JSON.parse(xhr.responseText));}catch{reject(new Error('Malformed part response'));}}else{let message=xhr.statusText;try{message=JSON.parse(xhr.responseText).error||message;}catch{}reject(new Error(message||'Part upload failed'));}};
    xhr.onerror=()=>reject(new Error('Network error during upload'));
    xhr.onabort=()=>reject(new Error('Upload cancelled'));
    xhr.send(blob);
  });
}
function cancelUpload(id){const task=uploadTasks.find(t=>t.id===id);if(!task||!['queued','uploading'].includes(task.status))return;task.status='cancelled';task.error='';if(task.xhr)task.xhr.abort();renderTransfers();processUploadQueue();}
function cancelUploadsForBucket(name){uploadTasks.filter(t=>t.bucket===name&&['queued','uploading'].includes(t.status)).forEach(t=>cancelUpload(t.id));}
function clearTransfers(){uploadTasks=uploadTasks.filter(t=>['queued','uploading'].includes(t.status));renderTransfers();}
function scheduleUploadRefresh(task){if(task.bucket!==bucket||!task.key.startsWith(prefix))return;clearTimeout(uploadReloadTimer);uploadReloadTimer=setTimeout(()=>loadObjects(),350);}
function renderTransfers(){const center=$('transferCenter');center.classList.toggle('hidden',!uploadTasks.length);if(!uploadTasks.length)return;const pending=uploadTasks.filter(t=>['queued','uploading'].includes(t.status)).length;$('transferCount').textContent=pending?`${pending} active`:`${uploadTasks.length} finished`;$('clearTransfersBtn').disabled=uploadTasks.every(t=>['queued','uploading'].includes(t.status));$('transferList').innerHTML=uploadTasks.map(task=>{const status=task.status==='uploading'?Math.round(task.progress)+'%':task.status==='queued'?'Waiting':task.status==='complete'?'Complete':task.status==='cancelled'?'Cancelled':'Failed';const stateClass=task.status==='complete'?'ok':['error','cancelled'].includes(task.status)?'err':'';return `<div class="transfer-row"><div class="transfer-name"><strong title="${esc(task.file.name)}">${esc(task.file.name)}</strong><span title="${esc(task.bucket+'/'+task.key)}">${esc(task.bucket+'/'+task.key)} · ${fmtSize(task.file.size)}${task.error?' · '+esc(task.error):''}</span>${task.status==='uploading'?`<div class="progress"><span style="width:${task.progress}%"></span></div>`:''}</div><div class="transfer-state ${stateClass}">${status}</div>${['queued','uploading'].includes(task.status)?`<button class="row-action danger" title="Cancel upload" onclick="cancelUpload(${task.id})">${icons.x}</button>`:'<span></span>'}</div>`;}).join('');}
