// ── tasks ─────────────────────────────────────────────────────────────────
// The live task stream: an enveloped WebSocket that pushes a snapshot of active
// and just-finished work, with a polling fallback and a client-side probe for
// reconciling stale rows. Rendered into the top-bar popover on the console and,
// unchanged, into the standalone /tasks page — this module is the single source
// of truth for "what is the server doing right now".
let badgeTimer=null,taskBytes=null,dismissedTasks=new Set(),taskSeen={},tasksWs=null,tasksWatchdogTimer=null,lastMsgAt=0,probeSeq=0,lastTasksData=null;
const COMPLETED_LINGER=5000, PROBE_STALE_MS=10000;

function toggleTasks(){const pop=$('tasksPopover');const opening=pop.classList.contains('hidden');pop.classList.toggle('hidden');if(opening){if(lastTasksData)renderTasksData(lastTasksData);else refreshTasks();}}
function closeTasks(){$('tasksPopover').classList.add('hidden');}

// Map a (verbose) operation name to a compact glyph. The full name is kept as
// the row's tooltip, so the icon just needs to convey the category at a glance.
function opIcon(op,kind){
  const o=(op||'').toUpperCase();
  // Order matters: listings must be caught before UPLOAD/MULTIPART so that
  // LIST_MULTIPART_UPLOADS reads as a list, not an upload.
  if(o.includes('DOWNLOAD'))return 'download';
  if(o.includes('DELETE')||o.includes('ABORT')||o.includes('TRASH'))return 'trash';
  if(o.includes('LIST')||o.includes('HEAD')||o.includes('LOCATION'))return 'list';
  if(o.includes('UPLOAD')||o.includes('MULTIPART')||o==='PUT'||o.includes('COPY')||o.includes('COMPLETE')||o==='POST')return 'upload';
  if(o.includes('CREATE'))return 'plus';
  if(o.includes('REBUILD')||o.includes('INDEX'))return 'database';
  return kind==='job'?'activity':'list';
}

// ── live task stream (WebSocket, enveloped) ──
function connectTasksWs(){
  if(!me?.is_admin||tasksWs)return;
  let opened=false,ws;
  try{ws=new WebSocket((location.protocol==='https:'?'wss:':'ws:')+'//'+location.host+'/api/tasks/ws');}catch{startTaskPolling();return;}
  tasksWs=ws;
  ws.onopen=()=>{opened=true;lastMsgAt=Date.now();stopTaskPolling();startWatchdog();};
  ws.onmessage=e=>{lastMsgAt=Date.now();let env;try{env=JSON.parse(e.data);}catch{return;}if(env.type==='snapshot')renderTasksData(env);else if(env.type==='reply')handleProbeReply(env);};
  ws.onerror=()=>{try{ws.close();}catch{}};
  ws.onclose=()=>{tasksWs=null;stopWatchdog();if(!me?.is_admin)return;if(opened){setTimeout(connectTasksWs,1500);}else{startTaskPolling();setTimeout(connectTasksWs,5000);}};
}
function startWatchdog(){stopWatchdog();tasksWatchdogTimer=setInterval(()=>{if(!tasksWs)return;if(Date.now()-lastMsgAt>PROBE_STALE_MS){sendProbe(Object.keys(taskSeen));setTimeout(()=>{if(tasksWs&&Date.now()-lastMsgAt>PROBE_STALE_MS){try{tasksWs.close();}catch{}}},4000);}},5000);}
function stopWatchdog(){clearInterval(tasksWatchdogTimer);tasksWatchdogTimer=null;}
function sendProbe(ids){if(!tasksWs||tasksWs.readyState!==1)return;try{tasksWs.send(JSON.stringify({type:'probe',id:'p'+(++probeSeq),tasks:ids}));}catch{}}
function handleProbeReply(env){const t=env.tasks||{};let gone=false;for(const id in t){if(t[id]===null&&taskSeen[id]!==undefined){delete taskSeen[id];gone=true;}}if(gone&&lastTasksData){lastTasksData.tasks=(lastTasksData.tasks||[]).filter(x=>t[x.id]!==null);renderTasksData(lastTasksData);}}
function startTaskPolling(){if(badgeTimer)return;refreshTasks();badgeTimer=setInterval(refreshTasks,3000);}
function stopTaskPolling(){clearInterval(badgeTimer);badgeTimer=null;}
async function refreshTasks(){if(!me?.is_admin)return;try{renderTasksData(await api('GET','/api/tasks'));}catch{}}
function renderTasksData(data){
  lastTasksData=data;const tasks=data.tasks||[];const active=tasks.filter(t=>!t.completed);
  const badge=$('taskBadge');badge.classList.remove('hidden');badge.classList.toggle('idle',active.length===0);$('taskBadgeLabel').textContent=active.length===0?'Idle':`${active.length} running`;
  if(taskBytes&&data.now_ms>taskBytes.at){const dt=(data.now_ms-taskBytes.at)/1000;const up=Math.max(0,(data.bytes_in-taskBytes.in)/dt);const dn=Math.max(0,(data.bytes_out-taskBytes.out)/dt);$('taskBadgeRate').innerHTML=`<span class="rate-up">↑ ${fmtSize(Math.round(up))}/s</span> <span class="rate-dn">↓ ${fmtSize(Math.round(dn))}/s</span>`;}
  taskBytes={in:data.bytes_in,out:data.bytes_out,at:data.now_ms};
  const now=data.now_ms;const seen={};for(const t of active)seen[t.id]=now;taskSeen=seen;
  if($('tasksPopover').classList.contains('hidden'))return;
  $('tasksActive').innerHTML=active.map(t=>taskRowHtml(t,now,false)).join('');
  const completedNow=new Set();for(const t of tasks)if(t.completed){completedNow.add(t.id);showCompleted(t,now);}
  for(const id of [...dismissedTasks])if(!completedNow.has(id))dismissedTasks.delete(id);
  updateTasksEmpty(active.length);
}
function taskRowHtml(t,now,done){const up=fmtDur(now-t.started_at_ms);const status=t.status||'';const cancel=(!done&&t.cancellable)?`<button class="task-cancel" title="Cancel task" onclick="cancelTask('${esc(t.id)}')"><span class="stop"></span></button>`:'<span class="task-cancel-spacer"></span>';const bar=done?'':(t.total>0?`<div class="task-bar"><span style="width:${Math.min(100,Math.round(t.done/t.total*100))}%"></span></div>`:(t.kind==='job'?`<div class="task-bar indet"><span></span></div>`:''));const glyph=icons[done?'check':opIcon(t.op,t.kind)]||icons.activity;return `<div class="task-row${done?' done':''}"><span class="task-op ${done?'done':esc(t.kind)}" title="${esc(t.op)}">${glyph}</span><div class="task-main"><span class="task-target" title="${esc(t.target)}">${esc(t.target)}</span><span class="task-status" title="${esc(status)}">${esc(status)}</span>${bar}</div><span class="task-up" title="running for ${esc(up)}">${esc(up)}</span>${cancel}</div>`;}
function showCompleted(t,now){if(dismissedTasks.has(t.id))return;dismissedTasks.add(t.id);const wrap=document.createElement('div');wrap.innerHTML=taskRowHtml(t,now,true);const row=wrap.firstElementChild;$('tasksDone').append(row);setTimeout(()=>{row.classList.add('vanish');row.addEventListener('transitionend',()=>{row.remove();updateTasksEmpty($('tasksActive').children.length);},{once:true});setTimeout(()=>{if(row.isConnected){row.remove();updateTasksEmpty($('tasksActive').children.length);}},500);},COMPLETED_LINGER);}
function updateTasksEmpty(activeCount){$('tasksEmpty').classList.toggle('hidden',activeCount>0||$('tasksDone').children.length>0);}
async function cancelTask(id){try{const r=await api('POST','/api/tasks/'+encodeURIComponent(id)+'/cancel');toast('Cancelling',`${r.cancelled} task${r.cancelled===1?'':'s'} signalled`);}catch(e){toast('Cannot cancel',e.message,false);}}
