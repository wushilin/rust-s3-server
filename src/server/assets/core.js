// ── core ──────────────────────────────────────────────────────────────────
// Shared foundation loaded first by every console page: the icon set, small
// DOM/format helpers, the JSON API client, toasts, the confirm dialog, and the
// session / app-shell logic. Everything here is plain global scope so the other
// modules (tasks.js, objects.js, iam.js) and inline handlers can call it
// directly — no bundler, no module system, no build step.
const icons = {
  layers:'<svg viewBox="0 0 24 24" class="icon"><path d="m12 2 9 5-9 5-9-5 9-5Z"/><path d="m3 12 9 5 9-5M3 17l9 5 9-5"/></svg>',
  folder:'<svg viewBox="0 0 24 24" class="icon"><path d="M3 6.5A2.5 2.5 0 0 1 5.5 4H9l2 2h7.5A2.5 2.5 0 0 1 21 8.5v8A2.5 2.5 0 0 1 18.5 19h-13A2.5 2.5 0 0 1 3 16.5v-10Z"/></svg>',
  users:'<svg viewBox="0 0 24 24" class="icon"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87M16 3.13a4 4 0 0 1 0 7.75"/></svg>',
  key:'<svg viewBox="0 0 24 24" class="icon"><circle cx="7.5" cy="15.5" r="4.5"/><path d="m10.7 12.3 8-8M15 8l2 2M18 5l2 2"/></svg>',
  database:'<svg viewBox="0 0 24 24" class="icon"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v7c0 1.7 4 3 9 3s9-1.3 9-3V5M3 12v7c0 1.7 4 3 9 3s9-1.3 9-3v-7"/></svg>',
  shield:'<svg viewBox="0 0 24 24" class="icon"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10Z"/><path d="m9 12 2 2 4-4"/></svg>',
  zap:'<svg viewBox="0 0 24 24" class="icon"><path d="M13 2 3 14h9l-1 8 10-12h-9l1-8Z"/></svg>',
  plus:'<svg viewBox="0 0 24 24" class="icon"><path d="M12 5v14M5 12h14"/></svg>',
  upload:'<svg viewBox="0 0 24 24" class="icon"><path d="M12 16V3m0 0L7 8m5-5 5 5M5 21h14"/></svg>',
  download:'<svg viewBox="0 0 24 24" class="icon"><path d="M12 3v13m0 0 5-5m-5 5-5-5M5 21h14"/></svg>',
  search:'<svg viewBox="0 0 24 24" class="icon"><circle cx="11" cy="11" r="7"/><path d="m20 20-4-4"/></svg>',
  refresh:'<svg viewBox="0 0 24 24" class="icon"><path d="M20 6v5h-5M4 18v-5h5"/><path d="M18.5 9A7 7 0 0 0 6 6.5L4 11m16 2-2 4.5A7 7 0 0 1 5.5 15"/></svg>',
  file:'<svg viewBox="0 0 24 24" class="icon"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8Z"/><path d="M14 2v6h6"/></svg>',
  share:'<svg viewBox="0 0 24 24" class="icon"><circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><path d="m8.6 10.5 6.8-4M8.6 13.5l6.8 4"/></svg>',
  trash:'<svg viewBox="0 0 24 24" class="icon"><path d="M3 6h18M8 6V4h8v2m3 0-1 15H6L5 6M10 11v6M14 11v6"/></svg>',
  info:'<svg viewBox="0 0 24 24" class="icon"><circle cx="12" cy="12" r="9"/><path d="M12 11v5M12 8h.01"/></svg>',
  eye:'<svg viewBox="0 0 24 24" class="icon"><path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z"/><circle cx="12" cy="12" r="3"/></svg>',
  menu:'<svg viewBox="0 0 24 24" class="icon"><path d="M4 6h16M4 12h16M4 18h16"/></svg>',
  logout:'<svg viewBox="0 0 24 24" class="icon"><path d="M10 17l5-5-5-5M15 12H3M15 4h4a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2h-4"/></svg>',
  'chevron-down':'<svg viewBox="0 0 24 24" class="icon sm"><path d="m7 10 5 5 5-5"/></svg>',
  'chevron-right':'<svg viewBox="0 0 24 24" class="icon sm"><path d="m9 18 6-6-6-6"/></svg>',
  x:'<svg viewBox="0 0 24 24" class="icon"><path d="m6 6 12 12M18 6 6 18"/></svg>',
  copy:'<svg viewBox="0 0 24 24" class="icon"><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>',
  link:'<svg viewBox="0 0 24 24" class="icon"><path d="M10 13a5 5 0 0 0 7.5.5l3-3a5 5 0 0 0-7-7l-1.7 1.7M14 11a5 5 0 0 0-7.5-.5l-3 3a5 5 0 0 0 7 7l1.7-1.7"/></svg>',
  'user-plus':'<svg viewBox="0 0 24 24" class="icon"><path d="M15 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="8" cy="7" r="4"/><path d="M19 8v6M16 11h6"/></svg>',
  // Task-row glyphs: a compact icon stands in for the (long) operation name.
  list:'<svg viewBox="0 0 24 24" class="icon"><path d="M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01"/></svg>',
  activity:'<svg viewBox="0 0 24 24" class="icon"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>',
  check:'<svg viewBox="0 0 24 24" class="icon"><path d="M20 6 9 17l-5-5"/></svg>',
  external:'<svg viewBox="0 0 24 24" class="icon"><path d="M15 3h6v6M10 14 21 3M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/></svg>'
};
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const enc = value => encodeURIComponent(value).replace(/'/g,'%27');
const fmtSize = n => n < 1024 ? n+' B' : n < 1048576 ? (n/1024).toFixed(1)+' KiB' : n < 1073741824 ? (n/1048576).toFixed(1)+' MiB' : (n/1073741824).toFixed(2)+' GiB';
const fmtTime = ms => ms ? new Intl.DateTimeFormat(undefined,{dateStyle:'medium',timeStyle:'short'}).format(new Date(ms)) : '—';
function fmtDur(ms){const s=Math.max(0,Math.floor(ms/1000));if(s<60)return s+'s';const m=Math.floor(s/60),r=s%60;if(m<60)return r?`${m}m${r}s`:`${m}m`;const h=Math.floor(m/60),mr=m%60;return mr?`${h}h${mr}m`:`${h}h`;}
function hydrateIcons(root=document) { root.querySelectorAll('[data-icon]').forEach(el=>{ const name=el.dataset.icon; if(icons[name]) el.innerHTML=icons[name]; }); }
hydrateIcons();

let me=null, pingTimer=null;

async function api(method,url,body) {
  const opts={method,headers:{}};
  if(body!==undefined){opts.headers['content-type']='application/json';opts.body=JSON.stringify(body);}
  const resp=await fetch(url,opts); const data=await resp.json().catch(()=>({}));
  if(!resp.ok){if(resp.status===401&&me) location.reload();throw new Error(data.error||resp.statusText||'Request failed');}
  return data;
}
function setInlineError(id,message=''){const el=$(id);el.textContent=message;el.classList.toggle('show',!!message);}
function toast(title,message='',ok=true){const el=document.createElement('div');el.className='toast '+(ok?'ok':'err');el.innerHTML=`<span class="toast-symbol">${icons[ok?'shield':'info']}</span><div><strong>${esc(title)}</strong>${message?`<span>${esc(message)}</span>`:''}</div>`;$('toasts').append(el);setTimeout(()=>el.remove(),4200);}
function setBusy(button,busy,label){if(!button)return;if(busy){button.dataset.label=button.innerHTML;button.disabled=true;button.textContent=label||'Working…';}else{button.disabled=false;if(button.dataset.label)button.innerHTML=button.dataset.label;hydrateIcons(button);}}
async function copyText(value){
  // Async Clipboard API only exists in secure contexts (HTTPS or localhost);
  // this console is commonly served over plain HTTP on a LAN address.
  if(navigator.clipboard&&window.isSecureContext){try{await navigator.clipboard.writeText(value);return true;}catch{}}
  // Fallback for non-secure contexts. The temporary field MUST be attached to
  // the open <dialog> (top layer) — a node under document.body cannot be
  // selected while a modal dialog owns the top layer, so the copy would fail.
  try{
    const host=document.querySelector('dialog[open]')||document.body;
    const area=document.createElement('textarea');
    area.value=value;area.readOnly=true;
    area.style.position='fixed';area.style.top='0';area.style.left='0';area.style.opacity='0';
    host.append(area);area.focus();area.select();area.setSelectionRange(0,value.length);
    const ok=document.execCommand('copy');area.remove();return ok;
  }catch{return false;}
}
function showConfirm(title,subtitle,message,action,opts){opts=opts||{};$('confirmTitle').textContent=title;$('confirmSubtitle').textContent=subtitle;$('confirmMessage').textContent=message;const btn=$('confirmAction');const label=opts.confirmLabel||'Delete';btn.textContent=label;btn.classList.toggle('danger',opts.danger!==false);btn.classList.toggle('primary',opts.danger===false);btn.onclick=async()=>{setBusy(btn,true,opts.busyLabel||'Deleting…');try{await action();$('confirmDlg').close();}catch(e){toast('Action failed',e.message,false);}finally{setBusy(btn,false);btn.textContent=label;}};$('confirmDlg').showModal();}

// ── session / app shell ──
async function boot(){try{me=await api('GET','/api/me');onLoggedIn();}catch{$('loginView').classList.remove('hidden');}}
async function login(event){event.preventDefault();setInlineError('li_msg');const btn=$('loginBtn');setBusy(btn,true,'Signing in…');try{me=await api('POST','/api/login',{username:$('li_user').value.trim(),password:$('li_pass').value});onLoggedIn();}catch(e){setInlineError('li_msg',e.message);setBusy(btn,false);}}
async function logout(){try{await api('POST','/api/logout');}finally{location.reload();}}
function togglePassword(){const input=$('li_pass');input.type=input.type==='password'?'text':'password';}
function onLoggedIn(){
  $('loginView').classList.add('hidden');$('appView').classList.remove('hidden');
  document.body.classList.toggle('sidebar-collapsed',localStorage.getItem('sidebarCollapsed')==='1');
  $('whoami').textContent=me.username;$('userRole').textContent=me.is_admin?'Administrator':'IAM user';$('avatar').textContent=(me.username[0]||'U').toUpperCase();document.querySelectorAll('[data-admin-only]').forEach(el=>el.classList.toggle('hidden',!me.is_admin));
  showTab('objects');loadBuckets();pingServer();pingTimer=setInterval(pingServer,5000);
  // Active-task panel: admins only (the backend enforces this too). Live over a
  // WebSocket; falls back to polling if the socket can't be established.
  if(me.is_admin){refreshTasks();connectTasksWs();}
}
function toggleSidebar(){const c=document.body.classList.toggle('sidebar-collapsed');localStorage.setItem('sidebarCollapsed',c?'1':'0');}
function toggleProfile(){$('profilePopover').classList.toggle('hidden');}
const pageMeta={objects:['Object Browser','Manage buckets and objects'],users:['IAM Users','Manage users and policies'],groups:['IAM Groups','Reuse policies and assign administrative access'],keys:['My Access Keys','Manage your application credentials'],backup:['Backup & Restore','Export and import the global IAM database']};
function showTab(tab){
  if((tab==='users'||tab==='groups'||tab==='backup')&&!me?.is_admin)return;
  document.querySelectorAll('.nav-item').forEach(b=>b.classList.toggle('active',b.dataset.tab===tab));
  ['objects','users','groups','keys','backup'].forEach(t=>$('tab_'+t).classList.toggle('hidden',t!==tab));
  $('pageTitle').textContent=pageMeta[tab][0];$('pageSubtitle').textContent=pageMeta[tab][1];closeDetails();
  if(tab==='users')loadUsers();if(tab==='groups')loadGroups();if(tab==='keys')loadMyKeys();if(tab==='backup')resetBackupPanel();
}
async function pingServer(){try{const resp=await fetch('/api/ping',{cache:'no-store'});const data=await resp.json().catch(()=>({}));if(!resp.ok){if(resp.status===401&&me)location.reload();throw new Error('ping failed');}$('serverState').classList.remove('offline');$('serverStateText').textContent='Server connected';$('serverVersion').textContent='RustS3 v'+data.version;}catch{$('serverState').classList.add('offline');$('serverStateText').textContent='Connection interrupted';}}
