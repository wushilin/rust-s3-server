// ── my access keys ────────────────────────────────────────────────────────
// The signed-in user's own application credentials, the tag editor shared by
// every create/edit (also the admin view in users.js), and the one-time
// secret-reveal dialog.
const fmtLastUsed=k=>k.last_used_at_ms?`${fmtTime(k.last_used_at_ms)}<div class="muted" style="font-size:11px">from ${esc(k.last_used_from||'unknown')}</div>`:'<span class="muted">Never</span>';
// Read-only chips for a key's tags. A built-in key's one tag is server-assigned.
const tagChips=k=>`<div class="tag-list">${(k.tags||[]).map(t=>`<span class="tag ${k.builtin?'system':''}" title="${esc(t)}"><span class="tag-text">${esc(t)}</span></span>`).join('')}</div>`;
let myKeys=[];
async function loadMyKeys(){try{const data=await api('GET','/api/users/'+encodeURIComponent(me.username)+'/keys');const keys=myKeys=data.keys||[];$('keyCount').textContent=`${keys.length} key${keys.length===1?'':'s'}`;$('keyRows').innerHTML=keys.map((k,i)=>`<tr><td>${tagChips(k)}</td><td><code class="key">${esc(k.access_key)}</code> ${k.builtin?'<span class="badge amber">Built-in</span>':''}</td><td class="muted">${k.builtin?'Config managed':fmtTime(k.created_at_ms)}</td><td>${fmtLastUsed(k)}</td><td class="actions">${k.builtin?'':`<button class="row-action" title="Edit tags" onclick="editMyKeyTags(${i})">${icons.edit}</button><button class="row-action danger" title="Delete key" onclick="confirmDeleteKey(decodeURIComponent('${enc(k.access_key)}'))">${icons.trash}</button>`}</td></tr>`).join('');const empty=!keys.length;$('keyRows').closest('table').classList.toggle('hidden',empty);$('keyEmpty').classList.toggle('hidden',!empty);if(empty)$('keyEmpty').innerHTML=`<div class="empty-icon">${icons.key}</div><h3>No access keys</h3><p>Create a key to connect an S3-compatible application.</p><button class="btn primary" onclick="createMyKey()">Create access key</button>`;}catch(e){toast('Could not load access keys',e.message,false);}}

// ── tag editor ────────────────────────────────────────────────────────────
// One dialog for both "create" and "edit tags": `action(tags)` does the
// request; a failure stays in the dialog so the tags can be corrected.
// A comma or semicolon (or Enter) turns the typed text into a chip; Backspace
// in the empty field removes the last chip. The rules mirror the server's:
// letters, digits and spaces only, 256 characters, 100 tags, no duplicates.
const TAG_MAX_CHARS=256,TAG_MAX_COUNT=100;
const tagValid=t=>/^[A-Za-z0-9 ]+$/.test(t)&&t.length<=TAG_MAX_CHARS;
let keyTags=[],keyTagsAction=null;
function renderKeyTags(){$('kt_chips').innerHTML=keyTags.map((t,i)=>`<span class="tag ${tagValid(t)?'':'invalid'}" ${tagValid(t)?'':'title="Not a valid tag — remove it to save"'}>${esc(t)}<button type="button" class="tag-remove" title="Remove tag" aria-label="Remove tag ${esc(t)}" onclick="removeKeyTag(${i})">×</button></span>`).join('');$('kt_input').placeholder=keyTags.length?'':'e.g. backup job, nas01';}
function removeKeyTag(i){keyTags.splice(i,1);renderKeyTags();setInlineError('keyTagsError');$('kt_input').focus();}
// Adds one candidate tag. Returns an error message, or '' when it was added
// (or was blank, or a duplicate — neither worth complaining about).
function addKeyTag(raw){const tag=raw.trim().replace(/\s+/g,' ');if(!tag)return '';if(!/^[A-Za-z0-9 ]+$/.test(tag))return `“${tag}” is not a valid tag: use letters, digits, and spaces only.`;if(tag.length>TAG_MAX_CHARS)return `“${tag}” is longer than ${TAG_MAX_CHARS} characters.`;if(keyTags.some(t=>t.toLowerCase()===tag.toLowerCase()))return '';if(keyTags.length>=TAG_MAX_COUNT)return `A key can have at most ${TAG_MAX_COUNT} tags.`;keyTags.push(tag);return '';}
// Turns everything typed so far into chips. With `keepTail`, the text after
// the last separator is still being typed and stays in the field. A piece that
// is rejected goes back into the field to be fixed rather than vanishing —
// without its separator, so that fixing it does not re-commit per keystroke.
function commitKeyTagInput(keepTail){const input=$('kt_input');const parts=input.value.split(/[,;\n]/);const tail=keepTail?parts.pop():'';let error='';const rejected=[];for(const part of parts){const e=addKeyTag(part);if(e){error=error||e;rejected.push(part.trim());}}input.value=rejected.concat(tail.trim()?[tail.trim()]:[]).join(' ')||tail;renderKeyTags();setInlineError('keyTagsError',error);return !error;}
function initKeyTagInput(){const input=$('kt_input');if(input.dataset.ready)return;input.dataset.ready='1';input.addEventListener('input',()=>{if(/[,;\n]/.test(input.value))commitKeyTagInput(true);else setInlineError('keyTagsError');});input.addEventListener('keydown',e=>{if(e.key==='Enter'&&input.value.trim()){e.preventDefault();commitKeyTagInput(false);}else if(e.key==='Backspace'&&!input.value&&keyTags.length){keyTags.pop();renderKeyTags();}});input.addEventListener('blur',()=>commitKeyTagInput(false));}
function openKeyTagsDialog(title,subtitle,submitLabel,tags,action){initKeyTagInput();$('kt_title').textContent=title;$('kt_subtitle').textContent=subtitle;$('kt_submit').textContent=submitLabel;keyTags=[...tags];keyTagsAction=action;$('kt_input').value='';renderKeyTags();setInlineError('keyTagsError');$('keyTagsDlg').showModal();setTimeout(()=>$('kt_input').focus(),50);}
async function submitKeyTags(event){event.preventDefault();if(!commitKeyTagInput(false))return;if(!keyTags.length){setInlineError('keyTagsError','At least one tag is required.');return;}const bad=keyTags.find(t=>!tagValid(t));if(bad){setInlineError('keyTagsError',`Remove “${bad}”: tags may contain only letters, digits, and spaces.`);return;}try{await keyTagsAction([...keyTags]);$('keyTagsDlg').close();}catch(e){setInlineError('keyTagsError',e.message);}}

function createMyKey(){openKeyTagsDialog('Create access key','Tag the applications or devices that will use this key.','Create access key',[],async tags=>{const key=await api('POST','/api/users/'+encodeURIComponent(me.username)+'/keys',{tags});showSecret(key);loadMyKeys();});}
function editMyKeyTags(i){const k=myKeys[i];openKeyTagsDialog('Edit tags',k.access_key,'Save tags',k.tags||[],async tags=>{await api('PUT','/api/keys/'+encodeURIComponent(k.access_key),{tags});toast('Tags updated',tags.join(', '));await loadMyKeys();});}
function showSecret(key){$('sd_text').textContent=`tags: ${(key.tags||[]).join(', ')}\naccess_key: ${key.access_key}\nsecret_key: ${key.secret_key}`;$('secretDlg').showModal();}
async function copySecret(){const ok=await copyText($('sd_text').textContent);toast(ok?'Credentials copied':'Copy failed',ok?'':'Select the text and press Ctrl+C',ok);}
function confirmDeleteKey(ak){const k=myKeys.find(k=>k.access_key===ak);showConfirm('Delete access key?',k?`${(k.tags||[]).join(', ')} · ${ak}`:ak,'Applications using this key will immediately lose access.',async()=>{await api('DELETE','/api/keys/'+encodeURIComponent(ak));toast('Access key deleted');await loadMyKeys();});}
