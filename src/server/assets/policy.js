// ── policy editor ─────────────────────────────────────────────────────────
// The visual rule-builder / JSON policy editor shared by users and groups.
let policyTarget=null,policyMode='rules',policyRules=[];
async function compilePolicyRules(){const data=await api('POST','/api/policies/compile',{rules:policyRules});return data.policy;}
async function decompilePolicy(policy){const data=await api('POST','/api/policies/decompile',policy);return data.rules||[];}
async function openPolicy(kind,name,policy){
  policyTarget={kind,name};$('pd_target').textContent=name;$('policyBuckets').innerHTML=buckets.map(b=>`<option value="${esc(b.name)}"></option>`).join('');setInlineError('policyError');
  // No policy attached opens EMPTY — the starter rule is on demand (the "Add
  // rule" button / presets), never auto-filled, so the editor never implies a
  // policy exists when none does.
  if(policy){try{policyRules=await decompilePolicy(policy);renderPolicyRules();$('pd_text').value=JSON.stringify(policy,null,2);setPolicyMode('rules',false);}catch{$('pd_text').value=JSON.stringify(policy,null,2);setPolicyMode('json',false);}}else{policyRules=[];renderPolicyRules();$('pd_text').value=JSON.stringify({Version:'2012-10-17',Statement:[]},null,2);setPolicyMode('rules',false);}
  $('policyDlg').showModal();
}
async function setPolicyMode(mode,sync=true){
  try{
    if(sync&&mode==='json'&&policyMode==='rules')$('pd_text').value=JSON.stringify(await compilePolicyRules(),null,2);
    if(sync&&mode==='rules'&&policyMode==='json'){const parsed=await decompilePolicy(JSON.parse($('pd_text').value));policyRules=parsed.length?parsed:[{effect:'Allow',access:'read',bucket:bucket||'',prefix:''}];renderPolicyRules();}
    policyMode=mode;$('rulesEditor').classList.toggle('hidden',mode!=='rules');$('jsonEditor').classList.toggle('hidden',mode!=='json');$('rulesTab').classList.toggle('active',mode==='rules');$('jsonTab').classList.toggle('active',mode==='json');setInlineError('policyError');
  }catch(e){setInlineError('policyError',e.message);}
}
function renderPolicyRules(){
  $('ruleList').innerHTML=policyRules.map((rule,i)=>`<div class="policy-rule"><div class="field"><label>Effect</label><select class="input" onchange="updatePolicyRule(${i},'effect',this.value)"><option ${rule.effect==='Allow'?'selected':''}>Allow</option><option ${rule.effect==='Deny'?'selected':''}>Deny</option></select></div><div class="field"><label>Access</label><select class="input" onchange="updatePolicyRule(${i},'access',this.value)"><option value="read" ${rule.access==='read'?'selected':''}>Read</option><option value="write" ${rule.access==='write'?'selected':''}>Write + delete</option><option value="readwrite" ${rule.access==='readwrite'?'selected':''}>Read + write + delete</option></select></div><div class="field"><label>Bucket</label><input class="input" list="policyBuckets" value="${esc(rule.bucket)}" placeholder="bucket or *" oninput="updatePolicyRule(${i},'bucket',this.value)"></div><div class="field"><label>Prefix <span style="text-transform:none">(optional)</span></label><input class="input" value="${esc(rule.prefix)}" placeholder="e.g. incoming/" oninput="updatePolicyRule(${i},'prefix',this.value)"></div><button class="row-action danger" title="Remove rule" onclick="removePolicyRule(${i})">${icons.trash}</button></div>`).join('');
  hydrateIcons($('ruleList'));
}
function updatePolicyRule(index,key,value){policyRules[index][key]=value;}
function addPolicyRule(){policyRules.push({effect:'Allow',access:'read',bucket:bucket||'',prefix:''});renderPolicyRules();}
function removePolicyRule(index){policyRules.splice(index,1);renderPolicyRules();}
function editPolicy(i){const u=users[i];openPolicy('user',u.username,u.policy);}
function editGroupPolicy(i){const g=groups[i];if(g.is_system)return;openPolicy('group',g.name,g.policy);}
function preset(kind){const doc={Version:'2012-10-17',Statement:[]},needsBucket=kind.endsWith('-bucket');let b=bucket;if(needsBucket&&!b){setInlineError('policyError','Select a bucket in the Object Browser first.');return;}if(kind==='full-all')doc.Statement.push({Sid:'FullAccessAllBuckets',Effect:'Allow',Action:'s3:*',Resource:'arn:aws:s3:::*'});if(kind==='ro-all')doc.Statement.push({Sid:'ReadOnlyAllBuckets',Effect:'Allow',Action:['s3:Get*','s3:List*'],Resource:'arn:aws:s3:::*'});if(kind==='full-bucket'){doc.Statement.push({Sid:'ListBucketNames',Effect:'Allow',Action:'s3:ListAllMyBuckets',Resource:'arn:aws:s3:::*'},{Sid:'FullAccessOneBucket',Effect:'Allow',Action:'s3:*',Resource:[`arn:aws:s3:::${b}`,`arn:aws:s3:::${b}/*`]});}if(kind==='ro-bucket'){doc.Statement.push({Sid:'ListBucketNames',Effect:'Allow',Action:'s3:ListAllMyBuckets',Resource:'arn:aws:s3:::*'},{Sid:'ReadOnlyOneBucket',Effect:'Allow',Action:['s3:GetObject','s3:ListBucket','s3:ListBucketVersions'],Resource:[`arn:aws:s3:::${b}`,`arn:aws:s3:::${b}/*`]});}if(kind==='wo-bucket')doc.Statement.push({Sid:'WriteOnlyIngest',Effect:'Allow',Action:['s3:PutObject','s3:AbortMultipartUpload','s3:ListMultipartUploadParts'],Resource:`arn:aws:s3:::${b}/*`});$('pd_text').value=JSON.stringify(doc,null,2);setInlineError('policyError');}
async function savePolicy(detach){try{const base=policyTarget.kind==='user'?'/api/users/':'/api/groups/';const target=base+encodeURIComponent(policyTarget.name)+'/policy';if(detach===null)await api('PUT',target,null);else if(policyMode==='rules')await api('PUT',target+'/rules',{rules:policyRules});else await api('PUT',target,JSON.parse($('pd_text').value));$('policyDlg').close();toast(detach===null?'Policy detached':'Policy saved',policyTarget.name);policyTarget.kind==='user'?loadUsers():loadGroups();}catch(e){setInlineError('policyError',e.message);}}
