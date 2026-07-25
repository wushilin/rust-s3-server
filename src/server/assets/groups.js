// ── groups ────────────────────────────────────────────────────────────────
// IAM groups: listing, create/delete. Policies attach via the policy editor.
let groups=[];
async function loadGroups(){
  try{
    const data=await api('GET','/api/groups');groups=data.groups||[];
    $('groupCount').textContent=`${groups.length} group${groups.length===1?'':'s'}`;
    $('groupRows').innerHTML=groups.map((g,i)=>`<tr><td><div class="identity"><span class="identity-icon">${icons.shield}</span><div><strong>${esc(g.name)}</strong><small>${g.is_system?'Reserved system group':'Reusable policy group'}</small></div></div></td><td><span class="badge ${g.is_system?'amber':''}">${g.is_system?'System':'Managed'}</span></td><td>${g.is_system?'<span class="badge green">Allow all</span>':g.has_policy?'<span class="badge green">Attached</span>':'<span class="badge">Deny all</span>'}</td><td>${Number(g.members).toLocaleString()}</td><td class="actions">${g.is_system?'':`<button class="row-action" title="Edit policy" onclick="editGroupPolicy(${i})">${icons.shield}</button><button class="row-action danger" title="Delete group" onclick="confirmDeleteGroup(${i})">${icons.trash}</button>`}</td></tr>`).join('');
    const empty=!groups.length;$('groupRows').closest('table').classList.toggle('hidden',empty);$('groupEmpty').classList.toggle('hidden',!empty);
    if(empty)$('groupEmpty').innerHTML=`<div class="empty-icon">${icons.shield}</div><h3>No groups</h3><p>Create a reusable policy group for your IAM users.</p>`;
  }catch(e){toast('Could not load groups',e.message,false);}
}
function openGroupDialog(){$('ng_name').value='';setInlineError('groupError');$('groupDlg').showModal();setTimeout(()=>$('ng_name').focus(),50);}
async function createGroup(event){event.preventDefault();const name=$('ng_name').value.trim();try{await api('POST','/api/groups',{name});$('groupDlg').close();toast('Group created',name);await loadGroups();}catch(e){setInlineError('groupError',e.message);}}
function confirmDeleteGroup(i){const name=groups[i].name;showConfirm('Delete IAM group?',name,`This detaches “${name}” from all users and permanently deletes its policy.`,async()=>{await api('DELETE','/api/groups/'+encodeURIComponent(name));toast('Group deleted',name);await loadGroups();});}
