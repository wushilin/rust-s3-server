// ── main ──────────────────────────────────────────────────────────────────
// Console bootstrap, loaded last: wires the app-shell event listeners (once
// every function from the other modules is defined) and starts the session.
// The standalone /tasks page does NOT load this file — it has its own tiny
// bootstrap — so nothing here assumes console-only elements exist elsewhere.
document.addEventListener('click',e=>{if(!e.target.closest('.user-menu')&&!e.target.closest('#profilePopover'))$('profilePopover')?.classList.add('hidden');if(!e.target.closest('.task-wrap'))closeTasks();});
document.querySelectorAll('.nav-item').forEach(btn=>btn.onclick=()=>showTab(btn.dataset.tab));
document.addEventListener('keydown',e=>{if(e.key==='/'&&!['INPUT','TEXTAREA','SELECT'].includes(document.activeElement.tagName)&&!$('tab_objects').classList.contains('hidden')){e.preventDefault();$('objectSearch').focus();}if(e.key==='Escape'){closeDetails();$('profilePopover').classList.add('hidden');}});
boot();
