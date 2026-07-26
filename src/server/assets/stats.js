// ── runtime stats ──────────────────────────────────────────────────────────
// Live system & process health charts, drawn with the vendored uPlot library.
// Admin-only tab; the /api/stats endpoint enforces that server-side. Data is
// downsampled server-side to ≤120 points, so this only ever draws a small set.
let statsRange='24h', statsCharts=[], statsTimer=null, statsInited=false;

// Series colours, in order.
const STATS_STROKE=['#38bdf8','#f472b6','#34d399','#fbbf24'];

// Value formatters by unit. Reuses fmtSize from core.js for bytes / byte-rates.
function statsFmt(kind){
  if(kind==='pct')  return v=>v==null?'':(v<10?v.toFixed(1):v.toFixed(0))+'%';
  if(kind==='bps')  return v=>v==null?'':fmtSize(v)+'/s';
  if(kind==='bytes')return v=>v==null?'':fmtSize(v);
  return v=>v==null?'':String(Math.round(v)); // qps / count
}

// Builds one uPlot chart into `el`. `defs` maps each line to a column index in
// the server's columnar response (data[idx]).
function statsChart(el,defs,kind){
  const fmt=statsFmt(kind);
  const opts={
    width: el.clientWidth||600, height:170,
    padding:[10,12,0,8],
    cursor:{y:false},
    scales:{x:{time:true}},
    series:[{},...defs.map((d,i)=>({
      label:d.label,
      stroke:STATS_STROKE[i%STATS_STROKE.length],
      width:1.5, spanGaps:false,
      value:(u,v)=>fmt(v),
    }))],
    axes:[
      {stroke:'#7c8aa5',grid:{stroke:'rgba(148,163,184,.12)'},ticks:{stroke:'rgba(148,163,184,.2)'}},
      {stroke:'#7c8aa5',size:64,grid:{stroke:'rgba(148,163,184,.12)'},ticks:{stroke:'rgba(148,163,184,.2)'},values:(u,vals)=>vals.map(fmt)},
    ],
  };
  const u=new uPlot(opts,[[],...defs.map(()=>[])],el);
  return {u,defs,el};
}

function initStats(){
  if(!statsInited){
    statsInited=true;
    const rangeSel=$('statsRange');
    if(rangeSel)rangeSel.value=statsRange;
    statsCharts=[
      statsChart($('chartCpu'), [{label:'System',idx:1},{label:'Process',idx:2}], 'pct'),
      statsChart($('chartMem'), [{label:'Used',idx:3},{label:'Process RSS',idx:5}], 'bytes'),
      statsChart($('chartDisk'),[{label:'Proc read',idx:6},{label:'Proc write',idx:7},{label:'Sys read',idx:8},{label:'Sys write',idx:9}], 'bps'),
      statsChart($('chartNet'), [{label:'In',idx:10},{label:'Out',idx:11}], 'bps'),
      statsChart($('chartQps'), [{label:'Requests',idx:12}], 'qps'),
    ];
    window.addEventListener('resize',statsResize);
  }
  loadStats();
  // Refresh every 5s while the tab is visible; the timer stops itself once the
  // user navigates away (there is no teardown hook in showTab).
  clearInterval(statsTimer);
  statsTimer=setInterval(()=>{
    if($('tab_stats').classList.contains('hidden')){clearInterval(statsTimer);statsTimer=null;return;}
    loadStats();
  },5000);
}

function statsResize(){
  if(!statsInited||$('tab_stats').classList.contains('hidden'))return;
  statsCharts.forEach(c=>c.u.setSize({width:c.el.clientWidth||600,height:170}));
}

function statsSetRange(v){statsRange=v;loadStats();}

async function loadStats(){
  try{
    const r=await api('GET','/api/stats/series?range='+enc(statsRange)+'&points=120');
    const note=$('statsDisabled');
    if(r.enabled===false){if(note)note.classList.remove('hidden');return;}
    if(note)note.classList.add('hidden');
    const d=r.data; // columnar: d[0]=time (unix seconds), d[idx]=series
    statsCharts.forEach(c=>c.u.setData([d[0],...c.defs.map(def=>d[def.idx])]));
  }catch(e){ /* transient poll error — the next tick retries */ }
}
