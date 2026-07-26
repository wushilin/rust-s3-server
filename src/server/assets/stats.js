// ── runtime stats ──────────────────────────────────────────────────────────
// Live system & process health charts, drawn with the vendored uPlot library.
// Admin-only tab; the /api/stats endpoint enforces that server-side. The server
// downsamples to <=120 points and clamps the window to where data exists, so
// this only ever draws a small, dense set — no empty expanses.
let statsRange='24h', statsCharts=[], statsTimer=null, statsInited=false;

// Categorical palette (light surface), validated colorblind-safe for adjacent
// line pairs. Assigned in fixed order, never cycled.
const STATS_COLORS=['#2a78d6','#eb6834','#1baf7a','#eda100'];
const STATS_AXIS='#647184', STATS_GRID='rgba(100,113,132,.13)';

// Value formatters by unit. Reuses fmtSize from core.js for bytes / byte-rates.
function statsFmt(kind){
  if(kind==='pct')  return v=>v==null?'':(v<10?v.toFixed(1):v.toFixed(0))+'%';
  if(kind==='bps')  return v=>v==null?'':fmtSize(v)+'/s';
  if(kind==='bytes')return v=>v==null?'':fmtSize(v);
  return v=>v==null?'':(v>=10?Math.round(v):v.toFixed(1)); // qps / count
}
function lastVal(arr){ for(let i=arr.length-1;i>=0;i--) if(arr[i]!=null) return arr[i]; return null; }

// Builds one uPlot chart into `el`. `defs` maps each line to a column index in
// the server's columnar response (data[idx]). We render our OWN legend (a fixed
// grid) instead of uPlot's table, so nothing shifts on hover and label/value
// stay aligned.
function statsChart(el,defs,kind){
  const fmt=statsFmt(kind);
  const single=defs.length===1;

  // Custom legend: one fixed-height row per series, updated live from the hook.
  const legend=document.createElement('div');
  legend.className='slegend';
  const valEls=defs.map((d,i)=>{
    const row=document.createElement('div');
    row.className='slegend-item';
    row.innerHTML='<span class="slegend-dot" style="background:'+STATS_COLORS[i%STATS_COLORS.length]
      +'"></span><span class="slegend-label"></span><span class="slegend-val"></span>';
    row.querySelector('.slegend-label').textContent=d.label;
    legend.appendChild(row);
    return row.querySelector('.slegend-val');
  });

  const setLegend=(idx)=>{
    for(let i=0;i<defs.length;i++){
      const col=u.data[i+1]; // data[0] is the x axis; series i → column i+1
      const v=idx!=null?col[idx]:lastVal(col);
      valEls[i].textContent=fmt(v);
    }
  };

  const opts={
    width: el.clientWidth||520, height:152,
    padding:[12,14,2,6],
    legend:{show:false},
    cursor:{points:{size:6,width:2},focus:{prox:28}},
    scales:{x:{time:true}, y:{range:(u,dmin,dmax)=>[0,(dmax==null||dmax<=0)?1:dmax*1.2]}},
    series:[{},...defs.map((d,i)=>({
      stroke:STATS_COLORS[i%STATS_COLORS.length],
      width:2, points:{show:false}, spanGaps:false,
      ...(single?{fill:'rgba(42,120,214,.09)'}:{}),
    }))],
    axes:[
      {stroke:STATS_AXIS, grid:{stroke:STATS_GRID,width:1}, ticks:{stroke:STATS_GRID,width:1,size:4},
       font:'11px system-ui,sans-serif', size:28},
      {stroke:STATS_AXIS, grid:{stroke:STATS_GRID,width:1}, ticks:{show:false},
       font:'11px system-ui,sans-serif', size:54, values:(u,vals)=>vals.map(fmt)},
    ],
    hooks:{setCursor:[u=>setLegend(u.cursor.idx)]},
  };
  const u=new uPlot(opts,[[],...defs.map(()=>[])],el);
  el.appendChild(legend);
  return {u,defs,el,kind,setLegend};
}

function initStats(){
  if(!statsInited){
    statsInited=true;
    const rangeSel=$('statsRange'); if(rangeSel)rangeSel.value=statsRange;
    const B='/s';
    statsCharts=[
      {c:statsChart($('chartCpu'), [{label:'System',idx:1},{label:'Process',idx:2}], 'pct'),
       now:'nowCpu', head:v=>statsFmt('pct')(v[0])},
      {c:statsChart($('chartMem'), [{label:'Used',idx:3},{label:'Process RSS',idx:5}], 'bytes'),
       now:'nowMem', head:v=>v[0]==null?'—':fmtSize(v[0])},
      {c:statsChart($('chartDisk'),[{label:'Proc read',idx:6},{label:'Proc write',idx:7},{label:'Sys read',idx:8},{label:'Sys write',idx:9}], 'bps'),
       now:'nowDisk', head:v=>fmtSize((v[2]||0)+(v[3]||0))+B},
      {c:statsChart($('chartNet'), [{label:'In',idx:10},{label:'Out',idx:11}], 'bps'),
       now:'nowNet', head:v=>fmtSize((v[0]||0)+(v[1]||0))+B},
      {c:statsChart($('chartQps'), [{label:'Requests',idx:12}], 'qps'),
       now:'nowQps', head:v=>statsFmt('qps')(v[0]||0)+B},
    ];
    window.addEventListener('resize',statsResize);
  }
  loadStats();
  clearInterval(statsTimer);
  statsTimer=setInterval(()=>{
    if($('tab_stats').classList.contains('hidden')){clearInterval(statsTimer);statsTimer=null;return;}
    loadStats();
  },5000);
}

function statsResize(){
  if(!statsInited||$('tab_stats').classList.contains('hidden'))return;
  statsCharts.forEach(c=>c.c.u.setSize({width:c.c.el.clientWidth||520,height:152}));
}

function statsSetRange(v){statsRange=v;loadStats();}

async function loadStats(){
  try{
    const r=await api('GET','/api/stats/series?range='+enc(statsRange)+'&points=120');
    const disabled=$('statsDisabled'), empty=$('statsEmpty'), grid=$('statsGrid');
    if(r.enabled===false){ disabled.classList.remove('hidden'); empty.classList.add('hidden'); grid.classList.add('hidden'); return; }
    disabled.classList.add('hidden');
    const d=r.data; // columnar: d[0]=time (unix seconds), d[idx]=series
    // Empty when every metric column is null (no samples yet / none in window).
    const hasData=d.slice(1).some(col=>col.some(v=>v!=null));
    empty.classList.toggle('hidden',hasData);
    grid.classList.toggle('hidden',!hasData);
    if(!hasData)return;
    statsCharts.forEach(c=>{
      c.c.u.setData([d[0],...c.c.defs.map(def=>d[def.idx])]);
      c.c.setLegend(null); // refresh legend to latest (cursor hook only fires on hover)
      const latest=c.c.defs.map(def=>lastVal(d[def.idx]));
      const el=$(c.now); if(el)el.textContent=c.head(latest);
    });
  }catch(e){ /* transient poll error — the next tick retries */ }
}
