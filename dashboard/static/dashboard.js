async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error("HTTP " + res.status);
  return await res.json();
}

function buildGraphOption(graph) {
  const categories = [
    { name: 'txn' }, { name: 'customer' }, { name: 'card' }, { name: 'device' }, { name: 'email' }
  ];
  const catIndex = (cat) => {
    if (cat === 'txn') return 0;
    if (cat === 'customer') return 1;
    if (cat === 'card') return 2;
    if (cat === 'device') return 3;
    if (cat === 'email') return 4;
    return 0;
  };

  return {
    backgroundColor: 'transparent',
    tooltip: { formatter: (p) => p.dataType === 'node' ? p.data.name : '' },
    legend: [{ data: categories.map(c => c.name), textStyle: { color: '#e5e7eb' } }],
    series: [{
      type: 'graph',
      layout: 'force',
      roam: true,
      draggable: true,
      categories,
      force: { repulsion: 140, edgeLength: 55, gravity: 0.05 },
      label: { show: false },
      emphasis: { focus: 'adjacency' },
      lineStyle: { opacity: 0.35, width: 1 },
      data: graph.nodes.map(n => ({
        name: n.id,
        category: catIndex(n.cat),
        symbolSize: n.cat === 'txn' ? (10 + Math.min(24, (n.value ?? 0) * 28)) : 12,
        value: n.value
      })),
      links: graph.edges.map(e => ({ source: e.source, target: e.target }))
    }]
  };
}

async function loadFraudTxnList(from, to) {
  const itemsEl = document.getElementById('fraud-txn-items');
  itemsEl.innerHTML = '<div style="padding:10px 12px;color:#94a3b8;">Загрузка…</div>';

  const rows = await fetchJSON(`/api/fraud-txns?from=${from}&to=${to}&limit=500`);
  if (!rows.length) {
    itemsEl.innerHTML = '<div style="padding:10px 12px;color:#94a3b8;">Нет фродовых транзакций в выбранном диапазоне</div>';
    return null;
  }

  itemsEl.innerHTML = rows.map((r, idx) => {
    const score = (r.fraud_score ?? 0).toFixed(3);
    const amt = (r.amount ?? 0).toFixed(2);
    return `
      <div class="txn-item" data-txn="${r.transaction_id}" data-idx="${idx}">
        <div>
          <div><b>${r.transaction_id}</b></div>
          <div class="sub">${r.event_date} · ${r.channel || 'UNKNOWN'} · ${amt}</div>
        </div>
        <div class="score">${score}</div>
      </div>
    `;
  }).join('');

  const nodes = itemsEl.querySelectorAll('.txn-item');
  nodes.forEach(n => {
    n.onclick = async () => {
      nodes.forEach(x => x.classList.remove('active'));
      n.classList.add('active');
      const txnId = n.getAttribute('data-txn');
      await renderTxnLinks(from, to, txnId);
    };
  });

  nodes[0].classList.add('active');
  return rows[0].transaction_id;
}

async function renderTxnLinks(from, to, txnId) {
  const el = document.getElementById('chart-txn-links');
  const chart = echarts.init(el);
  chart.showLoading('default', { text: 'Загрузка связей…', textColor: '#e5e7eb', maskColor: 'rgba(2,6,23,0.35)'});
  try {
    const graph = await fetchJSON(`/api/fraud-links-txn?from=${from}&to=${to}&txn_id=${txnId}&neighbors=600`);
    chart.hideLoading();
    chart.setOption(buildGraphOption(graph), true);
  } catch (e) {
    chart.hideLoading();
    chart.clear();
  }
}

async function renderLinks(from, to) {
  const graph = await fetchJSON(`/api/fraud-links?from=${from}&to=${to}&limit=250`);
  const el = document.getElementById('chart-links');
  const chart = echarts.init(el);

  const categories = [
    { name: 'txn' }, { name: 'customer' }, { name: 'card' }, { name: 'device' }, { name: 'email' }
  ];
  const catIndex = (cat) => {
    if (cat === 'txn') return 0;
    if (cat === 'customer') return 1;
    if (cat === 'card') return 2;
    if (cat === 'device') return 3;
    if (cat === 'email') return 4;
    return 0;
  };

  chart.setOption({
    backgroundColor: 'transparent',
    tooltip: { formatter: (p) => p.dataType === 'node' ? p.data.name : '' },
    legend: [{ data: categories.map(c => c.name), textStyle: { color: '#e5e7eb' } }],
    series: [{
      type: 'graph',
      layout: 'force',
      roam: true,
      draggable: true,
      categories,
      force: { repulsion: 120, edgeLength: 50, gravity: 0.05 },
      label: { show: false },
      emphasis: { focus: 'adjacency' },
      lineStyle: { opacity: 0.35, width: 1 },
      data: graph.nodes.map(n => ({
        name: n.id,
        category: catIndex(n.cat),
        symbolSize: n.cat === 'txn' ? (8 + Math.min(20, n.value * 25)) : 10,
        value: n.value
      })),
      links: graph.edges.map(e => ({ source: e.source, target: e.target }))
    }]
  });

  chart.off('click');
  chart.on('click', params => {
    if (params.dataType !== 'node') return;
    chart.dispatchAction({ type: 'focusNodeAdjacency', seriesIndex: 0, dataIndex: params.dataIndex });
  });
}

async function renderFraud3D(from, to) {
  const points = await fetchJSON(`/api/fraud-cloud-3d?from=${from}&to=${to}`);
  const chart = echarts.init(document.getElementById('chart-fraud-3d'));

  if (points.length === 0) return chart.clear();

  const data = points.map(p => [p.x, p.y, p.z, p.label, p.score]);

  chart.setOption({
    backgroundColor: 'transparent',
    tooltip: {
      formatter: (p) => {
        const [x,y,z,label,score] = p.data;
        return `
          log(amount): ${x.toFixed(2)}<br>
          secs_since_prev_txn: ${y.toFixed(0)}<br>
          cust_txn_cnt_7d: ${z}<br>
          fraud_score: ${score.toFixed(3)}<br>
          fraud: ${label === 1 ? 'Да' : 'Нет'}
        `;
      }
    },
    visualMap: [{
      dimension: 3,
      categories: [0,1],
      inRange: { color: ['#4ade80', '#f97373'] }
    }],
    grid3D: {
      viewControl: { autoRotate: false, autoRotateSpeed: 8, rotateSensitivity: 1.2, zoomSensitivity: 1.2 },
      boxWidth: 200,
      boxDepth: 180,
      boxHeight: 160
    },
    xAxis3D: { name: "log(amount)" },
    yAxis3D: { name: "log(secs_prev_txn)" },
    zAxis3D: { name: "lgo(txn_cnt_7d)" },
    series: [{
      type: 'scatter3D',
      data,
      symbolSize: p => 4 + p[4] * 10,
      itemStyle: { opacity: 0.9 }
    }]
  });
}

async function renderFraud3Dc(from, to) {
  const points = await fetchJSON(`/api/fraud-cloud-3d-cluster?from=${from}&to=${to}`);
  const chart = echarts.init(document.getElementById('chart-fraud-3d-cluster'));

  if (points.length === 0) return chart.clear();

  const data = points.map(p => [p.x, p.y, p.z, p.cluster, p.score, p.label]);

  chart.setOption({
    backgroundColor: 'transparent',
    tooltip: {
      formatter: (p) => {
        const [x,y,z,cluster,score,label] = p.data;
        return `
          log(amount): ${x.toFixed(2)}<br>
          log_secs_prev: ${y.toFixed(2)}<br>
          log_txn_cnt_7d: ${z.toFixed(2)}<br>
          fraud_score: ${score.toFixed(3)}<br>
          fraud_label: ${label}<br>
          cluster: ${cluster}
        `;
      }
    },
    visualMap: [{
      dimension: 3,
      min: 0,
      max: 6,
      calculable: true,
      inRange: { color: ['#3b82f6', '#10b981', '#f97316', '#ef4444', '#a855f7', '#14b8a6', '#eab308'] }
    }],
    grid3D: {
      viewControl: { autoRotate: false, rotateSensitivity: 1.2, zoomSensitivity: 1.2 }
    },
    xAxis3D: { name: "log(amount)" },
    yAxis3D: { name: "log(secs_prev)" },
    zAxis3D: { name: "log(txn_cnt_7d)" },
    series: [{
      type: 'scatter3D',
      data,
      symbolSize: p => 5 + p[4] * 8,
      itemStyle: { opacity: 0.9 }
    }]
  });
}

async function initDashboard(from, to) {
  const summary = await fetchJSON(`/api/summary?from=${from}&to=${to}`);
  const total = summary.total_tx;
  const flagged = summary.flagged_tx;
  const rate = total ? (flagged * 100 / total).toFixed(2) + '%' : '0%';

  document.getElementById('kpi-total').textContent = total;
  document.getElementById('kpi-flagged').textContent = flagged;
  document.getElementById('kpi-rate').textContent = rate;
  document.getElementById('kpi-avg').textContent = summary.avg_score.toFixed(3);

  const tsData = await fetchJSON(`/api/timeseries?from=${from}&to=${to}`);
  const tsChart = echarts.init(document.getElementById('chart-timeseries'));
  tsChart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    legend: { data: ['Всего', 'Флагнуто'], textStyle: { color: '#e5e7eb' } },
    grid: { left: 40, right: 20, top: 40, bottom: 40 },
    xAxis: {
      type: 'category',
      data: tsData.map(d => d.date),
      axisLine: { lineStyle: { color: '#64748b' } }
    },
    yAxis: {
      type: 'value',
      axisLine: { lineStyle: { color: '#64748b' } },
      splitLine: { lineStyle: { color: '#1f2937' } }
    },
    series: [
      { name: 'Всего', type: 'bar', data: tsData.map(d => d.total_tx) },
      { name: 'Флагнуто', type: 'line', data: tsData.map(d => d.flagged_tx), smooth: true }
    ]
  });

  const hist = await fetchJSON(`/api/score-histogram?from=${from}&to=${to}`);
  const histChart = echarts.init(document.getElementById('chart-score-hist'));
  histChart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    grid: { left: 40, right: 20, top: 40, bottom: 40 },
    xAxis: { type: 'category', data: hist.map(d => d.bin), axisLine: { lineStyle: { color: '#64748b' } } },
    yAxis: { type: 'value', axisLine: { lineStyle: { color: '#64748b' } }, splitLine: { lineStyle: { color: '#1f2937' } } },
    series: [{ type: 'bar', data: hist.map(d => d.cnt) }]
  });

  const byChannel = await fetchJSON(`/api/by-channel?from=${from}&to=${to}`);
  const chanChart = echarts.init(document.getElementById('chart-by-channel'));
  chanChart.setOption({
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    legend: { data: ['Всего', 'Флагнуто'], textStyle: { color: '#e5e7eb' } },
    grid: { left: 40, right: 20, top: 40, bottom: 40 },
    xAxis: { type: 'category', data: byChannel.map(d => d.channel), axisLine: { lineStyle: { color: '#64748b' } } },
    yAxis: { type: 'value', axisLine: { lineStyle: { color: '#64748b' } }, splitLine: { lineStyle: { color: '#1f2937' } } },
    series: [
      { name: 'Всего', type: 'bar', data: byChannel.map(d => d.total_tx) },
      { name: 'Флагнуто', type: 'bar', data: byChannel.map(d => d.flagged_tx) }
    ]
  });

  const top = await fetchJSON(`/api/top-transactions?from=${from}&to=${to}`);
  document.getElementById('top-body').innerHTML = top.map(row => `
    <tr>
      <td>${row.event_date}</td>
      <td>${row.transaction_id}</td>
      <td>${row.customer_id}</td>
      <td>${(row.amount ?? 0).toFixed(2)}</td>
      <td>${row.country || ''}</td>
      <td>${row.channel || ''}</td>
      <td>${row.fraud_score.toFixed(3)}</td>
    </tr>
  `).join('');

  const firstTxn = await loadFraudTxnList(from, to);
  document.getElementById('refresh-fraud-list').onclick = async () => {
    const first = await loadFraudTxnList(from, to);
    if (first) await renderTxnLinks(from, to, first);
  };
  if (firstTxn) await renderTxnLinks(from, to, firstTxn);

  await renderLinks(from, to);
  await renderFraud3Dc(from, to);
  await renderFraud3D(from, to);
}

async function init() {
  const range = await fetchJSON('/api/date-range');
  const fromInput = document.getElementById('from-date');
  const toInput = document.getElementById('to-date');

  fromInput.value = range.from;
  toInput.value = range.to;

  document.getElementById('apply-range').onclick = () => {
    const from = fromInput.value;
    const to = toInput.value;
    initDashboard(from, to).catch(console.error);
  };

  await initDashboard(range.from, range.to);
}

document.addEventListener('DOMContentLoaded', () => {
  init().catch(console.error);
});