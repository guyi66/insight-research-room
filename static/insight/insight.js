// 状态管理
const state = {
    jobId: null,
    pollTimer: null,
    logs: {}, // { orchestrator: [], tech: [] ... }
    activeLogAgent: 'orchestrator'
};

// [新增] 多股票解析缓存
let lastResolvedQuery = '';
let lastResolvedSymbols = [];

const STORAGE_KEYS = {
    form: 'bettafish_insight_form',
    settings: 'bettafish_insight_settings'
};

const FORM_FIELDS = [
    { id: 'symbolInput' },
    { id: 'nameInput' },
    { id: 'newsDays' },
    { id: 'mindspiderDays' },
    { id: 'crawlerKeywords' },
    { id: 'useBocha', type: 'checkbox' },
    { id: 'includeRealtime', type: 'checkbox' },
    { id: 'crawlMindspider', type: 'checkbox' },
    { id: 'crawlGuba', type: 'checkbox' }
];

const SETTINGS_FIELDS = [
    { id: 'techBaseUrl' },
    { id: 'techApiKey' },
    { id: 'techModelName' },
    { id: 'fundBaseUrl' },
    { id: 'fundApiKey' },
    { id: 'fundModelName' },
    { id: 'newsBaseUrl' },
    { id: 'newsApiKey' },
    { id: 'newsModelName' },
    { id: 'bochaApiKey' },
    { id: 'dbHost' },
    { id: 'dbPort' },
    { id: 'dbUser' },
    { id: 'dbPassword' },
    { id: 'dbName' }
];

const CONFIG_FIELD_MAP = {
    techBaseUrl: 'INSIGHT_ENGINE_BASE_URL',
    techApiKey: 'INSIGHT_ENGINE_API_KEY',
    techModelName: 'INSIGHT_ENGINE_MODEL_NAME',
    fundBaseUrl: 'REPORT_ENGINE_BASE_URL',
    fundApiKey: 'REPORT_ENGINE_API_KEY',
    fundModelName: 'REPORT_ENGINE_MODEL_NAME',
    newsBaseUrl: 'QUERY_ENGINE_BASE_URL',
    newsApiKey: 'QUERY_ENGINE_API_KEY',
    newsModelName: 'QUERY_ENGINE_MODEL_NAME',
    bochaApiKey: 'BOCHA_WEB_SEARCH_API_KEY',
    dbHost: 'DB_HOST',
    dbPort: 'DB_PORT',
    dbUser: 'DB_USER',
    dbPassword: 'DB_PASSWORD',
    dbName: 'DB_NAME'
};

function extractSymbolFromText(text) {
    const match = String(text || '').match(/\b(\d{6})\b/);
    return match ? match[1] : '';
}

function extractSymbolFromUrl(url) {
    const raw = String(url || '');
    const dirMatch = raw.match(/\/reports\/([^/]+)\//);
    if (dirMatch) {
        const dir = dirMatch[1];
        if (/^\d{6}$/.test(dir)) return dir;
    }
    return extractSymbolFromText(raw);
}

function resolveExportSymbol() {
    const activeBtn = document.querySelector('#chartSymbolLinks a.active');
    let symbol = extractSymbolFromText(activeBtn ? activeBtn.textContent : '');
    if (!symbol) {
        const iframe = document.getElementById('reportFrame');
        symbol = extractSymbolFromUrl(iframe ? iframe.src : '');
    }
    if (!symbol) {
        const inputVal = document.getElementById('symbolInput')?.value;
        symbol = extractSymbolFromText(inputVal);
    }
    return symbol;
}

function getFilenameFromDisposition(header) {
    if (!header) return '';
    const utfMatch = header.match(/filename\*\s*=\s*UTF-8''([^;]+)/i);
    if (utfMatch) {
        return decodeURIComponent(utfMatch[1]);
    }
    const match = header.match(/filename\s*=\s*\"?([^\";]+)\"?/i);
    return match ? match[1] : '';
}

async function downloadFile(url, fallbackName) {
    const resp = await fetch(url);
    if (!resp.ok) {
        let message = `Download failed (${resp.status})`;
        try {
            const data = await resp.json();
            if (data && data.message) message = data.message;
        } catch (e) {
            // ignore
        }
        throw new Error(message);
    }
    const blob = await resp.blob();
    const filename = getFilenameFromDisposition(resp.headers.get('Content-Disposition')) || fallbackName || 'download';
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    setTimeout(() => {
        URL.revokeObjectURL(link.href);
        link.remove();
    }, 0);
}

function getCheckedValues(selector) {
    return Array.from(document.querySelectorAll(selector)).filter(c => c.checked).map(c => c.value);
}

function setCheckedValues(selector, values) {
    const allow = new Set(values || []);
    document.querySelectorAll(selector).forEach(c => {
        c.checked = allow.has(c.value);
    });
}

function readFieldValue(id, type) {
    const el = document.getElementById(id);
    if (!el) return '';
    if (type === 'checkbox') return !!el.checked;
    return el.value;
}

function writeFieldValue(id, value, type) {
    const el = document.getElementById(id);
    if (!el || value === undefined || value === null) return;
    if (type === 'checkbox') {
        el.checked = !!value;
    } else {
        el.value = value;
    }
}

function collectFields(list) {
    const data = {};
    list.forEach(field => {
        data[field.id] = readFieldValue(field.id, field.type);
    });
    return data;
}

function applyFields(list, data) {
    if (!data) return;
    list.forEach(field => {
        if (Object.prototype.hasOwnProperty.call(data, field.id)) {
            writeFieldValue(field.id, data[field.id], field.type);
        }
    });
}

function collectFormState() {
    const data = collectFields(FORM_FIELDS);
    data.reportModes = getCheckedValues('#reportModes input');
    data.mindspiderPlatforms = getCheckedValues('#mindspiderPlatforms input');
    return data;
}

function applyFormState(data) {
    applyFields(FORM_FIELDS, data);
    if (data && data.reportModes) setCheckedValues('#reportModes input', data.reportModes);
    if (data && data.mindspiderPlatforms) setCheckedValues('#mindspiderPlatforms input', data.mindspiderPlatforms);
}

function collectSettingsState() {
    return collectFields(SETTINGS_FIELDS);
}

function applySettingsState(data) {
    applyFields(SETTINGS_FIELDS, data);
}

function saveLocalState() {
    try {
        localStorage.setItem(STORAGE_KEYS.form, JSON.stringify(collectFormState()));
        localStorage.setItem(STORAGE_KEYS.settings, JSON.stringify(collectSettingsState()));
    } catch (e) {
        console.warn('Failed to save local state', e);
    }
}

function loadLocalState() {
    try {
        const formRaw = localStorage.getItem(STORAGE_KEYS.form);
        if (formRaw) applyFormState(JSON.parse(formRaw));
        const settingsRaw = localStorage.getItem(STORAGE_KEYS.settings);
        if (settingsRaw) applySettingsState(JSON.parse(settingsRaw));
    } catch (e) {
        console.warn('Failed to load local state', e);
    }
}

function applyServerConfig(cfg) {
    Object.keys(CONFIG_FIELD_MAP).forEach(fieldId => {
        const key = CONFIG_FIELD_MAP[fieldId];
        if (Object.prototype.hasOwnProperty.call(cfg, key)) {
            writeFieldValue(fieldId, cfg[key]);
        }
    });
}

function buildConfigPayload() {
    const payload = {};
    Object.entries(CONFIG_FIELD_MAP).forEach(([fieldId, key]) => {
        payload[key] = readFieldValue(fieldId);
    });
    return payload;
}

function showToast(message, isError) {
    const toast = document.getElementById('configToast');
    if (!toast) return;
    toast.textContent = message;
    toast.style.display = 'block';
    toast.style.color = isError ? '#b91c1c' : '#166534';
    setTimeout(() => {
        toast.textContent = '';
    }, 3000);
}

// [新增] 解析输入框里的多个股票代码
async function resolveSymbolsFromInput() {
    const raw = document.getElementById('symbolInput').value.trim();
    if (!raw) return [];

    if (lastResolvedQuery === raw && lastResolvedSymbols.length) {
        return lastResolvedSymbols;
    }

    const codeMatches = raw.match(/\b\d{6}\b/g) || [];
    const uniqueCodes = [...new Set(codeMatches)];
    const hasChinese = /[\u4e00-\u9fa5]/.test(raw);

    let resolved = [];

    // 如果包含中文，尝试请求后端解析（兼容旧逻辑），失败则回退到纯正则提取
    if (hasChinese || uniqueCodes.length === 0) {
        try {
            const resp = await fetch(`/api/symbols/resolve?query=${encodeURIComponent(raw)}`);
            if (resp.ok) {
                const data = await resp.json();
                if (data.success && Array.isArray(data.symbols)) resolved = data.symbols;
            }
        } catch (e) { console.warn('Symbol resolve failed', e); }
    }

    if (!resolved.length && uniqueCodes.length) {
        resolved = uniqueCodes.map(code => ({ symbol: code, name: '' }));
    }

    lastResolvedQuery = raw;
    lastResolvedSymbols = resolved;
    return resolved;
}

// [新增] 渲染股票切换按钮
function setSymbolButtons(container, items, activeSymbol, onSelect) {
    if (!container) return;
    container.innerHTML = '';

    if (!Array.isArray(items) || items.length === 0) {
        container.style.display = 'none';
        return;
    }

    container.style.display = 'block';
    const active = String(activeSymbol || '').trim();

    items.forEach(item => {
        const symbol = item.symbol || item;
        const name = item.name || '';
        const a = document.createElement('a');
        a.href = "#";
        a.textContent = name ? `${symbol} ${name}` : symbol;
        if (String(symbol) === active) a.classList.add('active');

        a.onclick = (e) => {
            e.preventDefault();
            container.querySelectorAll('a').forEach(el => el.classList.remove('active'));
            a.classList.add('active');
            onSelect(symbol);
        };
        container.appendChild(a);
    });
}

// 视图切换逻辑
document.querySelectorAll('.nav-item').forEach(nav => {
    nav.addEventListener('click', (e) => {
        e.preventDefault();
        document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
        document.querySelectorAll('.view').forEach(el => el.classList.remove('active'));
        nav.classList.add('active');
        document.getElementById(nav.dataset.view).classList.add('active');
    });
});

// UI 辅助函数
function setGlobalStatus(status) {
    const dot = document.getElementById('globalStatusDot');
    const text = document.getElementById('statusText');
    if (status === 'running') {
        dot.classList.add('running');
        text.textContent = '系统运行中...';
    } else {
        dot.classList.remove('running');
        text.textContent = '系统空闲';
    }
}

function appendLog(logEntries) {
    if (!Array.isArray(logEntries)) return;

    // 分发日志到各 Agent 桶
    logEntries.forEach(entry => {
        const agent = entry.agent || 'orchestrator';
        if (!state.logs[agent]) state.logs[agent] = [];
        state.logs[agent].push(`[${entry.ts}] ${entry.message}`);
    });

    renderActiveLog();
}

function renderActiveLog() {
    const box = document.getElementById('logBox');
    const entries = state.logs[state.activeLogAgent] || [];
    box.textContent = entries.join('\n');
    box.scrollTop = box.scrollHeight;
}

function selectAgentLog(agent) {
    state.activeLogAgent = agent;
    document.getElementById('currentLogLabel').textContent = `${agent.toUpperCase()} 日志`;
    document.querySelectorAll('.agent-card').forEach(c => c.classList.remove('active'));
    document.getElementById(`card-${agent}`)?.classList.add('active');
    renderActiveLog();
}

function updateAgentCards(agentsStatus) {
    if (!agentsStatus) return;
    Object.keys(agentsStatus).forEach(agent => {
        const card = document.getElementById(`card-${agent}`);
        if (card) {
            const statusDiv = card.querySelector('.status');
            const status = agentsStatus[agent];
            card.className = `agent-card ${status}`; // reset and set class
            if(state.activeLogAgent === agent) card.classList.add('active');
            statusDiv.textContent = status.toUpperCase();
        }
    });
}

// 核心功能：运行任务
async function runJob(action) {
    const symbol = document.getElementById('symbolInput').value.trim();
    if (!symbol && action === 'analysis') return alert('请输入股票代码');
    
    saveLocalState();

    // 收集复选框
    const getChecked = (selector) => Array.from(document.querySelectorAll(selector)).filter(c => c.checked).map(c => c.value);
    
    const dbOverrides = {
        DB_HOST: document.getElementById('dbHost').value.trim(),
        DB_PORT: document.getElementById('dbPort').value.trim(),
        DB_USER: document.getElementById('dbUser').value.trim(),
        DB_PASSWORD: document.getElementById('dbPassword').value,
        DB_NAME: document.getElementById('dbName').value.trim()
    };
    Object.keys(dbOverrides).forEach((key) => {
        if (dbOverrides[key] === '') delete dbOverrides[key];
    });

    // 构建请求 payload
    const payload = {
        action: action,
        symbol: symbol,
        stock_name: document.getElementById('nameInput').value.trim(),
        report_modes: action === 'analysis' ? getChecked('#reportModes input') : [],
        news_days: parseInt(document.getElementById('newsDays').value),
        use_bocha: document.getElementById('useBocha').checked,

        // [关键修正] 必须使用下划线 include_realtime，与后端对齐
        include_realtime: document.getElementById('includeRealtime').checked,

        crawl_mindspider: document.getElementById('crawlMindspider').checked,
        mindspider_platforms: getChecked('#mindspiderPlatforms input'),
        crawl_guba: document.getElementById('crawlGuba').checked,

        // Config Overrides: 将实时开关同时也放入环境变量，确保后端能读到
        env_overrides: {
            BOCHA_WEB_SEARCH_API_KEY: document.getElementById('bochaApiKey').value,
            INCLUDE_REALTIME: document.getElementById('includeRealtime').checked ? 'true' : 'false'
        },

        db_overrides: dbOverrides,
        agent_overrides: {
            tech: { base_url: document.getElementById('techBaseUrl').value, api_key: document.getElementById('techApiKey').value, model_name: document.getElementById('techModelName').value },
            fund: { base_url: document.getElementById('fundBaseUrl').value, api_key: document.getElementById('fundApiKey').value, model_name: document.getElementById('fundModelName').value },
            news: { base_url: document.getElementById('newsBaseUrl').value, api_key: document.getElementById('newsApiKey').value, model_name: document.getElementById('newsModelName').value }
        }
    };

    try {
        setGlobalStatus('running');
        // 切换到监控视图
        document.querySelector('[data-view="view-monitor"]').click();

        // 重置日志
        state.logs = {};
        document.getElementById('logBox').textContent = '';

        const resp = await fetch('/api/analysis/run', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload) });
        const data = await resp.json();

        if (!data.success) throw new Error(data.message);

        state.jobId = data.job_id;
        state.pollTimer = setInterval(() => poll(state.jobId), 2000);

    } catch (e) {
        alert('启动失败: ' + e.message);
        setGlobalStatus('idle');
    }
}

async function poll(jobId) {
    try {
        const resp = await fetch(`/api/analysis/status/${jobId}`);
        const data = await resp.json();
        const job = data.job;

        appendLog(job.log);
        updateAgentCards(job.agents);

        if (job.status === 'done' || job.status === 'error') {
            clearInterval(state.pollTimer);
            setGlobalStatus('idle');

            if (job.status === 'done') {
                const action = job.result?.action || 'analysis';
                const reportUrls = job.result?.report_urls || {};
                if (action === 'analysis') {
                    // 1. 优先显示报告链接
                    if (Object.keys(reportUrls).length) {
                        renderReportLinks(reportUrls);
                    }
                    // 2. 加载图表与证据
                    loadCharts();
                    loadEvidence();
                    // 3. 自动切到报告页
                    document.querySelector('[data-view="view-reports"]').click();
                }
            }
        }
    } catch (e) {
        console.error(e);
    }
}

// 报告与图表
function renderReportLinks(urls) {
    const container = document.getElementById('reportLinks');
    container.innerHTML = '';
    const iframe = document.getElementById('reportFrame');

    Object.keys(urls).forEach((key, idx) => {
        const a = document.createElement('a');
        a.textContent = key.toUpperCase();
        a.onclick = () => {
            iframe.src = urls[key];
            document.querySelectorAll('.pill-list a').forEach(el => el.classList.remove('active'));
            a.classList.add('active');
            document.getElementById('openNewTabBtn').href = urls[key];
        };
        container.appendChild(a);
        if (idx === 0) a.click();
    });

    // 加载图表列表
    loadCharts(document.getElementById('symbolInput').value);
}

// [重写] 加载图表（入口）
async function loadCharts() {
    const symbols = await resolveSymbolsFromInput();
    const container = document.getElementById('chartSymbolLinks');

    if (!symbols.length) {
        container.style.display = 'none';
        document.getElementById('chartLinks').innerHTML = '<small>请输入股票代码</small>';
        return;
    }

    const firstSymbol = symbols[0].symbol;
    // 渲染切换按钮
    setSymbolButtons(container, symbols, firstSymbol, (selectedSymbol) => {
        loadChartsForSymbol(selectedSymbol);
    });
    // 默认加载第一个
    loadChartsForSymbol(firstSymbol);
}

// [修改] 加载指定股票的图表 (补全所有周期)
async function loadChartsForSymbol(symbol) {
    const container = document.getElementById('chartLinks');
    container.innerHTML = '<small>加载中...</small>';
    const iframe = document.getElementById('reportFrame');

    try {
        // 补全了 year, 60m, 30m, 15m, 5m 等周期
        const resp = await fetch('/api/market/charts', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({
                symbol,
                timeframes: ['day', 'week', 'month', 'year', '60m', '30m', '15m', '5m']
            })
        });
        const data = await resp.json();

        container.innerHTML = '';
        const urls = data.urls || {};
        if (!urls.day) {
            const guess = `/reports/${symbol}/interactive/${symbol}_day.html`;
            try {
                const head = await fetch(guess, { method: 'HEAD' });
                if (head.ok) {
                    urls.day = guess;
                }
            } catch (e) {
                // ignore
            }
        }
        if(Object.keys(urls).length > 0) {
            // 定义显示顺序和中文标签
            const labelMap = {
                'day': '日线', 'week': '周线', 'month': '月线', 'year': '年线',
                '60m': '60分', '30m': '30分', '15m': '15分', '5m': '5分'
            };
            // 按照我们想要的顺序排序，而不是随机顺序
            const order = ['day', 'week', 'month', 'year', '60m', '30m', '15m', '5m'];

            order.forEach(key => {
                if (!urls[key]) return;

                const a = document.createElement('a');
                a.textContent = labelMap[key] || key; // 显示中文
                a.onclick = (e) => {
                    e.preventDefault(); // 防止页面跳动
                    iframe.src = urls[key];
                    // 清除所有 active
                    document.querySelectorAll('#reportLinks a, #chartLinks a').forEach(el => el.classList.remove('active'));
                    a.classList.add('active');
                    document.getElementById('openNewTabBtn').href = urls[key];
                };
                container.appendChild(a);
            });
        } else {
            container.innerHTML = '<small>无图表数据</small>';
        }
    } catch (e) {
        container.innerHTML = `<small style="color:red">加载失败: ${e.message}</small>`;
    }
}

// 证据库
// [重写] 加载证据（入口）
async function loadEvidence() {
    const symbols = await resolveSymbolsFromInput();
    const container = document.getElementById('evidenceSymbolLinks');

    if (!symbols.length) {
        container.style.display = 'none';
        return;
    }

    const firstSymbol = symbols[0].symbol;
    setSymbolButtons(container, symbols, firstSymbol, (selectedSymbol) => {
        loadEvidenceForSymbol(selectedSymbol);
    });
    loadEvidenceForSymbol(firstSymbol);
}

// [新增] 加载指定股票的证据
async function loadEvidenceForSymbol(symbol) {
    const tbody = document.getElementById('evidenceBody');
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center">加载中...</td></tr>';

    try {
        const resp = await fetch(`/api/evidence/${symbol}`);
        const data = await resp.json();
        tbody.innerHTML = '';

        const items = data.items || [];
        window.currentEvidenceItems = items; // 保存供搜索用

        if (items.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center">暂无证据数据</td></tr>';
            return;
        }

        renderEvidenceRows(items);
    } catch (e) {
        tbody.innerHTML = `<tr><td colspan="7" style="text-align:center;color:red">加载错误: ${e.message}</td></tr>`;
    }
}

function renderEvidenceRows(items) {
    const tbody = document.getElementById('evidenceBody');
    tbody.innerHTML = '';
    items.forEach(item => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${item.id || ''}</td>
            <td><small>${item.type || '未知'}</small></td>
            <td>${item.time || ''}</td>
            <td title="${item.title || ''}">${item.title || ''}</td>
            <td title="${item.summary || ''}">${item.summary || ''}</td>
            <td>${item.source || ''}</td>
            <td>${item.url ? `<a href="${item.url}" target="_blank">链接</a>` : '-'}</td>
        `;
        tbody.appendChild(tr);
    });
}

// ==========================================
// Config load/save
// ==========================================

async function loadConfig(resetLocal = false) {
    if (resetLocal) {
        try {
            localStorage.removeItem(STORAGE_KEYS.form);
            localStorage.removeItem(STORAGE_KEYS.settings);
        } catch (e) {
            console.warn('Failed to reset local state', e);
        }
    }
    try {
        const resp = await fetch('/api/config');
        const data = await resp.json();
        const cfg = data.config || {};
        applyServerConfig(cfg);
        if (!resetLocal) loadLocalState();
        showToast('Config loaded');
    } catch (e) {
        console.error('Load config failed', e);
        showToast('Load config failed', true);
    }
}

async function saveConfig() {
    const payload = buildConfigPayload();
    saveLocalState();
    try {
        const resp = await fetch('/api/config', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        const data = await resp.json();
        if (data.success) {
            showToast('Config saved');
        } else {
            showToast('Save failed: ' + (data.message || ''), true);
        }
    } catch (e) {
        showToast('Save request failed', true);
    }
}
// ==========================================

document.getElementById('runAnalysisBtn').onclick = () => runJob('analysis');
document.getElementById('runCrawlerBtn').onclick = () => runJob('crawler');
document.getElementById('stopPollingBtn').onclick = () => { clearInterval(state.pollTimer); setGlobalStatus('idle'); };

// 证据库刷新按钮
document.getElementById('loadEvidenceBtn').onclick = () => loadEvidence();

// [插入在这里] 证据库搜索框输入监听
document.getElementById('evidenceSearch').addEventListener('input', (e) => {
    const val = e.target.value.toLowerCase();
    // window.currentEvidenceItems 是我们在 loadEvidenceForSymbol 里保存的全局变量
    const items = window.currentEvidenceItems || [];

    if(!items.length) {
        // 如果没有数据，清空或显示暂无
        return;
    }

    // 过滤逻辑：匹配标题、摘要或来源
    const filtered = items.filter(item =>
        (item.title && item.title.toLowerCase().includes(val)) ||
        (item.summary && item.summary.toLowerCase().includes(val)) ||
        (item.source && item.source.toLowerCase().includes(val))
    );

    // 调用之前定义的渲染函数刷新表格
    renderEvidenceRows(filtered);
});

document.getElementById('loadConfigBtn').onclick = () => loadConfig(true);
document.getElementById('saveConfigBtn').onclick = saveConfig;

// [新增] 修复导出 PDF 功能
document.getElementById('exportPdfBtn').onclick = async () => {
    const iframe = document.getElementById('reportFrame');
    const symbol = resolveExportSymbol();
    if (!symbol && (!iframe.src || iframe.src === 'about:blank')) {
        alert('请先生成并加载报告');
        return;
    }
    if (symbol) {
        try {
            await downloadFile(`/api/export/pdf?symbol=${encodeURIComponent(symbol)}`, `${symbol}_report.pdf`);
            return;
        } catch (e) {
            console.warn(e);
        }
    }
    if (!iframe.src || iframe.src === 'about:blank') {
        alert('请先生成并加载报告');
        return;
    }
    try {
        iframe.contentWindow.focus();
        iframe.contentWindow.print();
    } catch (e) {
        window.open(iframe.src, '_blank');
    }
};

// [新增] 修复导出 CSV 功能
document.getElementById('exportCsvBtn').onclick = async () => {
    const symbol = resolveExportSymbol();
    if (!symbol) return alert('请输入或选择股票代码');
    try {
        await downloadFile(`/api/export/csv?symbol=${encodeURIComponent(symbol)}`, `${symbol}_all_data.csv`);
    } catch (e) {
        alert(`导出失败: ${e.message}`);
    }
};

// 页面加载完毕自动加载配置
window.onload = () => loadConfig(false);
