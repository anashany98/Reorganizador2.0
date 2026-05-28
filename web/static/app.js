const startBtn = document.getElementById('startBtn');
const stopBtn = document.getElementById('stopBtn');
const previewBtn = document.getElementById('previewBtn');
const progressBar = document.getElementById('progressBar');
const progressText = document.getElementById('progressText');
const logContent = document.getElementById('logContent');
const statusText = document.getElementById('statusText');
const statusBadge = document.getElementById('statusBadge');

const statProcessed = document.getElementById('statProcessed');
const statSinNumero = document.getElementById('statSinNumero');
const statNoEncontrado = document.getElementById('statNoEncontrado');
const statAmbiguo = document.getElementById('statAmbiguo');
const statErrors = document.getElementById('statErrors');

let pollingInterval = null;
let reportInterval = null;
let lastLoggedFile = "";
let targetInputId = null;
let currentBrowsePath = "";
let currentTab = 'console';
let isSidebarCollapsed = localStorage.getItem('sidebar_collapsed') === 'true';

document.addEventListener('DOMContentLoaded', () => {
    loadConfig();
    initPresets();
    initSidebar();
    initDragDrop();
    initKeyboardShortcuts();
    setRuntimeState('idle', 'Listo');
    switchTab('console');
});

// ============================================================
// TOAST SYSTEM
// ============================================================

function showToast(message, type = 'info') {
    const container = document.getElementById('toastContainer');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    const icons = {
        success: '<svg viewBox="0 0 16 16"><polyline points="3,8 6,11 13,4"/></svg>',
        error: '<svg viewBox="0 0 16 16"><circle cx="8" cy="8" r="6"/><line x1="5" y1="5" x2="11" y2="11"/><line x1="11" y1="5" x2="5" y2="11"/></svg>',
        warn: '<svg viewBox="0 0 16 16"><path d="M8 2l6 11H2z"/><line x1="8" y1="6" x2="8" y2="9"/><circle cx="8" cy="11" r="0.5" fill="currentColor"/></svg>',
        info: '<svg viewBox="0 0 16 16"><circle cx="8" cy="8" r="6"/><line x1="8" y1="7" x2="8" y2="11"/><circle cx="8" cy="5" r="0.5" fill="currentColor"/></svg>'
    };
    toast.innerHTML = `${icons[type] || icons.info}<span class="toast-message">${message}</span>`;
    container.appendChild(toast);
    setTimeout(() => {
        toast.classList.add('toast-out');
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}

// ============================================================
// SIDEBAR COLLAPSE
// ============================================================

function initSidebar() {
    const sidebar = document.getElementById('sidebar');
    if (isSidebarCollapsed) {
        sidebar.classList.add('collapsed');
    }
}

function toggleSidebar() {
    const sidebar = document.getElementById('sidebar');
    sidebar.classList.toggle('collapsed');
    isSidebarCollapsed = sidebar.classList.contains('collapsed');
    localStorage.setItem('sidebar_collapsed', isSidebarCollapsed);
}

// ============================================================
// DRAG & DROP
// ============================================================

function initDragDrop() {
    const sourceInput = document.getElementById('source');
    const destInput = document.getElementById('dest');

    [sourceInput, destInput].forEach(input => {
        if (!input) return;

        // Make the parent .input-row a drop zone
        const row = input.closest('.input-row');
        if (!row) return;

        row.addEventListener('dragover', (e) => {
            e.preventDefault();
            row.classList.add('drag-over');
        });

        row.addEventListener('dragleave', (e) => {
            if (!row.contains(e.relatedTarget)) {
                row.classList.remove('drag-over');
            }
        });

        row.addEventListener('drop', (e) => {
            e.preventDefault();
            row.classList.remove('drag-over');
            const items = e.dataTransfer.items;
            if (items && items.length > 0) {
                const item = items[0];
                if (item.kind === 'file') {
                    const entry = item.webkitGetAsEntry ? item.webkitGetAsEntry() : null;
                    if (entry && entry.isDirectory) {
                        const file = e.dataTransfer.files[0];
                        if (file && file.path) {
                            input.value = file.path;
                            saveConfig();
                        }
                    } else {
                        // Try to get path from file
                        const file = e.dataTransfer.files[0];
                        if (file && file.path) {
                            input.value = file.path;
                            saveConfig();
                        }
                    }
                }
            }
        });
    });

    // Also handle the source/dest inputs directly for drop
    [sourceInput, destInput].forEach(input => {
        if (!input) return;
        input.addEventListener('dragover', (e) => {
            e.preventDefault();
            input.classList.add('drag-over');
        });
        input.addEventListener('dragleave', () => {
            input.classList.remove('drag-over');
        });
        input.addEventListener('drop', (e) => {
            e.preventDefault();
            input.classList.remove('drag-over');
            const file = e.dataTransfer.files[0];
            if (file && file.path) {
                input.value = file.path;
                saveConfig();
            }
        });
    });
}

// ============================================================
// KEYBOARD SHORTCUTS
// ============================================================

function initKeyboardShortcuts() {
    document.addEventListener('keydown', (e) => {
        // Ctrl+Enter or Cmd+Enter -> start scan
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            e.preventDefault();
            if (!startBtn.classList.contains('hidden')) {
                startScan();
            }
        }
        // Escape -> close modals
        if (e.key === 'Escape') {
            closeBrowseModal();
            closePreviewModal();
            closePresetModal();
        }
        // Ctrl+P -> preview
        if ((e.ctrlKey || e.metaKey) && e.key === 'p') {
            e.preventDefault();
            doPreview();
        }
        // Ctrl+B -> toggle sidebar
        if ((e.ctrlKey || e.metaKey) && e.key === 'b') {
            e.preventDefault();
            toggleSidebar();
        }
    });
}

// ============================================================
// PRESETS
// ============================================================

function initPresets() {
    const select = document.getElementById('presetSelect');
    if (!select) return;
    refreshPresetList();
}

function refreshPresetList() {
    const select = document.getElementById('presetSelect');
    if (!select) return;
    const presets = JSON.parse(localStorage.getItem('reorganizer_presets') || '{}');
    select.innerHTML = '<option value="">-- Cargar preset --</option>';
    Object.keys(presets).forEach(name => {
        const opt = document.createElement('option');
        opt.value = name;
        opt.textContent = name;
        select.appendChild(opt);
    });
}

function loadPreset(name) {
    if (!name) return;
    const presets = JSON.parse(localStorage.getItem('reorganizer_presets') || '{}');
    const preset = presets[name];
    if (!preset) return;
    if (preset.source) document.getElementById('source').value = preset.source;
    if (preset.dest) document.getElementById('dest').value = preset.dest;
    if (preset.mappingExcel) document.getElementById('mappingExcel').value = preset.mappingExcel;
    if (preset.years) setSelectedYears(preset.years);
    if (preset.organizeBy) document.getElementById('organizeBy').value = preset.organizeBy;
    if (preset.moveFiles !== undefined) document.getElementById('moveFiles').checked = preset.moveFiles;
    if (preset.dryRun !== undefined) document.getElementById('dryRun').checked = preset.dryRun;
    if (preset.minSize) document.getElementById('minSize').value = preset.minSize;
    if (preset.extensions) document.getElementById('extensions').value = preset.extensions;
    if (preset.projectFilter) document.getElementById('projectFilter').value = preset.projectFilter;
    if (preset.unmatchedDir) document.getElementById('unmatchedDir').value = preset.unmatchedDir;
    if (preset.requireBudgetMatch !== undefined) document.getElementById('requireBudgetMatch').checked = preset.requireBudgetMatch;
    if (preset.threads) document.getElementById('threads').value = preset.threads;
    if (preset.processes) document.getElementById('processes').value = preset.processes;
    if (preset.conflict) document.getElementById('conflict').value = preset.conflict;
    if (preset.dedup !== undefined) document.getElementById('dedup').checked = preset.dedup;
    saveConfig();
    showToast(`Preset "${name}" cargado`, 'success');
}

function openPresetModal() {
    document.getElementById('presetModal').classList.add('active');
    document.getElementById('presetName').value = '';
    document.getElementById('presetName').focus();
}

function closePresetModal() {
    document.getElementById('presetModal').classList.remove('active');
}

function savePreset() {
    const name = document.getElementById('presetName').value.trim();
    if (!name) {
        showToast('Ingresa un nombre para el preset', 'error');
        return;
    }
    const presets = JSON.parse(localStorage.getItem('reorganizer_presets') || '{}');
    const currentConfig = {
        source: document.getElementById('source').value,
        dest: document.getElementById('dest').value,
        mappingExcel: document.getElementById('mappingExcel').value,
        years: getSelectedYears(),
        organizeBy: document.getElementById('organizeBy').value,
        moveFiles: document.getElementById('moveFiles').checked,
        dryRun: document.getElementById('dryRun').checked,
        minSize: document.getElementById('minSize').value,
        extensions: document.getElementById('extensions').value,
        projectFilter: document.getElementById('projectFilter').value,
        unmatchedDir: document.getElementById('unmatchedDir').value,
        requireBudgetMatch: document.getElementById('requireBudgetMatch').checked,
        threads: document.getElementById('threads').value,
        processes: document.getElementById('processes').value,
        conflict: document.getElementById('conflict').value,
        dedup: document.getElementById('dedup').checked,
    };
    presets[name] = currentConfig;
    localStorage.setItem('reorganizer_presets', JSON.stringify(presets));
    refreshPresetList();
    document.getElementById('presetSelect').value = name;
    closePresetModal();
    showToast(`Preset "${name}" guardado`, 'success');
}

function deleteCurrentPreset() {
    const select = document.getElementById('presetSelect');
    const name = select.value;
    if (!name) {
        showToast('Selecciona un preset para eliminar', 'error');
        return;
    }
    const presets = JSON.parse(localStorage.getItem('reorganizer_presets') || '{}');
    delete presets[name];
    localStorage.setItem('reorganizer_presets', JSON.stringify(presets));
    refreshPresetList();
    showToast(`Preset "${name}" eliminado`, 'info');
}

// ============================================================
// PREVIEW
// ============================================================

async function doPreview() {
    const source = document.getElementById('source').value;
    const projects = document.getElementById('projectFilter').value;
    const mappingExcel = document.getElementById('mappingExcel').value;
    const years = getSelectedYears().join(',');
    const dest = document.getElementById('dest').value;
    const unmatchedDir = document.getElementById('unmatchedDir').value || '_REVISION';
    if (!source) {
        showToast('Especifica una carpeta origen', 'error');
        return;
    }
    const modal = document.getElementById('previewModal');
    const content = document.getElementById('previewContent');
    modal.classList.add('active');
    content.innerHTML = '<div class="preview-loading">Analizando carpeta...</div>';

    try {
        const params = new URLSearchParams({
            source,
            projects,
            mapping_excel: mappingExcel,
            years,
            dest,
            unmatched_dir: unmatchedDir,
        });
        const resp = await fetch(`/api/preview?${params.toString()}`);
        if (!resp.ok) throw new Error((await resp.json()).detail || 'Error');
        const data = await resp.json();
        renderPreview(data);
    } catch (err) {
        content.innerHTML = `<div class="preview-loading" style="color:var(--err)">Error: ${err.message}</div>`;
    }
}

function renderPreview(data) {
    const content = document.getElementById('previewContent');
    const fmtSize = (bytes) => {
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
        if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
        return (bytes / 1073741824).toFixed(2) + ' GB';
    };

    const counters = data.match_counters || {};
    let html = `
    <div class="preview-grid">
        <div class="preview-stat">
            <span class="preview-stat-num">${data.total_files.toLocaleString()}</span>
            <span class="preview-stat-label">Archivos totales</span>
        </div>
        <div class="preview-stat">
            <span class="preview-stat-num">${fmtSize(data.total_size_bytes)}</span>
            <span class="preview-stat-label">Tamano total</span>
        </div>
        <div class="preview-stat">
            <span class="preview-stat-num">${data.processed_already.toLocaleString()}</span>
            <span class="preview-stat-label">Ya procesados</span>
        </div>
        <div class="preview-stat">
            <span class="preview-stat-num">${data.pending.toLocaleString()}</span>
            <span class="preview-stat-label">Pendientes</span>
        </div>
        <div class="preview-stat">
            <span class="preview-stat-num">${(counters.OK || 0).toLocaleString()}</span>
            <span class="preview-stat-label">OK</span>
        </div>
        <div class="preview-stat">
            <span class="preview-stat-num">${(counters.AMBIGUO || 0).toLocaleString()}</span>
            <span class="preview-stat-label">Ambiguos</span>
        </div>
    </div>`;

    if (data.items && data.items.length > 0) {
        html += `<div class="preview-section">
            <h4>Coincidencias FactuSOL</h4>
            <div class="table-wrap preview-table-wrap">
                <table>
                    <thead>
                        <tr>
                            <th>Archivo</th>
                            <th>Presupuesto</th>
                            <th>Cliente</th>
                            <th>Sede_Hotel_Direccion</th>
                            <th>Referencia</th>
                            <th>TipoDocumento</th>
                            <th>MatchStatus</th>
                            <th>Confianza</th>
                            <th>Ruta destino</th>
                        </tr>
                    </thead>
                    <tbody>`;
        data.items.forEach((item) => {
            html += `<tr>
                <td title="${escapeHtml(item.src_path || '')}">${escapeHtml(item.file_name || '')}</td>
                <td>${escapeHtml(item.presupuesto_detectado || '')}</td>
                <td>${escapeHtml(item.cliente || '')}</td>
                <td>${escapeHtml(item.sede_hotel_direccion || '')}</td>
                <td>${escapeHtml(item.referencia || '')}</td>
                <td>${escapeHtml(item.tipo_documento || '')}</td>
                <td>${escapeHtml(item.match_status || '')}</td>
                <td>${escapeHtml(String(item.match_confidence ?? ''))}</td>
                <td title="${escapeHtml(item.dst_path || '')}">${escapeHtml(item.dst_path || '')}</td>
            </tr>`;
        });
        html += `</tbody></table></div></div>`;
    }

    // Extensions
    const exts = Object.entries(data.extensions || {}).sort((a, b) => b[1] - a[1]).slice(0, 15);
    if (exts.length > 0) {
        html += `<div class="preview-section">
            <h4>Por extension</h4>
            <div class="preview-list">`;
        exts.forEach(([ext, count]) => {
            html += `<div class="preview-list-item">
                <span>.${ext}</span>
                <span class="preview-ext-count">${count.toLocaleString()}</span>
            </div>`;
        });
        html += `</div></div>`;
    }

    // Gestores
    const gestos = Object.entries(data.gestores || {}).sort((a, b) => b[1] - a[1]).slice(0, 10);
    if (gestos.length > 0) {
        html += `<div class="preview-section">
            <h4>Por gestor</h4>
            <div class="preview-list">`;
        gestos.forEach(([gesto, count]) => {
            html += `<div class="preview-list-item">
                <span>${gesto}</span>
                <span class="preview-gestor-count">${count.toLocaleString()}</span>
            </div>`;
        });
        html += `</div></div>`;
    }

    content.innerHTML = html;
}

function closePreviewModal() {
    document.getElementById('previewModal').classList.remove('active');
}

// ============================================================
// RUNTIME STATE
// ============================================================

function setRuntimeState(state, label) {
    document.body.dataset.runtime = state;
    if (statusBadge) {
        statusBadge.dataset.state = state;
    }
    if (label) {
        statusText.innerText = label;
    }
}

// ============================================================
// CONFIG
// ============================================================

function getSelectedYears() {
    const years = [];
    if (document.getElementById('year2025').checked) years.push('2025');
    if (document.getElementById('year2026').checked) years.push('2026');
    return years;
}

function setSelectedYears(years) {
    const selected = Array.isArray(years) ? years : String(years || '').split(',');
    document.getElementById('year2025').checked = selected.includes('2025');
    document.getElementById('year2026').checked = selected.includes('2026');
}

function escapeHtml(value) {
    return String(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

function loadConfig() {
    const saved = JSON.parse(localStorage.getItem('reorganizer_config') || '{}');
    if (saved.source) document.getElementById('source').value = saved.source;
    if (saved.dest) document.getElementById('dest').value = saved.dest;
    if (saved.mappingExcel) document.getElementById('mappingExcel').value = saved.mappingExcel;
    if (saved.years) setSelectedYears(saved.years);
    if (saved.organizeBy) document.getElementById('organizeBy').value = saved.organizeBy;
    if (saved.moveFiles !== undefined) document.getElementById('moveFiles').checked = saved.moveFiles;
    if (saved.dryRun !== undefined) document.getElementById('dryRun').checked = saved.dryRun;
    if (saved.minSize) document.getElementById('minSize').value = saved.minSize;
    if (saved.extensions) document.getElementById('extensions').value = saved.extensions;
    if (saved.projectFilter) document.getElementById('projectFilter').value = saved.projectFilter;
    if (saved.unmatchedDir) document.getElementById('unmatchedDir').value = saved.unmatchedDir;
    if (saved.requireBudgetMatch !== undefined) document.getElementById('requireBudgetMatch').checked = saved.requireBudgetMatch;
    if (saved.threads) document.getElementById('threads').value = saved.threads;
    if (saved.processes) document.getElementById('processes').value = saved.processes;
    if (saved.conflict) document.getElementById('conflict').value = saved.conflict;
    if (saved.dedup !== undefined) document.getElementById('dedup').checked = saved.dedup;
}

function saveConfig() {
    const config = {
        source: document.getElementById('source').value,
        dest: document.getElementById('dest').value,
        mappingExcel: document.getElementById('mappingExcel').value,
        years: getSelectedYears(),
        organizeBy: document.getElementById('organizeBy').value,
        moveFiles: document.getElementById('moveFiles').checked,
        dryRun: document.getElementById('dryRun').checked,
        minSize: document.getElementById('minSize').value,
        extensions: document.getElementById('extensions').value,
        projectFilter: document.getElementById('projectFilter').value,
        unmatchedDir: document.getElementById('unmatchedDir').value,
        requireBudgetMatch: document.getElementById('requireBudgetMatch').checked,
        threads: document.getElementById('threads').value,
        processes: document.getElementById('processes').value,
        conflict: document.getElementById('conflict').value,
        dedup: document.getElementById('dedup').checked,
    };
    localStorage.setItem('reorganizer_config', JSON.stringify(config));
}

['source', 'dest', 'mappingExcel', 'year2025', 'year2026', 'organizeBy', 'moveFiles', 'dryRun', 'minSize', 'extensions', 'projectFilter', 'unmatchedDir', 'requireBudgetMatch', 'threads', 'processes', 'conflict', 'dedup'].forEach((id) => {
    const element = document.getElementById(id);
    if (element) element.addEventListener('change', saveConfig);
});

function resetProgress() {
    if (progressBar) progressBar.style.width = '0%';
    if (progressText) progressText.textContent = '0%';
    statProcessed.innerText = '0';
    statSinNumero.innerText = '0';
    statNoEncontrado.innerText = '0';
    statAmbiguo.innerText = '0';
    statErrors.innerText = '0';
}

// ============================================================
// SCAN
// ============================================================

async function startScan() {
    saveConfig();
    lastLoggedFile = "";
    resetProgress();

    const config = {
        source: document.getElementById('source').value,
        dest: document.getElementById('dest').value,
        organize_by: document.getElementById('organizeBy').value,
        mapping_excel: document.getElementById('mappingExcel').value,
        years: getSelectedYears().join(','),
        unmatched_dir: document.getElementById('unmatchedDir').value || '_REVISION',
        require_budget_match: document.getElementById('requireBudgetMatch').checked,
        move: document.getElementById('moveFiles').checked,
        dry_run: document.getElementById('dryRun').checked,
        min_size_mb: parseFloat(document.getElementById('minSize').value) || 0,
        extensions: document.getElementById('extensions').value,
        project_filter: document.getElementById('projectFilter').value,
        threads: parseInt(document.getElementById('threads').value) || 0,
        processes: parseInt(document.getElementById('processes').value) || 0,
        conflict: document.getElementById('conflict').value,
        dedup: document.getElementById('dedup').checked,
    };

    if (!config.source) {
        showToast('Debes especificar una carpeta origen', 'error');
        setRuntimeState('error', 'Falta origen');
        return;
    }
    if (config.organize_by === 'factusol-client-budget' && !config.mapping_excel) {
        showToast('Selecciona el Excel simplificado de FactuSOL', 'error');
        setRuntimeState('error', 'Falta Excel');
        return;
    }
    if (config.organize_by === 'factusol-client-budget' && !config.dest) {
        showToast('Debes especificar una carpeta destino', 'error');
        setRuntimeState('error', 'Falta destino');
        return;
    }

    setLoading(true);
    setRuntimeState('preparing', 'Preparando');
    log('Iniciando escaneo...', 'system');

    try {
        const response = await fetch('/api/scan', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(config),
        });

        if (!response.ok) {
            throw new Error((await response.json()).detail || 'Error en el servidor');
        }

        const data = await response.json();
        log(`Tarea iniciada: ${data.job_id}`, 'system');

        if (pollingInterval) clearInterval(pollingInterval);
        pollingInterval = setInterval(checkStatus, 500);

        // Auto-refresh report during scan
        if (reportInterval) clearInterval(reportInterval);
        reportInterval = setInterval(() => {
            if (currentTab === 'report') loadReport();
        }, 5000);
    } catch (err) {
        setRuntimeState('error', 'Error');
        showToast(err.message, 'error');
        setLoading(false);
    }
}

function startRealCopy() {
    document.getElementById('dryRun').checked = false;
    saveConfig();
    startScan();
}

async function stopScan() {
    try {
        await fetch('/api/stop', { method: 'POST' });
        setRuntimeState('stopping', 'Deteniendo');
        log('Solicitando cancelacion...', 'warning');
        stopBtn.disabled = true;
    } catch (err) {
        setRuntimeState('error', 'Error');
        showToast(`Error al detener: ${err.message}`, 'error');
    }
}

async function checkStatus() {
    try {
        const response = await fetch('/api/status');
        const data = await response.json();
        const percent = data.percent ?? Math.round((data.processed / (data.total || 1)) * 100);

        const matchCounters = data.match_counters || {};
        statProcessed.innerText = matchCounters.OK ?? data.stats.processed;
        statSinNumero.innerText = matchCounters.SIN_NUMERO_PRESUPUESTO || 0;
        statNoEncontrado.innerText = matchCounters.NO_ENCONTRADO_EN_EXCEL || 0;
        statAmbiguo.innerText = matchCounters.AMBIGUO || 0;
        statErrors.innerText = data.stats.errors;
        if (progressBar) progressBar.style.width = `${percent}%`;
        if (progressText) progressText.textContent = `${percent}%`;

        if (!data.active) {
            clearInterval(pollingInterval);
            pollingInterval = null;
            if (reportInterval) {
                clearInterval(reportInterval);
                reportInterval = null;
            }
            setLoading(false);

            if (percent >= 100) {
                log('Proceso completado.', 'system');
                setRuntimeState(
                    data.stats.errors > 0 ? 'warning' : 'complete',
                    data.stats.errors > 0 ? 'Completado con errores' : 'Completado'
                );
                showToast(data.stats.errors > 0 ? 'Escaneo completo con errores' : 'Escaneo completado', data.stats.errors > 0 ? 'warn' : 'success');
            }

            loadHistory(1);
            loadReport();
            return;
        }

        setRuntimeState('running', `Procesando ${percent}%`);

        if (data.current_file && data.current_file !== lastLoggedFile) {
            lastLoggedFile = data.current_file;
            log(`Scan: ${data.current_file}`, 'info');
        }
    } catch (err) {
        console.error('Error polling status:', err);
        setRuntimeState('warning', 'Sin respuesta');
    }
}

function log(message, type = 'info') {
    const line = document.createElement('div');
    line.className = `log-line ${type}`;
    line.innerText = `> ${message}`;
    logContent.appendChild(line);
    logContent.scrollTop = logContent.scrollHeight;
}

function setLoading(isLoading) {
    if (isLoading) {
        startBtn.classList.add('hidden');
        if (previewBtn) previewBtn.classList.add('hidden');
        stopBtn.classList.remove('hidden');
        stopBtn.disabled = false;
    } else {
        startBtn.classList.remove('hidden');
        if (previewBtn) previewBtn.classList.remove('hidden');
        stopBtn.classList.add('hidden');
    }
}

// ============================================================
// TABS
// ============================================================

function switchTab(tabName) {
    currentTab = tabName;
    document.querySelectorAll('.tab-btn').forEach((button) => button.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach((content) => content.classList.remove('active'));

    const buttons = document.querySelectorAll('.tab-btn');
    if (tabName === 'console') buttons[0].classList.add('active');
    if (tabName === 'history') {
        buttons[1].classList.add('active');
        loadHistory(1);
    }
    if (tabName === 'report') {
        buttons[2].classList.add('active');
        loadReport();
    }

    document.getElementById(`${tabName}Tab`).classList.add('active');
}

// ============================================================
// HISTORY
// ============================================================

async function loadHistory(page = 1) {
    const tbody = document.querySelector('#historyTable tbody');
    tbody.innerHTML = '<tr><td colspan="5">Cargando...</td></tr>';

    try {
        const response = await fetch(`/api/history?page=${page}&page_size=50`);
        const data = await response.json();

        tbody.innerHTML = '';
        if (data.items.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5">No hay historial reciente</td></tr>';
            renderPagination(0, page, 50);
            return;
        }

        data.items.forEach((item) => {
            const row = document.createElement('tr');
            const dateCell = document.createElement('td');
            dateCell.textContent = item.created_time || '';

            const fileCell = document.createElement('td');
            fileCell.title = item.file_name || '';
            fileCell.textContent = item.file_name || '';

            const actionCell = document.createElement('td');
            actionCell.textContent = item.action || '';

            const statusCell = document.createElement('td');
            statusCell.textContent = item.action_status || '';
            if (item.action_status === 'error') statusCell.classList.add('error-text');

            const copyCell = document.createElement('td');
            const copyButton = document.createElement('button');
            copyButton.className = 'copy-btn';
            copyButton.type = 'button';
            copyButton.title = 'Copiar ruta';
            copyButton.innerHTML = '<svg viewBox="0 0 16 16"><rect x="5" y="5" width="9" height="9" rx="1"/><path d="M3 11V3a1 1 0 0 1 1-1h8"/></svg>';
            copyButton.addEventListener('click', () => copyPath(copyButton, item.src_path || ''));
            copyCell.appendChild(copyButton);

            row.append(dateCell, fileCell, actionCell, statusCell, copyCell);
            tbody.appendChild(row);
        });

        renderPagination(data.total, page, data.page_size);
    } catch (err) {
        showToast('Error al cargar historial', 'error');
        tbody.innerHTML = `<tr><td colspan="5" class="error-text">Error: ${err.message}</td></tr>`;
    }
}

function copyPath(btn, path) {
    if (!path) return;
    navigator.clipboard.writeText(path).then(() => {
        btn.classList.add('copied');
        btn.innerHTML = '<svg viewBox="0 0 16 16"><polyline points="3,8 6,11 13,4"/></svg>';
        setTimeout(() => {
            btn.classList.remove('copied');
            btn.innerHTML = '<svg viewBox="0 0 16 16"><rect x="5" y="5" width="9" height="9" rx="1"/><path d="M3 11V3a1 1 0 0 1 1-1h8"/></svg>';
        }, 1500);
    }).catch(() => {
        showToast('No se pudo copiar', 'error');
    });
}

function renderPagination(total, currentPage, pageSize) {
    const container = document.getElementById('historyPagination');
    if (!container) return;

    const totalPages = Math.ceil(total / pageSize);
    if (totalPages <= 1) {
        container.innerHTML = '';
        document.getElementById('historyInfo').textContent = total > 0 ? `${total} registros` : '';
        return;
    }

    document.getElementById('historyInfo').textContent = `${total} registros · Pag ${currentPage} de ${totalPages}`;

    let html = '';
    if (currentPage > 1) {
        html += `<button class="btn btn-ghost btn-xs" onclick="loadHistory(${currentPage - 1})">Anterior</button>`;
    }
    html += `<span class="page-indicator">${currentPage} / ${totalPages}</span>`;
    if (currentPage < totalPages) {
        html += `<button class="btn btn-ghost btn-xs" onclick="loadHistory(${currentPage + 1})">Siguiente</button>`;
    }
    container.innerHTML = html;
}

// ============================================================
// REPORT
// ============================================================

async function loadReport() {
    try {
        const response = await fetch('/api/report');
        const data = await response.json();

        // Tabla de gestores
        const gestorTbody = document.querySelector('#reportGestorTable tbody');
        gestorTbody.innerHTML = '';
        if (data.gestores && data.gestores.length > 0) {
            data.gestores.forEach((g) => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${g.gestor}</td>
                    <td>${g.total}</td>
                    <td>${g.copiados || 0}</td>
                    <td>${g.movidos || 0}</td>
                    <td>${g.omitidos || 0}</td>
                    <td class="${g.errores > 0 ? 'error-text' : ''}">${g.errores || 0}</td>
                `;
                gestorTbody.appendChild(row);
            });
        } else {
            gestorTbody.innerHTML = '<tr><td colspan="6">Sin datos</td></tr>';
        }

        // Tabla de proyectos
        const proyectoTable = document.getElementById('reportProyectoTable');
        if (proyectoTable) {
            const proyectoTbody = proyectoTable.querySelector('tbody');
            proyectoTbody.innerHTML = '';
            if (data.proyectos && data.proyectos.length > 0) {
                data.proyectos.forEach((p) => {
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td>${p.gestor}</td>
                        <td>${p.proyecto}</td>
                        <td>${p.total}</td>
                        <td class="${p.errores > 0 ? 'error-text' : ''}">${p.errores || 0}</td>
                    `;
                    proyectoTbody.appendChild(row);
                });
            } else {
                proyectoTbody.innerHTML = '<tr><td colspan="4">Sin datos</td></tr>';
            }
        }
    } catch (err) {
        console.error('Report error:', err);
    }
}

// ============================================================
// BROWSE
// ============================================================

async function browseFolder(inputId) {
    targetInputId = inputId;
    const modal = document.getElementById('browseModal');
    modal.classList.add('active');

    const currentVal = document.getElementById(inputId).value;
    loadPath(currentVal || '');
}

function closeBrowseModal() {
    document.getElementById('browseModal').classList.remove('active');
}

async function loadPath(path) {
    const list = document.getElementById('folderList');
    const pathInput = document.getElementById('currentPath');
    const upBtn = document.getElementById('upBtn');

    list.innerHTML = '<div class="folder-item">Cargando...</div>';

    try {
        const response = await fetch(`/api/browse?path=${encodeURIComponent(path)}`);
        if (!response.ok) throw new Error('No se pudo cargar la ruta');

        const data = await response.json();
        currentBrowsePath = data.current;
        pathInput.value = data.current;

        if (data.parent) {
            upBtn.disabled = false;
            upBtn.onclick = () => loadPath(data.parent);
        } else {
            upBtn.disabled = true;
        }

        list.innerHTML = '';
        data.items.forEach((item) => {
            const div = document.createElement('div');
            div.className = 'folder-item';
            div.innerHTML = `
                <span class="folder-icon" aria-hidden="true">
                    <svg viewBox="0 0 24 24">
                        <path d="M3 7.5A2.5 2.5 0 0 1 5.5 5H9l2 2h7.5A2.5 2.5 0 0 1 21 9.5v7A2.5 2.5 0 0 1 18.5 19h-13A2.5 2.5 0 0 1 3 16.5z"></path>
                    </svg>
                </span>
                <span>${item.name}</span>
            `;
            div.onclick = () => loadPath(item.path);
            list.appendChild(div);
        });
    } catch (err) {
        list.innerHTML = `<div class="folder-item" style="color:var(--err)">Error: ${err.message}</div>`;
    }
}

function selectCurrentFolder() {
    if (targetInputId && currentBrowsePath) {
        document.getElementById(targetInputId).value = currentBrowsePath;
        saveConfig();
        closeBrowseModal();
    }
}

// ============================================================
// AUDIT
// ============================================================

function downloadAudit() {
    const link = document.createElement('a');
    link.href = '/api/audit';
    link.download = 'auditoria_reorganizador.xlsx';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    showToast('Descargando Excel de auditoria...', 'info');
}
