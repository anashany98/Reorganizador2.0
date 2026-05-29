const startBtn = document.getElementById('startBtn');
const stopBtn = document.getElementById('stopBtn');
const previewBtn = document.getElementById('previewBtn');
const realCopyBtn = document.getElementById('realCopyBtn');
const progressBar = document.getElementById('progressBar');
const progressText = document.getElementById('progressText');
const logContent = document.getElementById('logContent');
const statusText = document.getElementById('statusText');
const statusBadge = document.getElementById('statusBadge');

const statProcessed = document.getElementById('statProcessed');
const statSinNumero = document.getElementById('statSinNumero');
const statNoEncontrado = document.getElementById('statNoEncontrado');
const statAmbiguo = document.getElementById('statAmbiguo');
const statAmbiguoSerie = document.getElementById('statAmbiguoSerie');
const statSerieNoEncontrada = document.getElementById('statSerieNoEncontrada');
const statErrors = document.getElementById('statErrors');

let pollingInterval = null;
let reportInterval = null;
let lastLoggedFile = "";
let targetInputId = null;
let currentBrowsePath = "";
let selectedBrowseFilePath = "";
let browseMode = "folder";
let browseFileExtensions = "";
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
    updatePreflight();
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
    const mappingInput = document.getElementById('mappingExcel');

    [sourceInput, destInput, mappingInput].forEach(input => {
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
                            updatePreflight();
                        }
                    } else {
                        // Try to get path from file
                        const file = e.dataTransfer.files[0];
                        if (file && file.path) {
                            input.value = file.path;
                            saveConfig();
                            updatePreflight();
                        }
                    }
                }
            }
        });
    });

    // Also handle the source/dest inputs directly for drop
    [sourceInput, destInput, mappingInput].forEach(input => {
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
                updatePreflight();
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
                startDryRunScan();
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
    if (preset.sources) document.getElementById('sources').value = preset.sources;
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
    updatePreflight();
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
        sources: document.getElementById('sources').value,
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
    const sources = document.getElementById('sources').value;
    const projects = document.getElementById('projectFilter').value;
    const mappingExcel = document.getElementById('mappingExcel').value;
    const years = getSelectedYears().join(',');
    const dest = document.getElementById('dest').value;
    const unmatchedDir = document.getElementById('unmatchedDir').value || '_REVISION';
    const mode = document.getElementById('organizeBy').value;
    if (!source.trim() && !sources.trim()) {
        showToast('Especifica al menos una carpeta origen', 'error');
        return;
    }
    if (mode === 'factusol-client-budget' && !mappingExcel.trim()) {
        showToast('Selecciona el Excel simplificado de FactuSOL', 'error');
        return;
    }
    if (mode === 'factusol-client-budget' && !dest.trim()) {
        showToast('Debes especificar una carpeta destino', 'error');
        return;
    }
    if (mode === 'factusol-client-budget' && getSelectedYears().length === 0) {
        showToast('Selecciona al menos un ano', 'error');
        return;
    }
    const modal = document.getElementById('previewModal');
    const content = document.getElementById('previewContent');
    modal.classList.add('active');
    content.innerHTML = '<div class="preview-loading">Analizando carpeta...</div>';

    try {
        const params = new URLSearchParams({
            source,
            sources,
            projects,
            mapping_excel: mappingExcel,
            years,
            dest,
            unmatched_dir: unmatchedDir,
            extensions: buildExtensionsString(),
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
    const ambiguosTotal = (counters.AMBIGUO || 0) + (counters.AMBIGUO_SERIE || 0) + (counters.SERIE_NO_ENCONTRADA || 0);
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
            <span class="preview-stat-num">${ambiguosTotal.toLocaleString()}</span>
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
                            <th>Serie</th>
                            <th>ClaveApp</th>
                            <th>Cliente</th>
                            <th>MatchStatus</th>
                            <th>Confianza</th>
                            <th>Ruta destino</th>
                        </tr>
                    </thead>
                    <tbody>`;
        data.items.forEach((item) => {
            const statusClass = item.match_status?.startsWith('OK_') ? 'ok' : 
                               item.match_status?.includes('AMBIGUO') ? 'warn' : 
                               item.match_status?.includes('SERIE') ? 'warn' : '';
            html += `<tr>
                <td title="${escapeHtml(item.src_path || '')}">${escapeHtml(item.file_name || '')}</td>
                <td>${escapeHtml(item.presupuesto_detectado || '')}</td>
                <td>${escapeHtml(item.serie_excel || 'GENERAL')}</td>
                <td title="${escapeHtml(item.clave_app || '')}">${escapeHtml(item.clave_app || '')}</td>
                <td>${escapeHtml(item.cliente || '')}</td>
                <td class="${statusClass}">${escapeHtml(item.match_status || '')}</td>
                <td>${escapeHtml(String(item.match_confidence ? (item.match_confidence * 100).toFixed(0) + '%' : ''))}</td>
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
    if (saved.sources) document.getElementById('sources').value = saved.sources;
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
        sources: document.getElementById('sources').value,
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

['source', 'sources', 'dest', 'mappingExcel', 'year2025', 'year2026', 'organizeBy', 'moveFiles', 'dryRun', 'minSize', 'extensions', 'projectFilter', 'unmatchedDir', 'requireBudgetMatch', 'threads', 'processes', 'conflict', 'dedup'].forEach((id) => {
    const element = document.getElementById(id);
    if (element) {
        element.addEventListener('change', () => {
            saveConfig();
            updatePreflight();
        });
        element.addEventListener('input', () => {
            saveConfig();
            updatePreflight();
        });
    }
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

function updatePreflight() {
    const panel = document.getElementById('preflightPanel');
    const list = document.getElementById('preflightItems');
    if (!panel || !list) return;

    const mode = document.getElementById('organizeBy').value;
    const hasSource = Boolean(
        document.getElementById('source').value.trim() ||
        document.getElementById('sources').value.trim()
    );
    const checks = [
        { ok: hasSource, text: 'Al menos un origen seleccionado' },
    ];
    if (mode === 'factusol-client-budget') {
        checks.push(
            { ok: Boolean(document.getElementById('dest').value.trim()), text: 'Destino seleccionado' },
            { ok: Boolean(document.getElementById('mappingExcel').value.trim()), text: 'Excel FactuSOL seleccionado' },
            { ok: getSelectedYears().length > 0, text: 'Al menos un ano marcado' },
        );
    } else if (document.getElementById('dest').value.trim()) {
        checks.push({ ok: true, text: 'Destino seleccionado' });
    }

    const ready = checks.every((item) => item.ok);
    panel.dataset.ready = ready ? 'true' : 'false';
    panel.querySelector('.preflight-title').textContent = ready
        ? 'Listo para previsualizar'
        : 'Faltan datos para ejecutar';
    list.innerHTML = checks.map((item) => `
        <div class="preflight-item ${item.ok ? 'ok' : 'pending'}">
            <span>${item.ok ? 'OK' : '--'}</span>
            <span>${item.text}</span>
        </div>
    `).join('');
    startBtn.disabled = !ready;
    previewBtn.disabled = mode === 'factusol-client-budget' ? !ready : !hasSource;
    realCopyBtn.disabled = !ready;
}

// ============================================================
// SCAN
// ============================================================

async function buildExtensionsString() {
    const parts = [];
    if (document.getElementById('typePdf').checked) parts.push('pdf');
    if (document.getElementById('typeExcel').checked) parts.push('xls', 'xlsx', 'xlsm');
    if (document.getElementById('typeWord').checked) parts.push('doc', 'docx');
    if (document.getElementById('typeImage').checked) parts.push('jpg', 'jpeg', 'png', 'tif', 'tiff', 'bmp', 'gif', 'webp', 'heic');
    if (document.getElementById('typeMail').checked) parts.push('msg', 'eml', 'pst', 'ost');
    if (document.getElementById('typeCad').checked) parts.push('dwg', 'dxf', 'skp', 'rvt', 'ifc', 'pln', '3dm');
    if (document.getElementById('typeZip').checked) parts.push('zip', 'rar', '7z');
    if (document.getElementById('typeVideo').checked) parts.push('mp4', 'avi', 'mkv', 'mov', 'wmv', 'flv', 'webm');
    if (document.getElementById('typeAudio').checked) parts.push('mp3', 'wav', 'flac', 'aac', 'ogg', 'wma', 'm4a');
    // typeOther means we include files without recognized extensions
    const extra = document.getElementById('extensions').value.trim();
    if (extra) parts.push(...extra.split(',').map(e => e.trim().replace(/^\./, '')));
    return parts.join(',');
}

async function startScan() {
    saveConfig();
    lastLoggedFile = "";
    resetProgress();

    const config = {
        source: document.getElementById('source').value,
        sources: document.getElementById('sources').value,
        dest: document.getElementById('dest').value,
        organize_by: document.getElementById('organizeBy').value,
        mapping_excel: document.getElementById('mappingExcel').value,
        years: getSelectedYears().join(','),
        unmatched_dir: document.getElementById('unmatchedDir').value || '_REVISION',
        require_budget_match: document.getElementById('requireBudgetMatch').checked,
        move: document.getElementById('moveFiles').checked,
        dry_run: document.getElementById('dryRun').checked,
        min_size_mb: parseFloat(document.getElementById('minSize').value) || 0,
        extensions: buildExtensionsString(),
        project_filter: document.getElementById('projectFilter').value,
        threads: parseInt(document.getElementById('threads').value) || 0,
        processes: parseInt(document.getElementById('processes').value) || 0,
        conflict: document.getElementById('conflict').value,
        dedup: document.getElementById('dedup').checked,
    };

    if (!config.source.trim() && !config.sources.trim()) {
        showToast('Debes especificar al menos una carpeta origen', 'error');
        setRuntimeState('error', 'Falta origen');
        return;
    }
    if (config.organize_by === 'factusol-client-budget' && getSelectedYears().length === 0) {
        showToast('Selecciona al menos un ano', 'error');
        setRuntimeState('error', 'Falta ano');
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
            let detail = 'Error en el servidor';
            try {
                const errBody = await response.json();
                if (Array.isArray(errBody.detail)) {
                    detail = errBody.detail.map(e => e.msg || JSON.stringify(e)).join(', ');
                } else if (typeof errBody.detail === 'object' && errBody.detail !== null) {
                    detail = JSON.stringify(errBody.detail);
                } else {
                    detail = String(errBody.detail || 'Error ' + response.status);
                }
            } catch (_) {}
            throw new Error(detail);
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
        const msg = err instanceof Error ? err.message : String(err);
        console.error('Scan error:', err);
        showToast(msg, 'error');
        setLoading(false);
    }
}

function startDryRunScan() {
    document.getElementById('dryRun').checked = true;
    saveConfig();
    startScan();
}

function startRealCopy() {
    if (!confirm('Vas a copiar archivos al destino seleccionado. Ejecuta primero una previsualizacion o simulacion si tienes dudas.')) {
        return;
    }
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
        statAmbiguo.innerText = (matchCounters.AMBIGUO || 0) + (matchCounters.AMBIGUO_SERIE || 0);
        statAmbiguoSerie.innerText = matchCounters.AMBIGUO_SERIE || 0;
        statSerieNoEncontrada.innerText = matchCounters.SERIE_NO_ENCONTRADA || 0;
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
        if (realCopyBtn) realCopyBtn.classList.add('hidden');
        stopBtn.classList.remove('hidden');
        stopBtn.disabled = false;
    } else {
        startBtn.classList.remove('hidden');
        if (previewBtn) previewBtn.classList.remove('hidden');
        if (realCopyBtn) realCopyBtn.classList.remove('hidden');
        stopBtn.classList.add('hidden');
        updatePreflight();
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
    tbody.innerHTML = '<tr><td colspan="7">Cargando...</td></tr>';

    try {
        const response = await fetch(`/api/history?page=${page}&page_size=50`);
        const data = await response.json();

        tbody.innerHTML = '';
        if (data.items.length === 0) {
            tbody.innerHTML = '<tr><td colspan="7">No hay historial reciente</td></tr>';
            renderPagination(0, page, 50);
            return;
        }

        data.items.forEach((item) => {
            const row = document.createElement('tr');
            const makeCell = (value, className = '') => {
                const cell = document.createElement('td');
                cell.textContent = value || '';
                cell.title = value || '';
                if (className) cell.className = className;
                return cell;
            };

            const status = item.match_status || item.action_status || item.action || '';
            const statusCell = makeCell(status);
            if (item.action_status === 'error' || ['AMBIGUO', 'NO_ENCONTRADO_EN_EXCEL'].includes(status)) {
                statusCell.classList.add('error-text');
            }

            const copyCell = document.createElement('td');
            const copyButton = document.createElement('button');
            copyButton.className = 'copy-btn';
            copyButton.type = 'button';
            copyButton.title = 'Copiar ruta';
            copyButton.innerHTML = '<svg viewBox="0 0 16 16"><rect x="5" y="5" width="9" height="9" rx="1"/><path d="M3 11V3a1 1 0 0 1 1-1h8"/></svg>';
            copyButton.addEventListener('click', () => copyPath(copyButton, item.dst_path || item.src_path || ''));
            copyCell.appendChild(copyButton);

            row.append(
                makeCell(item.created_time || ''),
                makeCell(item.file_name || ''),
                makeCell(item.presupuesto_detectado || item.proyecto || ''),
                makeCell(item.cliente || item.gestor || ''),
                statusCell,
                makeCell(item.dst_path || '', 'path-cell'),
                copyCell,
            );
            tbody.appendChild(row);
        });

        renderPagination(data.total, page, data.page_size);
    } catch (err) {
        showToast('Error al cargar historial', 'error');
        tbody.innerHTML = `<tr><td colspan="7" class="error-text">Error: ${err.message}</td></tr>`;
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
    openBrowseModal(inputId, 'folder');
}

async function browseFile(inputId, fileExtensions = '') {
    openBrowseModal(inputId, 'file', fileExtensions);
}

function openBrowseModal(inputId, mode, fileExtensions = '') {
    targetInputId = inputId;
    browseMode = mode;
    browseFileExtensions = fileExtensions;
    selectedBrowseFilePath = mode === 'file' ? document.getElementById(inputId).value.trim() : "";

    const modal = document.getElementById('browseModal');
    const title = modal.querySelector('.modal-bar h2');
    const selectBtn = document.getElementById('selectCurrentBtn');
    title.textContent = mode === 'file' ? 'Seleccionar archivo' : 'Seleccionar carpeta';
    selectBtn.textContent = mode === 'file' ? 'Usar archivo seleccionado' : 'Usar esta carpeta';
    selectBtn.disabled = mode === 'file' && !selectedBrowseFilePath;
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
        const params = new URLSearchParams({
            path,
            include_files: browseMode === 'file' ? 'true' : 'false',
            file_extensions: browseFileExtensions,
        });
        const response = await fetch(`/api/browse?${params.toString()}`);
        if (!response.ok) throw new Error('No se pudo cargar la ruta');

        const data = await response.json();
        currentBrowsePath = data.current;
        pathInput.value = selectedBrowseFilePath || data.current;
        updateBrowseSelectButton();

        if (data.parent) {
            upBtn.disabled = false;
            upBtn.onclick = () => loadPath(data.parent);
        } else {
            upBtn.disabled = true;
        }

        list.innerHTML = '';
        if (data.items.length === 0) {
            list.innerHTML = '<div class="folder-item empty">No hay elementos disponibles</div>';
            return;
        }
        data.items.forEach((item) => {
            const div = document.createElement('div');
            const isFile = item.type === 'file';
            div.className = `folder-item ${isFile ? 'file-item' : 'dir-item'} ${item.path === selectedBrowseFilePath ? 'selected' : ''}`;
            div.innerHTML = `
                <span class="folder-icon" aria-hidden="true">
                    ${isFile
                        ? '<svg viewBox="0 0 24 24"><path d="M6 3h8l4 4v14H6z"></path><path d="M14 3v5h4"></path></svg>'
                        : '<svg viewBox="0 0 24 24"><path d="M3 7.5A2.5 2.5 0 0 1 5.5 5H9l2 2h7.5A2.5 2.5 0 0 1 21 9.5v7A2.5 2.5 0 0 1 18.5 19h-13A2.5 2.5 0 0 1 3 16.5z"></path></svg>'
                    }
                </span>
                <span>${escapeHtml(item.name)}</span>
                <span class="folder-meta">${isFile ? 'archivo' : 'carpeta'}</span>
            `;
            div.onclick = () => {
                if (isFile) {
                    selectedBrowseFilePath = item.path;
                    pathInput.value = item.path;
                    document.querySelectorAll('.folder-item.selected').forEach((node) => node.classList.remove('selected'));
                    div.classList.add('selected');
                    updateBrowseSelectButton();
                } else {
                    if (browseMode === 'file') selectedBrowseFilePath = "";
                    loadPath(item.path);
                }
            };
            list.appendChild(div);
        });
    } catch (err) {
        list.innerHTML = `<div class="folder-item" style="color:var(--err)">Error: ${err.message}</div>`;
    }
}

function updateBrowseSelectButton() {
    const selectBtn = document.getElementById('selectCurrentBtn');
    if (!selectBtn) return;
    selectBtn.disabled = browseMode === 'file' ? !selectedBrowseFilePath : !currentBrowsePath;
}

function selectCurrentFolder() {
    const selectedPath = browseMode === 'file' ? selectedBrowseFilePath : currentBrowsePath;
    if (targetInputId && selectedPath) {
        document.getElementById(targetInputId).value = selectedPath;
        saveConfig();
        updatePreflight();
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
