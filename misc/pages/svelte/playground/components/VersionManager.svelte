<script>
    import { onMount } from 'svelte';
    import { slide } from 'svelte/transition';
    import { cubicInOut, cubicOut } from 'svelte/easing';
    import { storeLocalVersion, getAllLocalVersions } from '../db.js';
    import { setStatus, showError, jsStore, wasmModule, activeVersionMetadata, engineSettings, reloadStoreTrigger, expandedStatusSection, isEngineInitializing, localVersions } from '../store.js';
    import EngineSettings from './EngineSettings.svelte';
    import { OFFICIAL_VERSIONS, ensureOfficialVersionDownloaded, redownloadOfficialVersion, activateVersion } from '../engine.js';

    const versions = OFFICIAL_VERSIONS;

    let selectedVersion = $state("");
    let draftVersion = $state("");
    let draftSettings = $state({
        memoryLimit: 1024,
        metrics: {
            showMetrics: true,
            showStatistics: false
        },
        dataFusion: {
            enableDynamicFilterPushdown: true
        },
        rdfFusion: {
            enableDataCache: true,
            dataCacheBlockSizeKb: 2048,
            dataCacheNumBlocks: 1024
        },
        customConfig: []
    });

    let isDownloading = $state(false);
    let isUploading = $state(false);

    // Custom Version Upload states
    let customVersionName = $state("");
    let customJsFile = $state(null);
    let customWasmFile = $state(null);
    let customUploadError = $state("");

    let officialVersions = $derived(versions.map(v => {
        const local = $localVersions.find(c => c.id === v.id);
        const size = local ? (local.size ?? (local.wasmBlob?.size ? local.wasmBlob.size + (local.jsBlob?.size || 0) : null)) : null;
        return {
            ...v,
            isDownloaded: !!local,
            size
        };
    }));
    
    let uploadedVersions = $derived($localVersions.filter(c => c.customName));

    let latestUpdateAvailable = $state(false);
    let errorModalMsg = $state("");

    let draftIsLocal = $derived($localVersions.some(v => v.id === draftVersion));
    let hasSettingsChanges = $derived(JSON.stringify(draftSettings) !== JSON.stringify($engineSettings));
    let hasChanges = $derived(draftVersion !== selectedVersion || hasSettingsChanges);
    let needsDownload = $derived(!draftIsLocal && draftVersion && versions.some(v => v.id === draftVersion));

    let isApplyDisabled = $derived(
        isDownloading || 
        isUploading ||
        !draftVersion || 
        (!hasChanges && !needsDownload && selectedVersion !== '' && $wasmModule !== null)
    );

    function syncDraftState() {
        draftVersion = selectedVersion;
        customUploadError = "";
        if ($engineSettings) {
            draftSettings = JSON.parse(JSON.stringify($engineSettings));
        }
    }

    $effect(() => {
        if ($expandedStatusSection === 'engine') {
            syncDraftState();
        }
    });

    function resetDraftSettingsDefaults() {
        draftSettings = {
            memoryLimit: 1024,
            metrics: {
                showMetrics: true,
                showStatistics: false
            },
            dataFusion: {
                enableDynamicFilterPushdown: true
            },
            rdfFusion: {
                enableDataCache: true,
                dataCacheBlockSizeKb: 2048,
                dataCacheNumBlocks: 1024
            },
            customConfig: []
        };
    }

    async function handleApplyOrDownload() {
        if (needsDownload) {
            await downloadActiveVersion();
            if (!$localVersions.some(v => v.id === draftVersion)) {
                return;
            }
        }
        await acceptEngineChanges();
    }

    async function acceptEngineChanges() {
        const versionChanged = (selectedVersion !== draftVersion || !$wasmModule);
        const settingsChanged = hasSettingsChanges;

        selectedVersion = draftVersion;
        $engineSettings = JSON.parse(JSON.stringify(draftSettings));
        $expandedStatusSection = null;

        if (versionChanged) {
            await handleVersionSelect();
        } else if (settingsChanged) {
            reloadStoreTrigger.update(n => n + 1);
        }
    }

    onMount(async () => {
        syncDraftState();

        const engineModalEl = document.getElementById('engineModal');
        if (engineModalEl) {
            engineModalEl.addEventListener('show.bs.modal', syncDraftState);
            engineModalEl.addEventListener('hidden.bs.modal', syncDraftState);
        }

        window.showErrorModal = (msg) => {
            errorModalMsg = msg;
            const modalEl = document.getElementById('errorModal');
            if (modalEl) {
                const modal = window.bootstrap.Modal.getInstance(modalEl) || new window.bootstrap.Modal(modalEl);
                modal.show();
            }
        };

        await updateLocalVersionsModal();
        
        const lastVersion = localStorage.getItem('lastRdfFusionVersion');
        let isValid = false;
        
        if (lastVersion) {
            isValid = versions.some(v => v.id === lastVersion) || $localVersions.some(v => v.id === lastVersion);
        }

        if (isValid) {
            selectedVersion = lastVersion;
            draftVersion = lastVersion;
            $isEngineInitializing = true;
            await handleVersionSelect();
            $isEngineInitializing = false;
        } else if (versions.length > 0) {
            selectedVersion = versions[0].id;
            draftVersion = versions[0].id;
            $isEngineInitializing = true;
            await handleVersionSelect();
            $isEngineInitializing = false;
        } else if (selectedVersion) {
            draftVersion = selectedVersion;
            $isEngineInitializing = true;
            await handleVersionSelect();
            $isEngineInitializing = false;
        }
    });

    async function handleVersionSelect() {
        $jsStore = null;
        $wasmModule = null;
        $activeVersionMetadata = null;

        if (!selectedVersion) {
            setStatus('Ready. Select a version to begin.', 'fa-hand-pointer', 'brown');
            localStorage.removeItem('lastRdfFusionVersion');
            return;
        }

        const { hasUpdate } = await activateVersion(selectedVersion, { localVersions: $localVersions });
        latestUpdateAvailable = hasUpdate;
    }

    async function downloadActiveVersion() {
        const vInfo = versions.find(v => v.id === draftVersion);
        if (!vInfo) return;

        isDownloading = true;

        try {
            await ensureOfficialVersionDownloaded(draftVersion);
            await updateLocalVersionsModal();
            latestUpdateAvailable = false;
        } catch (e) {
            console.error(e);
            showError(e.message);
        } finally {
            isDownloading = false;
        }
    }

    // The active version may already be present locally, so the normal Apply/Download path
    // (keyed on `needsDownload`) would skip the fetch and just close the pane. "Update Now"
    // must force a fresh download that overwrites the cached copy, then re-initialize.
    async function handleUpdateNow() {
        isDownloading = true;
        try {
            await redownloadOfficialVersion(draftVersion);
            await updateLocalVersionsModal();
            latestUpdateAvailable = false;
            $expandedStatusSection = null;
            await handleVersionSelect();
            setStatus('Engine updated to the latest build.', 'fa-circle-check', 'success');
        } catch (e) {
            console.error(e);
            showError(e.message || String(e));
        } finally {
            isDownloading = false;
        }
    }

    async function updateLocalVersionsModal() {
        $localVersions = await getAllLocalVersions();
    }

    async function loadCustomVersion() {
        customUploadError = "";
        if (!customJsFile || customJsFile.length === 0 || !customWasmFile || customWasmFile.length === 0 || !customVersionName.trim()) {
            customUploadError = "Please provide a name and select both the .js and .wasm files.";
            return;
        }

        isUploading = true;
        try {
            const localData = {
                jsBlob: new Blob([await customJsFile[0].arrayBuffer()], {type: 'application/javascript'}),
                wasmBlob: new Blob([await customWasmFile[0].arrayBuffer()], {type: 'application/wasm'})
            };

            const customId = `uploaded-${Date.now()}`;
            const name = customVersionName.trim();
            
            await storeLocalVersion(customId, localData.jsBlob, localData.wasmBlob, name);
            await updateLocalVersionsModal();

            draftVersion = customId;
            customVersionName = "";
            customJsFile = null;
            customWasmFile = null;

            const modalEl = document.getElementById('customEngineModal');
            if (modalEl) {
                const modal = window.bootstrap.Modal.getInstance(modalEl);
                modal?.hide();
            }

            setStatus(`Custom build '${name}' ready. Click Apply to load.`, 'fa-circle-check', 'success');
        } catch (e) {
            console.error(e);
            customUploadError = "Failed to upload build: " + (e.message || String(e));
        } finally {
            isUploading = false;
        }
    }
</script>

{#if $expandedStatusSection === 'engine'}
    <div class="border-top bg-white overflow-hidden">
        <div in:slide={{ duration: 220, easing: cubicOut }} out:slide={{ duration: 160, easing: cubicInOut }} class="p-4 d-flex flex-column gap-3">
        <div class="d-flex align-items-center justify-content-between border-bottom pb-2">
            <h6 class="pane-heading mb-0 text-dark d-flex align-items-center gap-2">
                <i class="fa-solid fa-microchip text-brown"></i> Engine Configuration
            </h6>
            <button type="button" class="btn-close small" aria-label="Close" onclick={() => $expandedStatusSection = null}></button>
        </div>

        <!-- Version Select row — constrained width, no full-stretch -->
        <div class="d-flex align-items-center gap-2 flex-wrap">
            <label for="versionSelectDropdown" class="fw-bold text-nowrap mb-0" style="min-width: 80px;">Version:</label>
            <select id="versionSelectDropdown" bind:value={draftVersion} disabled={isDownloading || isUploading} class="form-select form-select-sm bg-white" style="max-width: 320px;">
                <option value="">Select a version...</option>
                <optgroup label="Official Releases">
                    {#each officialVersions as v (v.id)}
                        <option value={v.id}>{v.name}</option>
                    {/each}
                </optgroup>
                {#if uploadedVersions.length > 0}
                    <optgroup label="Uploaded Builds">
                        {#each uploadedVersions as v (v.id)}
                            <option value={v.id}>{v.customName}</option>
                        {/each}
                    </optgroup>
                {/if}
            </select>
            {#if draftIsLocal && draftVersion}
                <span class="text-success" title="Downloaded (Available locally)" style="font-size: 1.1rem; cursor: default;">
                    <i class="fa-solid fa-hard-drive"></i>
                </span>
            {:else if needsDownload}
                <span class="text-brown" title="Build not locally available — will be downloaded on Apply" style="font-size: 1.1rem; cursor: help;">
                    <i class="fa-solid fa-cloud-arrow-down"></i>
                </span>
            {/if}
        </div>

        {#if latestUpdateAvailable}
            <div class="p-2 bg-light border rounded text-center">
                <div class="small text-success fw-bold d-flex align-items-center justify-content-center gap-2">
                    <i class="fa-solid fa-arrows-rotate"></i> A newer build of this version was released!
                    <button class="btn btn-sm btn-primary py-0 px-2" onclick={handleUpdateNow} disabled={isDownloading}>Update Now</button>
                </div>
            </div>
        {/if}

        <!-- Inner Collapsible Engine Settings & Tuning Sections -->
        <EngineSettings bind:settings={draftSettings} />

        <!-- Inline Actions -->
        <div class="d-flex justify-content-between align-items-center flex-wrap gap-2 pt-2 border-top">
            <div class="d-flex gap-2">
                <button type="button" class="btn btn-sm btn-outline-secondary" onclick={resetDraftSettingsDefaults} disabled={isDownloading || isUploading}>
                    <i class="fa-solid fa-rotate-left me-1"></i> Reset to Defaults
                </button>
                <button 
                    type="button" 
                    class="btn btn-sm btn-outline-primary d-flex align-items-center gap-2"
                    data-bs-toggle="modal"
                    data-bs-target="#customEngineModal"
                    title="Upload custom local WASM build">
                    <i class="fa-solid fa-upload"></i>
                    <span>Upload Build</span>
                </button>
            </div>
            <div class="d-flex gap-2">
                <button type="button" class="btn btn-sm btn-secondary px-3" onclick={() => $expandedStatusSection = null} disabled={isDownloading || isUploading}>Close</button>
                <button 
                    type="button" 
                    class="btn btn-sm btn-primary px-3 fw-semibold d-flex align-items-center gap-2" 
                    onclick={handleApplyOrDownload} 
                    disabled={isApplyDisabled}>
                    {#if isDownloading}
                        <i class="fa-solid fa-spinner fa-spin"></i>
                        <span>Downloading...</span>
                    {:else if needsDownload}
                        <i class="fa-solid fa-download"></i>
                        <span>Download & Apply</span>
                    {:else}
                        <i class="fa-solid fa-check"></i>
                        <span>Apply</span>
                    {/if}
                </button>
            </div>
        </div>
    </div>
    </div>
{/if}

<!-- Dedicated Upload Custom WASM Build Modal -->
<div class="modal fade" id="customEngineModal" tabindex="-1" aria-labelledby="customEngineModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-lg modal-dialog-scrollable">
        <div class="modal-content border-0 shadow">
            <div class="modal-header bg-light border-bottom py-3 px-4 d-flex align-items-center justify-content-between">
                <div class="d-flex align-items-center gap-2">
                    <i class="fa-solid fa-upload text-brown fs-5"></i>
                    <h5 class="modal-title pane-heading mb-0" id="customEngineModalLabel">Upload Custom WASM Build</h5>
                </div>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>
            
            <div class="modal-body p-4 d-flex flex-column gap-3">
                {#if customUploadError}
                    <div class="alert alert-danger py-2 px-3 small d-flex align-items-center gap-2 mb-0">
                        <i class="fa-solid fa-circle-exclamation flex-shrink-0"></i>
                        <span>{customUploadError}</span>
                    </div>
                {/if}

                <div class="card card-body bg-light small border-info text-dark py-2 px-3">
                    <p class="mb-1 fw-bold"><i class="fa-solid fa-terminal me-1"></i> How to build locally:</p>
                    <code class="text-dark bg-white px-2 py-1 rounded border mb-1 d-block">wasm-pack build --target web --release lib/wasm</code>
                    <p class="mb-0 text-muted" style="font-size: 0.75rem;">
                        Select the generated <code>pkg/rdf_fusion_wasm.js</code> and <code>pkg/rdf_fusion_wasm_bg.wasm</code> files below.
                    </p>
                </div>
                
                <div>
                    <label for="customVersionName" class="form-label small fw-bold">Build Name <span class="text-danger">*</span></label>
                    <input id="customVersionName" type="text" class="form-control form-control-sm" bind:value={customVersionName} placeholder="e.g. Local Dev Branch">
                </div>

                <div class="row g-3">
                    <div class="col-md-6">
                        <label for="customJsFile" class="form-label small fw-bold">RDF Fusion JS (.js) <span class="text-danger">*</span></label>
                        <input id="customJsFile" class="form-control form-control-sm" type="file" bind:files={customJsFile} accept=".js">
                    </div>
                    <div class="col-md-6">
                        <label for="customWasmFile" class="form-label small fw-bold">RDF Fusion WASM (.wasm) <span class="text-danger">*</span></label>
                        <input id="customWasmFile" class="form-control form-control-sm" type="file" bind:files={customWasmFile} accept=".wasm">
                    </div>
                </div>
            </div>

            <div class="modal-footer bg-light border-top py-2 px-4 d-flex justify-content-end align-items-center gap-2">
                <button type="button" class="btn btn-sm btn-secondary px-3" data-bs-dismiss="modal" disabled={isUploading}>Cancel</button>
                <button 
                    type="button" 
                    class="btn btn-sm btn-primary px-3 fw-semibold d-flex align-items-center gap-2" 
                    onclick={loadCustomVersion} 
                    disabled={isUploading || !customVersionName.trim() || !customJsFile || customJsFile.length === 0 || !customWasmFile || customWasmFile.length === 0}>
                    {#if isUploading}
                        <i class="fa-solid fa-spinner fa-spin"></i> Uploading...
                    {:else}
                        <i class="fa-solid fa-check"></i> Upload & Select
                    {/if}
                </button>
            </div>
        </div>
    </div>
</div>

<!-- Error Details Modal -->
<div class="modal fade" id="errorModal" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog">
    <div class="modal-content">
      <div class="modal-header bg-danger text-white border-bottom-0">
        <h5 class="modal-title"><i class="fa-solid fa-triangle-exclamation me-2"></i> Error Details</h5>
        <button type="button" class="btn-close btn-close-white" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body">
        <p class="mb-0 text-break font-monospace small">{errorModalMsg}</p>
      </div>
      <div class="modal-footer border-top-0">
        <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Close</button>
      </div>
    </div>
  </div>
</div>
