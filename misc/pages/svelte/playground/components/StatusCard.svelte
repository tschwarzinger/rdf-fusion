<script>
    import { onMount } from 'svelte';
    import { jsStore, wasmModule, activeVersionMetadata, activeDatasetMetadata, queryResults, isDatasetLoading, isEngineInitializing, setStatus, expandedStatusSection } from '../store.js';
    import { ensureOfficialVersionDownloaded, activateVersion, OFFICIAL_VERSIONS } from '../engine.js';
    import { getLocalVersion } from '../db.js';
    import VersionManager from './VersionManager.svelte';
    import DatasetSelector from './DatasetSelector.svelte';
    import ClearAllModal from './ClearAllModal.svelte';
    import ManageDownloadsModal from './ManageDownloadsModal.svelte';

    let isEngineReady = $derived($wasmModule !== null);
    let isDatasetSelected = $derived($activeDatasetMetadata !== null);
    let isDatasetBusy = $derived($isDatasetLoading);
    let isDatasetReady = $derived($jsStore !== null);
    let isQueryExecuting = $derived($queryResults?.isExecuting ?? false);

    let isQuickConfiguring = $state(false);
    let hasStoredState = $state(false);
    let isRecreating = $state(false);

    // True when the engine worker has died after having been active earlier in
    // this session (wasmModule cleared by onCrash, but version metadata and the
    // stored version remain). Lets the Recreate button stay available to restart
    // the engine instead of disappearing along with the dataset store.
    let isEngineCrashed = $derived(!isEngineReady && $activeVersionMetadata !== null && !$isEngineInitializing);
    // Show Recreate while running normally, or when the engine has crashed so the
    // user can recover without re-selecting anything manually.
    let showRecreate = $derived(isEngineCrashed || (isDatasetReady && !isDatasetBusy && !isQueryExecuting));

    onMount(() => {
        const storedVer = localStorage.getItem('lastRdfFusionVersion');
        const storedDs = localStorage.getItem('rdfFusionLastDataset');
        hasStoredState = Boolean(storedVer || storedDs);
    });

    let showQuickConfigure = $derived(!hasStoredState && !isEngineReady && !isDatasetSelected);

    async function handleQuickConfigure() {
        isQuickConfiguring = true;
        setStatus('Setting up stable WASM release & BSBM 10000 dataset...', 'fa-cog fa-spin', 'brown');
        try {
            const targetVersionId = "initial";
            const targetDatasetId = "bsbm-10000-parquet";

            // 1. Ensure the stable release is available locally.
            const localVer = await ensureOfficialVersionDownloaded(targetVersionId);

            // 2. Record the dataset selection, then activate the engine via the shared
            //    engine.js path (same as selecting it in the UI). This also triggers the
            //    dataset store (re-)creation for BSBM 10000 via reloadStoreTrigger.
            localStorage.setItem('rdfFusionLastDataset', targetDatasetId);
            await activateVersion(targetVersionId, { localVersion: localVer });
            if ($wasmModule === null) {
                throw new Error("Engine did not initialize.");
            }

            setStatus('Quick Start complete! Initial release & BSBM 10000 loaded.', 'fa-circle-check', 'green');
        } catch (e) {
            console.error("Quick configure failed:", e);
            setStatus('Quick configure failed: ' + (e.message || String(e)), 'fa-triangle-exclamation', 'danger');
        } finally {
            isQuickConfiguring = false;
        }
    }

    function toggleSection(section) {
        $expandedStatusSection = $expandedStatusSection === section ? null : section;
    }

    // Re-initializes the engine + dataset (creates the RDF Fusion instance again).
    // This is the manual recovery path after the engine worker crashes.
    async function handleRecreateInstance() {
        const storedVer = localStorage.getItem('lastRdfFusionVersion');
        if (!storedVer) {
            setStatus('No engine selected yet. Please select an engine first.', 'fa-info-circle', 'info');
            return;
        }
        let localVer = await getLocalVersion(storedVer);
        if (!localVer && OFFICIAL_VERSIONS.some(v => v.id === storedVer)) {
            localVer = await ensureOfficialVersionDownloaded(storedVer);
        }
        if (!localVer) {
            setStatus('Engine build not found. Please re-select an engine.', 'fa-triangle-exclamation', 'warning');
            return;
        }
        isRecreating = true;
        setStatus('Recreating RDF Fusion instance...', 'fa-cog fa-spin', 'brown');
        try {
            await activateVersion(storedVer, { localVersion: localVer });
            setStatus('RDF Fusion instance recreated.', 'fa-circle-check', 'green');
        } catch (e) {
            console.error("Recreate instance failed:", e);
            setStatus('Recreate failed: ' + (e.message || String(e)), 'fa-triangle-exclamation', 'danger');
        } finally {
            isRecreating = false;
        }
    }

    // Compute actionable guidance for the user
    let guidance = $derived.by(() => {
        if (!isEngineReady) {
            return {
                step: 1,
                title: "Select Engine Version",
                detail: "Choose an official build or upload a custom WASM query engine in Engine Configuration.",
                badgeClass: "bg-warning text-dark",
                icon: "fa-microchip"
            };
        }
        if (!isDatasetSelected && !isDatasetBusy) {
            return {
                step: 2,
                title: "Select Dataset",
                detail: "Pick a dataset from the Library or upload a custom RDF / Parquet dataset.",
                badgeClass: "bg-info text-dark",
                icon: "fa-database"
            };
        }
        if (isDatasetBusy) {
            return {
                step: 3,
                title: "Create RDF Fusion Instance",
                detail: "Initializing the query engine instance and building the query store with the selected dataset.",
                badgeClass: "bg-info text-dark",
                icon: "fa-spinner fa-spin"
            };
        }
        if (isQueryExecuting) {
            return {
                step: 3,
                title: "Running Query...",
                detail: "The RDF Fusion query engine is currently processing your SPARQL query.",
                badgeClass: "bg-primary text-white",
                icon: "fa-spinner fa-spin"
            };
        }
        return {
            step: 3,
            title: "Ready to Query",
            detail: "Compose a SPARQL query in the editor below and press Run Query.",
            badgeClass: "bg-success text-white",
            icon: "fa-circle-check"
        };
    });
</script>

<div class="card shadow-sm border-0 rounded-3 mb-3 bg-white overflow-hidden">
    <!-- Top Bar with Guidance / Action Prompt -->
    <div class="p-3 border-bottom d-flex flex-column flex-md-row justify-content-between align-items-start align-items-md-center gap-2 bg-light-subtle">
        <div class="d-flex align-items-center gap-3">
            <div class="rounded-circle d-flex align-items-center justify-content-center flex-shrink-0"
                 style="width: 40px; height: 40px; background-color: {isEngineReady && isDatasetReady ? '#e8f5e9' : '#fff3e0'}; color: {isEngineReady && isDatasetReady ? '#2e7d32' : '#e65100'};">
                <i class="fa-solid {guidance.icon} fs-5"></i>
            </div>
            <div>
                <div class="d-flex align-items-center gap-2">
                    <span class="badge {guidance.badgeClass} rounded-pill px-2 py-1" style="font-size: 0.75rem;">
                        Step {guidance.step} of 3
                    </span>
                    <strong class="text-dark" style="font-size: 0.95rem;">{guidance.title}</strong>
                </div>
                <div class="text-muted small mt-1">{guidance.detail}</div>
            </div>
        </div>
        <div class="d-flex align-items-center gap-2 align-self-stretch align-self-md-auto justify-content-end flex-wrap">
            <button type="button" class="btn btn-sm btn-outline-secondary d-flex align-items-center gap-2"
                    data-bs-toggle="modal" data-bs-target="#manageDownloadsModal"
                    title="Manage downloaded engine builds, custom datasets, and library data stored locally">
                <i class="fa-solid fa-hard-drive"></i>
                <span>Manage Local Data</span>
            </button>
            {#if showRecreate}
                <button type="button" class="btn btn-sm btn-outline-secondary d-flex align-items-center gap-2"
                        onclick={handleRecreateInstance}
                        disabled={isRecreating}
                        title="Recreate the RDF Fusion engine instance (e.g. after a crash)">
                    {#if isRecreating}
                        <i class="fa-solid fa-spinner fa-spin"></i>
                    {:else}
                        <i class="fa-solid fa-rotate"></i>
                    {/if}
                    <span>Recreate</span>
                </button>
            {/if}
            <button type="button" class="btn btn-sm btn-outline-danger d-flex align-items-center gap-2"
                    data-bs-toggle="modal" data-bs-target="#clearAllModal"
                    title="Clear all IndexedDB databases, LocalStorage, and reload the playground">
                <i class="fa-solid fa-trash-can"></i>
                <span>Clear All</span>
            </button>
        </div>
    </div>
    <!-- 3 Pipeline Step Indicators -->
    <div class="p-3 bg-white">
        <div class="row g-2 text-start">
            <!-- Step 1: Select Engine Version (Inline Expand Toggle) -->
            <div class="col-12 col-md-4">
                <button type="button"
                        class="step-card btn w-100 p-2 border rounded-2 h-100 d-flex align-items-center justify-content-between text-start {$expandedStatusSection === 'engine' ? 'bg-primary-subtle border-primary' : ($isEngineInitializing ? 'bg-info-subtle border-info-subtle' : (!isEngineReady ? 'bg-light border-dashed' : 'bg-success-subtle border-success-subtle'))}"
                        onclick={() => toggleSection('engine')}
                        title="Click to configure WASM query engine and settings">
                    <div class="d-flex align-items-center gap-2 overflow-hidden text-truncate me-1">
                        <i class="fa-solid {$isEngineInitializing ? 'fa-spinner fa-spin text-info' : (!isEngineReady ? 'fa-circle-notch text-muted' : 'fa-circle-check text-success')} fs-5 ms-1 flex-shrink-0"></i>
                        <div class="overflow-hidden text-truncate">
                            <div class="fw-bold small text-truncate text-dark">
                                1. Select Engine Version
                            </div>
                            <div class="text-muted text-truncate" style="font-size: 0.72rem;"
                                 title={$isEngineInitializing ? 'Loading engine...' : (!isEngineReady ? 'Click to select engine' : ($activeVersionMetadata?.name || $activeVersionMetadata?.id || 'Loaded'))}>
                                {#if $isEngineInitializing}
                                    <span class="text-info fw-medium">Loading {$activeVersionMetadata?.name || 'engine'}...</span>
                                {:else if !isEngineReady}
                                    <span class="text-primary fw-medium">Click to select engine...</span>
                                {:else}
                                    {$activeVersionMetadata?.name || $activeVersionMetadata?.id || 'Loaded'}
                                {/if}
                            </div>
                        </div>
                    </div>
                    <i class="fa-solid {$expandedStatusSection === 'engine' ? 'fa-chevron-up' : 'fa-chevron-down'} text-muted opacity-50 small me-1 flex-shrink-0"></i>
                </button>
            </div>
            <!-- Step 2: Select Dataset (Inline Expand Toggle) -->
            <div class="col-12 col-md-4">
                <button type="button"
                        class="step-card btn w-100 p-2 border rounded-2 h-100 d-flex align-items-center justify-content-between text-start {$expandedStatusSection === 'dataset' ? 'bg-primary-subtle border-primary' : (isDatasetSelected ? 'bg-success-subtle border-success-subtle' : (isEngineReady ? 'bg-warning-subtle border-warning-subtle' : 'bg-light opacity-75'))}"
                        onclick={() => toggleSection('dataset')}
                        disabled={!isEngineReady && !$isEngineInitializing}
                        title="Click to select or upload a dataset">
                    <div class="d-flex align-items-center gap-2 overflow-hidden text-truncate me-1">
                        <i class="fa-solid {isDatasetSelected ? 'fa-circle-check text-success' : (isEngineReady ? 'fa-triangle-exclamation text-warning' : 'fa-circle text-muted opacity-25')} fs-5 ms-1 flex-shrink-0"></i>
                        <div class="overflow-hidden text-truncate">
                            <div class="fw-bold small text-truncate text-dark">
                                2. Select Dataset
                            </div>
                            <div class="text-muted text-truncate" style="font-size: 0.72rem;"
                                 title={isDatasetSelected ? $activeDatasetMetadata.name : 'Click to select dataset'}>
                                {#if isDatasetSelected}
                                    {$activeDatasetMetadata.name}
                                {:else if isEngineReady}
                                    <span class="text-primary fw-medium">Click to choose dataset...</span>
                                {:else}
                                    None selected
                                {/if}
                            </div>
                        </div>
                    </div>
                    <i class="fa-solid {$expandedStatusSection === 'dataset' ? 'fa-chevron-up' : 'fa-chevron-down'} text-muted opacity-50 small me-1 flex-shrink-0"></i>
                </button>
            </div>
            <!-- Step 3: Create RDF Fusion Instance (Live State Indicator) -->
            <div class="col-12 col-md-4">
                <div class="p-2 border rounded-2 h-100 d-flex align-items-center gap-2 {isDatasetBusy ? 'bg-info-subtle border-info-subtle' : (isQueryExecuting ? 'bg-primary-subtle border-primary-subtle' : ($queryResults?.error ? 'bg-danger-subtle border-danger-subtle' : (isDatasetReady ? 'bg-success-subtle border-success-subtle' : 'bg-light')))}">
                    <i class="fa-solid {isDatasetBusy ? 'fa-spinner fa-spin text-info' : (isQueryExecuting ? 'fa-spinner fa-spin text-primary' : ($queryResults?.error ? 'fa-triangle-exclamation text-danger' : (isDatasetReady ? 'fa-circle-check text-success' : 'fa-circle text-muted opacity-25')))} fs-5 ms-1 flex-shrink-0"></i>
                    <div class="overflow-hidden text-truncate w-100">
                        <div class="fw-bold small text-truncate">
                            3. Create RDF Fusion Instance
                        </div>
                        <div class="text-muted text-truncate" style="font-size: 0.72rem;">
                            {#if isDatasetBusy}
                                Creating instance...
                            {:else if isQueryExecuting}
                                Running query...
                            {:else if $queryResults?.error}
                                <span class="text-danger">Execution failed</span>
                            {:else if isDatasetReady}
                                {#if $queryResults?.totalSeconds !== null}
                                    <span title="Query evaluation + conversion to a JSON object, measured in the worker. Does not include time for copying the results to the UI or rendering them, so the observed latency can be higher.">Ready (Last: {$queryResults.totalSeconds}s)</span>
                                {:else}
                                    Ready
                                {/if}
                            {:else}
                                Waiting for dataset
                            {/if}
                        </div>
                    </div>
                </div>
            </div>
        </div>
        {#if showQuickConfigure}
            <div class="mt-3 p-2 px-3 bg-light-subtle border rounded-2 d-flex flex-column flex-sm-row align-items-center justify-content-between gap-2">
                <div class="d-flex align-items-center gap-2 small text-muted">
                    <i class="fa-solid fa-bolt text-warning"></i>
                    <span><strong>New to RDF Fusion?</strong> Quick start with a stable release and sample dataset.</span>
                </div>
                <button type="button" class="btn btn-sm btn-primary d-flex align-items-center gap-2 flex-shrink-0"
                        onclick={handleQuickConfigure}
                        disabled={isQuickConfiguring}>
                    {#if isQuickConfiguring}
                        <i class="fa-solid fa-spinner fa-spin"></i>
                        <span>Configuring...</span>
                    {:else}
                        <i class="fa-solid fa-wand-magic-sparkles"></i>
                        <span>Quick Configure</span>
                    {/if}
                </button>
            </div>
        {/if}
    </div>
    <!-- Always mount so onMount auto-loads saved version/dataset; components show/hide their UI internally -->
    <VersionManager />
    <DatasetSelector />
</div>

<ClearAllModal />
<ManageDownloadsModal />

<style>
    .step-card {
        transition: all 0.25s ease-in-out;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .step-card:hover:not(:disabled) {
        transform: translateY(-2px);
        box-shadow: 0 6px 15px rgba(0, 0, 0, 0.1);
        filter: brightness(0.98);
    }
    .step-card:active:not(:disabled) {
        transform: translateY(0);
    }
    .border-dashed {
        border-style: dashed !important;
    }
</style>
