<script>
    import { onMount, untrack } from 'svelte';
    import { slide } from 'svelte/transition';
    import { cubicInOut, cubicOut } from 'svelte/easing';
    import { jsStore, wasmModule, activeVersionMetadata, activeDatasetMetadata, downloadedDatasets, customDatasets, isDatasetLoading, setStatus, engineSettings, reloadStoreTrigger, clearQueryResults, queryResults, expandedStatusSection } from '../store.js';
    import { getCustomDatasets, saveCustomDataset, deleteCustomDataset, getDownloadedDatasets, saveDownloadedDataset, putBlob, deleteBlob, DB_NAME } from '../db.js';
    import { DATASETS } from '../data_datasets.js';

    const datasets = DATASETS;

    let selectedDistributionId = $state("");
    let draftDistributionId = $state("");

    let selectedDataset = $state(null);
    let selectedDistribution = $state(null);
    let selectedCustomDataset = $state(null);

    // Custom Dataset Creation State
    let customName = $state("");
    let customLocation = $state("url"); // 'url' or 'file'
    let customUrl = $state("");
    let customFiles = $state(null);
    let isSaving = $state(false);
    let customErrorMsg = $state("");

    let draftSelectedDistribution = $derived.by(() => {
        if (!draftDistributionId) return null;
        for (const ds of datasets) {
            const d = ds.distributions.find(dist => dist.id === draftDistributionId);
            if (d) return d;
        }
        return null;
    });

    let draftSelectedCustomDataset = $derived.by(() => {
        if (!draftDistributionId) return null;
        return $customDatasets.find(d => d.id === draftDistributionId) || null;
    });

    let hasDatasetChanges = $derived(draftDistributionId !== selectedDistributionId && draftDistributionId !== "");

    let downloadingDatasetId = $state(null);
    let lastLoadedKey = $state("");

    function handleFileChange(e) {
        const file = e.target.files?.[0];
        if (file && !customName.trim()) {
            customName = file.name.replace(/\.[^/.]+$/, "");
        }
    }

    function handleUrlBlur() {
        if (customUrl.trim() && !customName.trim()) {
            try {
                const pathname = new URL(customUrl.trim()).pathname;
                const base = pathname.split('/').filter(Boolean).pop() || "";
                if (base) {
                    customName = decodeURIComponent(base).replace(/\.[^/.]+$/, "");
                }
            } catch {
                // Ignore invalid URL parsing
            }
        }
    }

    function resetCustomForm() {
        customName = "";
        customLocation = "url";
        customUrl = "";
        customFiles = null;
        customErrorMsg = "";
    }

    function syncDraftState() {
        if (selectedDistributionId) {
            draftDistributionId = selectedDistributionId;
        } else {
            // Default to first available distribution
            const first = datasets[0]?.distributions[0];
            if (first) draftDistributionId = first.id;
        }
        customErrorMsg = "";
    }

    $effect(() => {
        if ($expandedStatusSection === 'dataset') {
            syncDraftState();
        }
    });

    function acceptDatasetChanges() {
        if (draftDistributionId && draftDistributionId !== selectedDistributionId) {
            selectedDistributionId = draftDistributionId;
            handleDistributionSelect();
        }
        $expandedStatusSection = null;
    }

    async function handleSaveCustomDataset() {
        customErrorMsg = "";
        if (!customName.trim()) {
            customErrorMsg = "Dataset name is required.";
            return;
        }

        if (customLocation === 'url' && !customUrl.trim()) {
            customErrorMsg = "Parquet dataset URL is required.";
            return;
        }
        
        if (customLocation === 'file' && (!customFiles || customFiles.length === 0)) {
            customErrorMsg = "Please select a .parquet file to upload.";
            return;
        }

        isSaving = true;
        try {
            let finalBlob = null;
            let finalEncoding = "String";
            let finalSourceType = customLocation;
            let finalUrl = customLocation === 'url' ? customUrl.trim() : null;
            const id = "custom-" + Date.now();

            if (customLocation === 'file') {
                finalBlob = customFiles[0];
                if (!finalBlob.name.toLowerCase().endsWith('.parquet')) {
                    throw new Error("Only .parquet files are supported.");
                }

                await putBlob(id, finalBlob);

                // Verify Parquet file structure with WASM engine if available
                if ($wasmModule) {
                    const settings = {
                        memoryLimitMb: $engineSettings.memoryLimit,
                        metrics: {
                            showMetrics: $engineSettings.metrics.showMetrics,
                            showStatistics: $engineSettings.metrics.showStatistics
                        },
                        customConfig: {}
                    };
                    let verified = false;
                    try {
                        await $wasmModule.setDataset({ source: 'indexeddb', dbName: DB_NAME, key: id, encoding: 'String', settings });
                        finalEncoding = "String";
                        verified = true;
                    } catch {
                        try {
                            await $wasmModule.setDataset({ source: 'indexeddb', dbName: DB_NAME, key: id, encoding: 'PlainTerm', settings });
                            finalEncoding = "PlainTerm";
                            verified = true;
                        } catch (e2) {
                            await deleteBlob(id);
                            console.error("Failed to verify Parquet dataset:", e2);
                            const errMsg = e2?.message || String(e2);
                            throw new Error(`Uploaded file is not a valid RDF Parquet dataset: ${errMsg}`, { cause: e2 });
                        }
                    }

                    if (!verified) {
                        await deleteBlob(id);
                        throw new Error("Could not verify Parquet encoding.");
                    }
                }
            }

            const newDataset = {
                id,
                name: customName.trim(),
                sourceType: finalSourceType,
                url: finalUrl,
                fileBlob: finalBlob,
                format: "parquet",
                encoding: finalEncoding,
                sortOrder: "GPOS",
                size: finalBlob ? finalBlob.size : null
            };

            await saveCustomDataset(newDataset);
            await reloadCustomDatasets();

            draftDistributionId = newDataset.id;
            selectedDistributionId = newDataset.id;
            resetCustomForm();

            const modalEl = document.getElementById('customDatasetModal');
            if (modalEl) {
                const modal = window.bootstrap.Modal.getInstance(modalEl);
                modal?.hide();
            }

            handleDistributionSelect();
            $expandedStatusSection = null;
            setStatus(`Custom dataset '${newDataset.name}' loaded.`, 'fa-check-circle', 'success');
        } catch (e) {
            console.error(e);
            customErrorMsg = e.message || String(e);
            setStatus('Failed to save dataset: ' + e, 'fa-bug', 'danger');
        } finally {
            isSaving = false;
        }
    }

    onMount(async () => {
        await reloadCustomDatasets();
        await reloadDownloadedDatasets();

        const datasetModalEl = document.getElementById('datasetModal');
        if (datasetModalEl) {
            datasetModalEl.addEventListener('show.bs.modal', syncDraftState);
            datasetModalEl.addEventListener('hidden.bs.modal', syncDraftState);
        }
        
        const storedId = localStorage.getItem('rdfFusionLastDataset');
        if (storedId) {
            selectedDistributionId = storedId;
            draftDistributionId = storedId;
        }
    });

    $effect(() => {
        const trigger = $reloadStoreTrigger;
        if (trigger > 0) {
            untrack(() => {
                lastLoadedKey = "";
                const storedId = localStorage.getItem('rdfFusionLastDataset');
                if (storedId && selectedDistributionId !== storedId) {
                    selectedDistributionId = storedId;
                    draftDistributionId = storedId;
                }
                if (selectedDistribution || selectedCustomDataset || selectedDistributionId) {
                    handleDistributionSelect();
                }
            });
        }
    });

    $effect(() => {
        const distId = selectedDistributionId;
        if (distId) {
            localStorage.setItem('rdfFusionLastDataset', distId);
        }
    });

    $effect(() => {
        const wasm = $wasmModule;
        const distId = selectedDistributionId;
        if (wasm && distId) {
            const key = `${distId}`;
            if (key !== lastLoadedKey) {
                lastLoadedKey = key;
                untrack(() => {
                    handleDistributionSelect();
                });
            }
        }
    });

    async function reloadCustomDatasets() {
        $customDatasets = await getCustomDatasets();
        if (selectedDistributionId && selectedCustomDataset && !$customDatasets.find(d => d.id === selectedDistributionId)) {
            selectedDistributionId = "";
            selectedCustomDataset = null;
            $jsStore = null;
        }
    }

    async function reloadDownloadedDatasets() {
        $downloadedDatasets = await getDownloadedDatasets();
    }

    function isSupported(dist) {
        if (!$activeVersionMetadata) return true;
        if ($activeVersionMetadata.isCustom) return true;
        return $activeVersionMetadata.supportedStorage.some(
            s => s.type === dist.quadStorage.type && s.version === dist.quadStorage.version
        );
    }

    $effect(() => {
        const meta = $activeVersionMetadata;
        const dist = selectedDistribution;
        if (meta && dist && !isSupported(dist)) {
            untrack(() => {
                selectedDistributionId = "";
                lastLoadedKey = "";
                handleDistributionSelect();
            });
        }
    });

    async function downloadDatasetUrl(id, name, url) {
        try {
            downloadingDatasetId = id;
            const response = await fetch(url);
            if (!response.ok) throw new Error("Failed to download");
            const blob = await response.blob();
            await saveDownloadedDataset(id, blob, name, url);
            await reloadDownloadedDatasets();
            setStatus('Download complete.', 'fa-check-circle', 'green');
            
            if (selectedDistributionId === id) handleDistributionSelect();
        } catch (e) {
            setStatus('Failed to download: ' + e, 'fa-bug', 'danger');
        } finally {
            downloadingDatasetId = null;
        }
    }

    async function handleDistributionSelect() {
        $jsStore = null;
        if (!$queryResults?.isExecuting) {
            clearQueryResults();
        }
        selectedDataset = null;
        selectedDistribution = null;
        selectedCustomDataset = null;
        
        if (!selectedDistributionId) {
            $activeDatasetMetadata = null;
            return;
        }

        for (const ds of datasets) {
            const dist = ds.distributions.find(d => d.id === selectedDistributionId);
            if (dist) {
                selectedDataset = ds;
                selectedDistribution = dist;
                break;
            }
        }

        if (!selectedDistribution) {
            selectedCustomDataset = $customDatasets.find(d => d.id === selectedDistributionId);
        }

        if (!selectedDistribution && !selectedCustomDataset) {
            $activeDatasetMetadata = null;
            return;
        }

        const isCustom = !!selectedCustomDataset;
        const currentName = isCustom ? selectedCustomDataset.name : `${selectedDataset.name} - ${selectedDistribution.name}`;
        const queryGroup = isCustom ? null : selectedDataset.queryGroup;
        const activeSize = selectedCustomDataset?.size ?? $downloadedDatasets.find(d => d.id === selectedDistributionId)?.size ?? null;

        $activeDatasetMetadata = {
            id: selectedDistributionId,
            name: currentName,
            queryGroup,
            size: activeSize,
            isCustom
        };

        if (!$wasmModule) {
            setStatus('Please load a WASM version first.', 'fa-info-circle', 'info');
            return;
        }

        if (!isCustom && !isSupported(selectedDistribution)) {
            setStatus(`Storage format not supported by active WASM version.`, 'fa-triangle-exclamation', 'warning');
            return;
        }

        $isDatasetLoading = true;
        setStatus('Initializing dataset...', 'fa-cog fa-spin', 'brown');

        const dfConfigObj = {
            "datafusion.optimizer.enable_dynamic_filter_pushdown": $engineSettings.dataFusion.enableDynamicFilterPushdown ? "true" : "false",
            "datafusion.execution.target_partitions": String($engineSettings.dataFusion.targetPartitions || 1)
        };

        if ($engineSettings.rdfFusion) {
            const isCacheEnabled = $engineSettings.rdfFusion.enableDataCache ? "true" : "false";
            const blockSizeBytes = String(($engineSettings.rdfFusion.dataCacheBlockSizeKb || 2048) * 1024);
            const numBlocks = String($engineSettings.rdfFusion.dataCacheNumBlocks || 1024);

            dfConfigObj["rdf_fusion.storage.parquet.data_cache_enabled"] = isCacheEnabled;
            dfConfigObj["rdf_fusion.storage.parquet.data_cache_block_size"] = blockSizeBytes;
            dfConfigObj["rdf_fusion.storage.parquet.data_cache_num_blocks"] = numBlocks;

            dfConfigObj["rdf_fusion.storage.delta.data_cache_enabled"] = isCacheEnabled;
            dfConfigObj["rdf_fusion.storage.delta.data_cache_block_size"] = blockSizeBytes;
            dfConfigObj["rdf_fusion.storage.delta.data_cache_num_blocks"] = numBlocks;
        }

        if ($engineSettings.customConfig) {
            for (const conf of $engineSettings.customConfig) {
                if (conf.key && conf.value) {
                    dfConfigObj[conf.key] = conf.value;
                }
            }
        }

        try {
            const settings = {
                memoryLimitMb: $engineSettings.memoryLimit,
                metrics: {
                    showMetrics: $engineSettings.metrics.showMetrics,
                    showStatistics: $engineSettings.metrics.showStatistics
                },
                customConfig: dfConfigObj
            };

            let datasetSpec;
            if (isCustom) {
                if (selectedCustomDataset.sourceType === 'file'
                    || $downloadedDatasets.some(d => d.id === selectedCustomDataset.id)) {
                    datasetSpec = {
                        source: 'indexeddb',
                        dbName: DB_NAME,
                        key: selectedCustomDataset.id,
                        encoding: selectedCustomDataset.encoding || 'String',
                        settings
                    };
                } else {
                    datasetSpec = {
                        source: 'http',
                        url: selectedCustomDataset.url,
                        encoding: selectedCustomDataset.encoding || 'String',
                        settings
                    };
                }
            } else {
                const localItem = $downloadedDatasets.find(d => d.id === selectedDistribution.id);
                if (localItem) {
                    datasetSpec = {
                        source: 'indexeddb',
                        dbName: DB_NAME,
                        key: localItem.id,
                        encoding: 'String',
                        settings
                    };
                } else {
                    datasetSpec = {
                        source: 'http',
                        url: selectedDistribution.url,
                        encoding: 'String',
                        settings
                    };
                }
            }

            await $wasmModule.setDataset(datasetSpec);
            $jsStore = true;
            setStatus('Store initialized and ready for queries.', 'fa-circle-check', 'green');
        } catch (e) {
            console.error("Store initialization error:", e);
            setStatus('Failed to initialize dataset: ' + e, 'fa-bug', 'danger', e);
        } finally {
            $isDatasetLoading = false;
        }
    }

    async function handleDeleteCustom(id) {
        if (confirm("Are you sure you want to delete this custom dataset?")) {
            await deleteCustomDataset(id);
            await reloadCustomDatasets();
            if (selectedDistributionId === id) {
                selectedDistributionId = "";
                draftDistributionId = "";
                $jsStore = null;
                $activeDatasetMetadata = null;
            }
        }
    }
</script>

<!-- Inline Dataset Selection Section (Expanded in Status Card) -->
{#if $expandedStatusSection === 'dataset'}
    <div class="border-top bg-white overflow-hidden">
        <div in:slide={{ duration: 220, easing: cubicOut }} out:slide={{ duration: 160, easing: cubicInOut }} class="p-4 d-flex flex-column gap-3">
        <div class="d-flex align-items-center justify-content-between border-bottom pb-2">
            <h6 class="pane-heading mb-0 text-dark d-flex align-items-center gap-2">
                <i class="fa-solid fa-database text-brown"></i> Dataset Selection
            </h6>
            <button type="button" class="btn-close small" aria-label="Close" onclick={() => $expandedStatusSection = null}></button>
        </div>

        <div class="d-flex flex-column gap-3">
            <!-- Single grouped picker: "Dataset - Distribution" -->
            <div class="d-flex align-items-center gap-2 flex-wrap">
                <label for="distributionPicker" class="fw-bold text-nowrap mb-0" style="min-width: 80px;">Dataset:</label>
                <select
                    id="distributionPicker"
                    bind:value={draftDistributionId}
                    disabled={isSaving}
                    class="form-select form-select-sm bg-white"
                    style="flex: 1; min-width: 220px; max-width: 480px;">
                    <optgroup label="Library">
                        {#each datasets as ds (ds.id)}
                            {#each ds.distributions as dist (dist.id)}
                                <option value={dist.id} disabled={!isSupported(dist)}>
                                    {ds.name} – {dist.name}{!isSupported(dist) ? ' (Unsupported)' : ''}
                                </option>
                            {/each}
                        {/each}
                    </optgroup>
                    {#if $customDatasets.length > 0}
                        <optgroup label="Custom Datasets">
                            {#each $customDatasets as ds (ds.id)}
                                <option value={ds.id}>{ds.name} ({ds.sourceType === 'url' ? 'URL' : 'Local File'})</option>
                            {/each}
                        </optgroup>
                    {/if}
                </select>

                <!-- Download / local indicator for the selected distribution -->
                {#if draftSelectedDistribution}
                    {#if downloadingDatasetId === draftDistributionId}
                        <button class="btn btn-sm btn-secondary" disabled>
                            <i class="fa-solid fa-spinner fa-spin me-1"></i> Downloading...
                        </button>
                    {:else if $downloadedDatasets.find(d => d.id === draftDistributionId)}
                        <span class="text-success" title="Downloaded (Available locally)" style="font-size: 1.1rem; cursor: default;">
                            <i class="fa-solid fa-hard-drive"></i>
                        </span>
                    {:else}
                        <button class="btn btn-sm btn-outline-secondary" onclick={() => downloadDatasetUrl(draftSelectedDistribution.id, draftSelectedDistribution.id, draftSelectedDistribution.url)} title="Download for faster offline loading">
                            <i class="fa-solid fa-download me-1"></i> Download
                        </button>
                    {/if}
                {:else if draftSelectedCustomDataset}
                    {#if downloadingDatasetId === draftSelectedCustomDataset.id}
                        <button class="btn btn-sm btn-secondary" disabled>
                            <i class="fa-solid fa-spinner fa-spin me-1"></i> Downloading...
                        </button>
                    {:else if $downloadedDatasets.find(d => d.id === draftSelectedCustomDataset.id)}
                        <span class="text-success" title="Downloaded (Available locally)" style="font-size: 1.1rem; cursor: default;">
                            <i class="fa-solid fa-hard-drive"></i>
                        </span>
                    {:else if draftSelectedCustomDataset.sourceType === 'url'}
                        <button class="btn btn-sm btn-outline-secondary" onclick={() => downloadDatasetUrl(draftSelectedCustomDataset.id, draftSelectedCustomDataset.name, draftSelectedCustomDataset.url)} title="Download for faster offline loading">
                            <i class="fa-solid fa-download me-1"></i> Download
                        </button>
                    {/if}
                    <button class="btn btn-sm btn-outline-danger" onclick={() => handleDeleteCustom(draftSelectedCustomDataset.id)} title="Delete Dataset">
                        <i class="fa-solid fa-trash"></i>
                    </button>
                {/if}
            </div>

            {#if draftSelectedDistribution && $activeVersionMetadata?.isCustom}
                <div class="text-warning small w-100 d-flex align-items-center gap-2 p-2 bg-white border rounded">
                    <i class="fa-solid fa-triangle-exclamation"></i>
                    <span><strong>Warning:</strong> You are using an uploaded engine version. We cannot guarantee compatibility with this distribution.</span>
                </div>
            {/if}
        </div>

        <!-- Inline Actions -->
        <div class="d-flex justify-content-between align-items-center gap-2 pt-2 border-top flex-wrap">
            <button 
                type="button" 
                class="btn btn-sm btn-outline-primary d-flex align-items-center gap-2"
                data-bs-toggle="modal"
                data-bs-target="#customDatasetModal"
                title="Add custom Parquet dataset">
                <i class="fa-solid fa-plus"></i>
                <span>Add Custom Dataset</span>
            </button>
            <div class="d-flex gap-2">
                <button type="button" class="btn btn-sm btn-secondary px-3" onclick={() => $expandedStatusSection = null} disabled={isSaving}>Close</button>
                <button 
                    type="button" 
                    class="btn btn-sm btn-primary px-3 fw-semibold d-flex align-items-center gap-2" 
                    onclick={acceptDatasetChanges} 
                    disabled={isSaving || (!hasDatasetChanges && selectedDistributionId !== '')}>
                    <i class="fa-solid fa-check"></i> Apply
                </button>
            </div>
        </div>
    </div>
    </div>
{/if}

<!-- Dedicated Add Custom Parquet Dataset Modal -->
<div class="modal fade" id="customDatasetModal" tabindex="-1" aria-labelledby="customDatasetModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-lg modal-dialog-scrollable">
        <div class="modal-content border-0 shadow">
            <div class="modal-header bg-light border-bottom py-3 px-4 d-flex align-items-center justify-content-between">
                <div class="d-flex align-items-center gap-2">
                    <i class="fa-solid fa-file-arrow-up text-brown fs-5"></i>
                    <h5 class="modal-title pane-heading mb-0" id="customDatasetModalLabel">Add Custom Parquet Dataset</h5>
                </div>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>

            <div class="modal-body p-4 d-flex flex-column gap-3">
                <div class="d-flex justify-content-between align-items-center flex-wrap gap-2 border-bottom pb-2">
                    <strong class="text-dark small">Dataset Source:</strong>
                    <div class="btn-group btn-group-sm" role="group">
                        <button type="button" class="btn {customLocation === 'url' ? 'btn-primary' : 'btn-secondary'}" onclick={() => customLocation = 'url'}>
                            <i class="fa-solid fa-link me-1"></i> Remote URL
                        </button>
                        <button type="button" class="btn {customLocation === 'file' ? 'btn-primary' : 'btn-secondary'}" onclick={() => customLocation = 'file'}>
                            <i class="fa-solid fa-upload me-1"></i> Local File
                        </button>
                    </div>
                </div>

                {#if customErrorMsg}
                    <div class="alert alert-danger py-2 px-3 small d-flex align-items-center gap-2 mb-0">
                        <i class="fa-solid fa-circle-exclamation flex-shrink-0"></i>
                        <span>{customErrorMsg}</span>
                    </div>
                {/if}

                <div>
                    <label for="customDsName" class="form-label small fw-bold">Dataset Name <span class="text-danger">*</span></label>
                    <input id="customDsName" type="text" class="form-control form-control-sm" bind:value={customName} placeholder="e.g. My Benchmark 2026">
                </div>

                <div>
                    <label for={customLocation === 'url' ? 'customDsUrl' : 'customDsFile'} class="form-label small fw-bold">
                        {customLocation === 'url' ? 'Dataset URL' : 'Upload File'} <span class="text-danger">*</span>
                    </label>
                    {#if customLocation === 'url'}
                        <input id="customDsUrl" type="url" class="form-control form-control-sm" bind:value={customUrl} onblur={handleUrlBlur} placeholder="https://example.com/dataset.parquet">
                        <div class="text-muted mt-1" style="font-size: 0.72rem;">Supports Parquet (.parquet) files from CORS-enabled URLs.</div>
                    {:else}
                        <input id="customDsFile" type="file" class="form-control form-control-sm" bind:files={customFiles} onchange={handleFileChange} accept=".parquet">
                        <div class="text-muted mt-1" style="font-size: 0.72rem;">Select a valid .parquet file with RDF terms.</div>
                    {/if}
                </div>
            </div>

            <div class="modal-footer bg-light border-top py-2 px-4 d-flex justify-content-end align-items-center gap-2">
                <button type="button" class="btn btn-sm btn-secondary px-3" data-bs-dismiss="modal" disabled={isSaving}>Cancel</button>
                <button 
                    type="button" 
                    class="btn btn-sm btn-primary px-3 fw-semibold d-flex align-items-center gap-2" 
                    onclick={handleSaveCustomDataset} 
                    disabled={isSaving || !customName.trim() || (customLocation === 'url' ? !customUrl.trim() : !customFiles || customFiles.length === 0)}>
                    {#if isSaving}
                        <i class="fa-solid fa-spinner fa-spin"></i> Saving...
                    {:else}
                        <i class="fa-solid fa-check"></i> Save & Select
                    {/if}
                </button>
            </div>
        </div>
    </div>
</div>
