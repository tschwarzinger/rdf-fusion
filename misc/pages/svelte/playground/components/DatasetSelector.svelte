<script>
    import { onMount, untrack } from 'svelte';
    import { slide } from 'svelte/transition';
    import { cubicInOut, cubicOut } from 'svelte/easing';
    import { jsStore, wasmModule, activeVersionMetadata, activeDatasetMetadata, downloadedDatasets, customDatasets, isDatasetLoading, setStatus, engineSettings, reloadStoreTrigger, clearQueryResults, queryResults, expandedStatusSection } from '../store.js';
    import { getCustomDatasets, saveCustomDataset, getDownloadedDatasets, saveDownloadedDataset, putBlob, getBlob, deleteBlob, DB_NAME } from '../db.js';
    import { DATASETS } from '../data_datasets.js';

    const datasets = DATASETS;

    let selectedDistributionId = $state("");
    let draftDistributionId = $state("");

    let selectedDataset = $state(null);
    let selectedDistribution = $state(null);
    let selectedCustomDataset = $state(null);

    // 1. Add Parquet Dataset Modal State
    let parquetName = $state("");
    let parquetLocation = $state("file"); // 'file' or 'url'
    let parquetUrl = $state("");
    let parquetFiles = $state(null);
    let isParquetSaving = $state(false);
    let parquetErrorMsg = $state("");

    // 2. Convert Traditional RDF Modal State
    let rdfName = $state("");
    let rdfFiles = $state(null);
    let rdfEncoding = $state("String"); // 'String' or 'PlainTerm'
    let rdfSortOrder = $state("GPOS"); // 'GPOS', 'GSPO', 'SPOG', 'POSG', 'OSPG', 'None', 'custom'
    let rdfSortOrderCustom = $state("");
    let isRdfConverting = $state(false);
    let rdfErrorMsg = $state("");

    const RDF_EXTENSIONS = ['ttl', 'nt', 'nq', 'trig', 'rdf', 'owl', 'xml', 'n3'];

    function getFileExtension(filename) {
        if (!filename) return '';
        const parts = filename.split('.');
        return parts.length > 1 ? parts.pop().toLowerCase() : '';
    }

    let isSaving = $derived(isParquetSaving || isRdfConverting);

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

    let canConvertRdf = $derived.by(() => {
        if (!$wasmModule || !$activeVersionMetadata) return false;
        const caps = $activeVersionMetadata.capabilities;
        if (Array.isArray(caps)) {
            return caps.includes('rdf-conversion') || caps.includes('rdf_conversion');
        }
        return !!$activeVersionMetadata.isCustom;
    });

    let convertRdfTooltip = $derived.by(() => {
        if (!$wasmModule || !$activeVersionMetadata) {
            return "Please select and load a WASM query engine first.";
        }
        if (!canConvertRdf) {
            return `The selected engine version (${$activeVersionMetadata.name || $activeVersionMetadata.id}) does not support RDF conversion. Please select a compatible build.`;
        }
        return "Upload traditional RDF data (.ttl, .nt, etc.) and convert to Parquet";
    });

    let downloadingDatasetId = $state(null);
    let lastLoadedSignature = $state("");
    let currentLoadSeq = 0;

    function handleParquetFileChange(e) {
        const file = e.target.files?.[0];
        if (file && !parquetName.trim()) {
            parquetName = file.name.replace(/\.[^/.]+$/, "");
        }
        parquetErrorMsg = "";
    }

    function handleParquetUrlBlur() {
        if (parquetUrl.trim() && !parquetName.trim()) {
            try {
                const pathname = new URL(parquetUrl.trim()).pathname;
                const base = pathname.split('/').filter(Boolean).pop() || "";
                if (base) {
                    parquetName = decodeURIComponent(base).replace(/\.[^/.]+$/, "");
                }
            } catch {
                // Ignore invalid URL parsing
            }
        }
    }

    function resetParquetForm() {
        parquetName = "";
        parquetLocation = "file";
        parquetUrl = "";
        parquetFiles = null;
        parquetErrorMsg = "";
    }

    function handleRdfFileChange(e) {
        const file = e.target.files?.[0];
        if (file && !rdfName.trim()) {
            rdfName = file.name.replace(/\.[^/.]+$/, "");
        }
        rdfErrorMsg = "";
    }

    function resetRdfForm() {
        rdfName = "";
        rdfFiles = null;
        rdfEncoding = "String";
        rdfSortOrder = "GPOS";
        rdfSortOrderCustom = "";
        rdfErrorMsg = "";
    }

    function syncDraftState() {
        if (selectedDistributionId) {
            draftDistributionId = selectedDistributionId;
        } else {
            // Default to first available distribution
            const first = datasets[0]?.distributions[0];
            if (first) draftDistributionId = first.id;
        }
        parquetErrorMsg = "";
        rdfErrorMsg = "";
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

    async function exportParquetFile(id, name) {
        try {
            const blob = await getBlob(id);
            if (!blob) throw new Error("Dataset Parquet file not found in storage.");
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `${name.replace(/[^a-zA-Z0-9_-]/g, '_')}.parquet`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        } catch (e) {
            console.error(e);
            setStatus('Failed to export Parquet file: ' + e, 'fa-bug', 'danger');
        }
    }

    async function handleSaveParquetDataset() {
        parquetErrorMsg = "";
        if (!parquetName.trim()) {
            parquetErrorMsg = "Dataset name is required.";
            return;
        }

        if (parquetLocation === 'url' && !parquetUrl.trim()) {
            parquetErrorMsg = "Parquet dataset URL is required.";
            return;
        }
        
        if (parquetLocation === 'file' && (!parquetFiles || parquetFiles.length === 0)) {
            parquetErrorMsg = "Please select a .parquet file to upload.";
            return;
        }

        isParquetSaving = true;
        try {
            let finalBlob = null;
            let finalEncoding = "String";
            let finalSourceType = parquetLocation;
            let finalUrl = parquetLocation === 'url' ? parquetUrl.trim() : null;
            const id = "custom-" + Date.now();

            if (parquetLocation === 'file') {
                finalBlob = parquetFiles[0];
                if (!finalBlob.name.toLowerCase().endsWith('.parquet')) {
                    throw new Error("Only .parquet files are supported in this dialog. To convert .ttl, .nt, etc., use 'Convert Dataset'.");
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
                name: parquetName.trim(),
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
            resetParquetForm();

            const modalEl = document.getElementById('parquetDatasetModal');
            if (modalEl) {
                const modal = window.bootstrap.Modal.getInstance(modalEl);
                modal?.hide();
            }

            handleDistributionSelect();
            $expandedStatusSection = null;
            setStatus(`Custom dataset '${newDataset.name}' loaded.`, 'fa-check-circle', 'success');
        } catch (e) {
            console.error(e);
            parquetErrorMsg = e.message || String(e);
            setStatus('Failed to save dataset: ' + e, 'fa-bug', 'danger');
        } finally {
            isParquetSaving = false;
        }
    }

    async function handleConvertRdfDataset() {
        rdfErrorMsg = "";
        if (!rdfName.trim()) {
            rdfErrorMsg = "Dataset name is required.";
            return;
        }

        if (!rdfFiles || rdfFiles.length === 0) {
            rdfErrorMsg = "Please select an RDF file (.ttl, .nt, .nq, .trig, .rdf, .xml, .n3) to convert.";
            return;
        }

        const file = rdfFiles[0];
        const ext = getFileExtension(file.name);
        if (!ext || !RDF_EXTENSIONS.includes(ext)) {
            rdfErrorMsg = `Unsupported file format '.${ext || 'unknown'}'. Please select a valid RDF file (.ttl, .nt, .nq, .trig, .rdf, .xml, .n3).`;
            return;
        }

        if (!$wasmModule) {
            rdfErrorMsg = "A loaded WASM query engine is required to convert RDF files to Parquet. Please select an engine version first.";
            return;
        }

        isRdfConverting = true;
        const id = "custom-" + Date.now();
        const tempInputKey = `convert-temp-${id}.${ext}`;
        const finalSortOrder = rdfSortOrder === 'custom' ? (rdfSortOrderCustom.trim() || "GPOS") : rdfSortOrder;
        const sortOrderParam = finalSortOrder === 'None' ? null : finalSortOrder;

        try {
            setStatus(`Uploading and converting RDF file '${file.name}' to Parquet (${rdfEncoding}, ${sortOrderParam || 'None'})...`, 'fa-cog fa-spin', 'brown');

            await putBlob(tempInputKey, file);

            await $wasmModule.convertRdf({
                dbName: DB_NAME,
                inputKey: tempInputKey,
                outputKey: id,
                encoding: rdfEncoding,
                sortOrder: sortOrderParam
            });

            const convertedBlob = await getBlob(id);
            if (!convertedBlob) {
                throw new Error("Parquet conversion finished but output was not found in storage.");
            }

            const newDataset = {
                id,
                name: rdfName.trim(),
                sourceType: 'file',
                url: null,
                fileBlob: null,
                format: "parquet",
                encoding: rdfEncoding,
                sortOrder: finalSortOrder,
                size: convertedBlob.size,
                originalFormat: ext,
                convertedFrom: file.name
            };

            await saveCustomDataset(newDataset);
            await reloadCustomDatasets();

            draftDistributionId = newDataset.id;
            selectedDistributionId = newDataset.id;
            resetRdfForm();

            const modalEl = document.getElementById('convertRdfModal');
            if (modalEl) {
                const modal = window.bootstrap.Modal.getInstance(modalEl);
                modal?.hide();
            }

            handleDistributionSelect();
            $expandedStatusSection = null;
            setStatus(`RDF dataset '${newDataset.name}' converted to Parquet and loaded.`, 'fa-check-circle', 'success');
        } catch (e) {
            console.error(e);
            rdfErrorMsg = e.message || String(e);
            setStatus('Failed to convert RDF dataset: ' + e, 'fa-bug', 'danger');
        } finally {
            await deleteBlob(tempInputKey).catch(() => {});
            isRdfConverting = false;
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
        const distId = selectedDistributionId;
        if (distId) {
            localStorage.setItem('rdfFusionLastDataset', distId);
        }
    });

    $effect(() => {
        const wasm = $wasmModule;
        const distId = selectedDistributionId;
        const trigger = $reloadStoreTrigger;
        const settingsStr = JSON.stringify($engineSettings);

        if (!wasm || !distId) {
            lastLoadedSignature = "";
            return;
        }

        const sig = `${distId}::${settingsStr}::${trigger}`;
        if (sig !== lastLoadedSignature) {
            lastLoadedSignature = sig;
            untrack(() => {
                handleDistributionSelect();
            });
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
                lastLoadedSignature = "";
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
            
            if (selectedDistributionId === id) {
                lastLoadedSignature = "";
                handleDistributionSelect();
            }
        } catch (e) {
            setStatus('Failed to download: ' + e, 'fa-bug', 'danger');
        } finally {
            downloadingDatasetId = null;
        }
    }

    async function handleDistributionSelect() {
        const thisSeq = ++currentLoadSeq;
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
            if (thisSeq !== currentLoadSeq) return;
            $jsStore = true;
            setStatus('Store initialized and ready for queries.', 'fa-circle-check', 'green');
        } catch (e) {
            if (thisSeq !== currentLoadSeq) return;
            console.error("Store initialization error:", e);
            setStatus('Failed to initialize dataset: ' + e, 'fa-bug', 'danger', e);
        } finally {
            if (thisSeq === currentLoadSeq) {
                $isDatasetLoading = false;
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
                    {:else if draftSelectedCustomDataset.sourceType === 'file' || $downloadedDatasets.find(d => d.id === draftSelectedCustomDataset.id)}
                        <button class="btn btn-sm btn-outline-secondary" onclick={() => exportParquetFile(draftSelectedCustomDataset.id, draftSelectedCustomDataset.name)} title="Export / Download Parquet file">
                            <i class="fa-solid fa-file-arrow-down me-1"></i> Export Parquet
                        </button>
                    {:else if draftSelectedCustomDataset.sourceType === 'url'}
                        <button class="btn btn-sm btn-outline-secondary" onclick={() => downloadDatasetUrl(draftSelectedCustomDataset.id, draftSelectedCustomDataset.name, draftSelectedCustomDataset.url)} title="Download for faster offline loading">
                            <i class="fa-solid fa-download me-1"></i> Download
                        </button>
                    {/if}
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
            <div class="d-flex align-items-center gap-2 flex-wrap">
                <button 
                    type="button" 
                    class="btn btn-sm btn-outline-primary d-flex align-items-center gap-2"
                    data-bs-toggle="modal"
                    data-bs-target="#parquetDatasetModal"
                    title="Add an existing Parquet dataset (URL or file)">
                    <i class="fa-solid fa-plus"></i>
                    <span>Add Dataset</span>
                </button>
                <span class="d-inline-block" title={convertRdfTooltip}>
                    <button 
                        type="button" 
                        class="btn btn-sm btn-outline-brown d-flex align-items-center gap-2"
                        data-bs-toggle="modal"
                        data-bs-target="#convertRdfModal"
                        disabled={!canConvertRdf || isSaving}
                        title={convertRdfTooltip}>
                        <i class="fa-solid fa-wand-magic-sparkles text-brown"></i>
                        <span>Convert Dataset</span>
                    </button>
                </span>
            </div>
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

<!-- 1. Dedicated Add Parquet Dataset Modal -->
<div class="modal fade" id="parquetDatasetModal" tabindex="-1" aria-labelledby="parquetDatasetModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-lg modal-dialog-scrollable">
        <div class="modal-content border-0 shadow">
            <div class="modal-header bg-light border-bottom py-3 px-4 d-flex align-items-center justify-content-between">
                <div class="d-flex align-items-center gap-2">
                    <i class="fa-solid fa-file-circle-plus text-primary fs-5"></i>
                    <h5 class="modal-title pane-heading mb-0" id="parquetDatasetModalLabel">Add Parquet Dataset</h5>
                </div>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>

            <div class="modal-body p-4 d-flex flex-column gap-3">
                <div class="d-flex justify-content-between align-items-center flex-wrap gap-2 border-bottom pb-2">
                    <strong class="text-dark small">Parquet Source:</strong>
                    <div class="btn-group btn-group-sm" role="group">
                        <button type="button" class="btn {parquetLocation === 'file' ? 'btn-primary' : 'btn-secondary'}" onclick={() => parquetLocation = 'file'}>
                            <i class="fa-solid fa-upload me-1"></i> Upload File (.parquet)
                        </button>
                        <button type="button" class="btn {parquetLocation === 'url' ? 'btn-primary' : 'btn-secondary'}" onclick={() => parquetLocation = 'url'}>
                            <i class="fa-solid fa-link me-1"></i> Remote Parquet URL
                        </button>
                    </div>
                </div>

                {#if parquetErrorMsg}
                    <div class="alert alert-danger py-2 px-3 small d-flex align-items-center gap-2 mb-0">
                        <i class="fa-solid fa-circle-exclamation flex-shrink-0"></i>
                        <span>{parquetErrorMsg}</span>
                    </div>
                {/if}

                <div>
                    <label for="parquetDsName" class="form-label small fw-bold">Dataset Name <span class="text-danger">*</span></label>
                    <input id="parquetDsName" type="text" class="form-control form-control-sm" bind:value={parquetName} placeholder="e.g. BSBM 1M Parquet">
                </div>

                <div>
                    <label for={parquetLocation === 'url' ? 'parquetDsUrl' : 'parquetDsFile'} class="form-label small fw-bold">
                        {parquetLocation === 'url' ? 'Dataset URL' : 'Select .parquet File'} <span class="text-danger">*</span>
                    </label>
                    {#if parquetLocation === 'url'}
                        <input id="parquetDsUrl" type="url" class="form-control form-control-sm" bind:value={parquetUrl} onblur={handleParquetUrlBlur} placeholder="https://example.com/dataset.parquet">
                        <div class="text-muted mt-1" style="font-size: 0.72rem;">Supports Parquet (<code>.parquet</code>) files from CORS-enabled URLs.</div>
                    {:else}
                        <input id="parquetDsFile" type="file" class="form-control form-control-sm" bind:files={parquetFiles} onchange={handleParquetFileChange} accept=".parquet">
                        <div class="text-muted mt-1" style="font-size: 0.72rem;">Select a valid <code>.parquet</code> RDF dataset file.</div>
                    {/if}
                </div>
            </div>

            <div class="modal-footer bg-light border-top py-2 px-4 d-flex justify-content-end align-items-center gap-2">
                <button type="button" class="btn btn-sm btn-secondary px-3" data-bs-dismiss="modal" disabled={isParquetSaving}>Cancel</button>
                <button 
                    type="button" 
                    class="btn btn-sm btn-primary px-3 fw-semibold d-flex align-items-center gap-2" 
                    onclick={handleSaveParquetDataset} 
                    disabled={isParquetSaving || !parquetName.trim() || (parquetLocation === 'url' ? !parquetUrl.trim() : !parquetFiles || parquetFiles.length === 0)}>
                    {#if isParquetSaving}
                        <i class="fa-solid fa-spinner fa-spin"></i> Saving...
                    {:else}
                        <i class="fa-solid fa-check"></i> Save & Select
                    {/if}
                </button>
            </div>
        </div>
    </div>
</div>

<!-- 2. Dedicated Convert Traditional RDF Dataset Modal -->
<div class="modal fade" id="convertRdfModal" tabindex="-1" aria-labelledby="convertRdfModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-lg modal-dialog-scrollable">
        <div class="modal-content border-0 shadow">
            <div class="modal-header bg-light border-bottom py-3 px-4 d-flex align-items-center justify-content-between">
                <div class="d-flex align-items-center gap-2">
                    <i class="fa-solid fa-wand-magic-sparkles text-brown fs-5"></i>
                    <h5 class="modal-title pane-heading mb-0" id="convertRdfModalLabel">Convert RDF Dataset to Parquet</h5>
                </div>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>

            <div class="modal-body p-4 d-flex flex-column gap-3">
                {#if rdfErrorMsg}
                    <div class="alert alert-danger py-2 px-3 small d-flex align-items-center gap-2 mb-0">
                        <i class="fa-solid fa-circle-exclamation flex-shrink-0"></i>
                        <span>{rdfErrorMsg}</span>
                    </div>
                {/if}

                <div>
                    <label for="rdfDsFile" class="form-label small fw-bold">Upload RDF File <span class="text-danger">*</span></label>
                    <input id="rdfDsFile" type="file" class="form-control form-control-sm" bind:files={rdfFiles} onchange={handleRdfFileChange} accept=".ttl,.nt,.nq,.trig,.rdf,.owl,.xml,.n3">
                    <div class="text-muted mt-1" style="font-size: 0.72rem;">
                        Supported RDF formats: Turtle (<code>.ttl</code>), N-Triples (<code>.nt</code>), N-Quads (<code>.nq</code>), TriG (<code>.trig</code>), RDF/XML (<code>.rdf</code>, <code>.xml</code>), Notation3 (<code>.n3</code>).
                    </div>
                </div>

                <div>
                    <label for="rdfDsName" class="form-label small fw-bold">Target Dataset Name <span class="text-danger">*</span></label>
                    <input id="rdfDsName" type="text" class="form-control form-control-sm" bind:value={rdfName} placeholder="e.g. LUBM 100K">
                </div>

                <div class="border rounded-3 p-3 bg-light d-flex flex-column gap-3">
                    <div class="d-flex align-items-center justify-content-between border-bottom pb-2">
                        <strong class="text-dark small d-flex align-items-center gap-2">
                            <i class="fa-solid fa-sliders text-brown"></i>
                            Parquet Target Options
                        </strong>
                        <span class="badge bg-primary-subtle text-primary border border-primary-subtle">
                            Output: Parquet
                        </span>
                    </div>

                    <div class="row g-3">
                        <div class="col-12 col-md-6">
                            <label for="rdfEncodingSelect" class="form-label small fw-bold mb-1">Quad Storage Encoding</label>
                            <select id="rdfEncodingSelect" class="form-select form-select-sm" bind:value={rdfEncoding}>
                                <option value="String">String (Standard string representation)</option>
                                <option value="PlainTerm">PlainTerm (Structured datatype &amp; value fields)</option>
                            </select>
                            <div class="text-muted mt-1" style="font-size: 0.72rem;">Specifies how RDF terms are stored within Parquet columns.</div>
                        </div>

                        <div class="col-12 col-md-6">
                            <label for="rdfSortOrderSelect" class="form-label small fw-bold mb-1">Quad Sort Order</label>
                            <select id="rdfSortOrderSelect" class="form-select form-select-sm" bind:value={rdfSortOrder}>
                                <option value="GPOS">GPOS (Graph, Predicate, Object, Subject - Default)</option>
                                <option value="GSPO">GSPO (Graph, Subject, Predicate, Object)</option>
                                <option value="SPOG">SPOG (Subject, Predicate, Object, Graph)</option>
                                <option value="POSG">POSG (Predicate, Object, Subject, Graph)</option>
                                <option value="OSPG">OSPG (Object, Subject, Predicate, Graph)</option>
                                <option value="None">None (Unsorted)</option>
                                <option value="custom">Custom Expression...</option>
                            </select>
                            <div class="text-muted mt-1" style="font-size: 0.72rem;">Sort order for dictionary compression and filter pushdown.</div>
                        </div>

                        {#if rdfSortOrder === 'custom'}
                            <div class="col-12">
                                <label for="rdfSortOrderCustom" class="form-label small fw-bold mb-1">Custom Sort Order Expression</label>
                                <input id="rdfSortOrderCustom" type="text" class="form-control form-control-sm" bind:value={rdfSortOrderCustom} placeholder="e.g. Native(GPOS) or SPO">
                                <div class="text-muted mt-1" style="font-size: 0.72rem;">Specify sequence of quad components (G, S, P, O) or a Native() expression.</div>
                            </div>
                        {/if}
                    </div>

                    <div class="small text-muted d-flex align-items-center gap-2">
                        <i class="fa-solid fa-circle-info text-info"></i>
                        <span>The RDF file is parsed and converted entirely locally in your browser using WebAssembly.</span>
                    </div>
                </div>
            </div>

            <div class="modal-footer bg-light border-top py-2 px-4 d-flex justify-content-end align-items-center gap-2">
                <button type="button" class="btn btn-sm btn-secondary px-3" data-bs-dismiss="modal" disabled={isRdfConverting}>Cancel</button>
                <button 
                    type="button" 
                    class="btn btn-sm btn-primary px-3 fw-semibold d-flex align-items-center gap-2" 
                    onclick={handleConvertRdfDataset} 
                    disabled={isRdfConverting || !rdfName.trim() || !rdfFiles || rdfFiles.length === 0}>
                    {#if isRdfConverting}
                        <i class="fa-solid fa-spinner fa-spin"></i> Converting...
                    {:else}
                        <i class="fa-solid fa-wand-magic-sparkles"></i> Convert & Select
                    {/if}
                </button>
            </div>
        </div>
    </div>
</div>
