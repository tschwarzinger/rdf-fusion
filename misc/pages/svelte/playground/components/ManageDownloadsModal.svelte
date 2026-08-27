<script>
    import { onMount } from 'svelte';
    import { getAllLocalVersions, deleteLocalVersion, getDownloadedDatasets, deleteDownloadedDataset, getCustomDatasets, deleteCustomDataset, getBlob } from '../db.js';
    import { activeVersionMetadata, activeDatasetMetadata, jsStore, wasmModule, downloadedDatasets, customDatasets, localVersions, reloadStoreTrigger, setStatus } from '../store.js';

    let totalItemsCount = $derived($localVersions.length + $downloadedDatasets.length + $customDatasets.length);

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

    async function loadAllDownloads() {
        try {
            const [versions, datasets, customs] = await Promise.all([
                getAllLocalVersions(),
                getDownloadedDatasets(),
                getCustomDatasets()
            ]);
            $localVersions = versions || [];
            $downloadedDatasets = datasets || [];
            $customDatasets = customs || [];
        } catch (e) {
            console.error("Failed to load downloads:", e);
        }
    }

    async function handleDeleteVersion(id) {
        if (confirm(`Delete downloaded engine version "${id}"?`)) {
            await deleteLocalVersion(id);
            await loadAllDownloads();
            
            // If active version was deleted, reset WASM module
            if ($activeVersionMetadata?.id === id) {
                $wasmModule = null;
                $jsStore = null;
                $activeVersionMetadata = null;
                setStatus('Engine version deleted. Please select a version.', 'fa-hand-pointer', 'brown');
            }
        }
    }

    async function handleDeleteDownloadedDataset(id) {
        if (confirm(`Delete downloaded dataset "${id}"?`)) {
            await deleteDownloadedDataset(id);
            await loadAllDownloads();

            // If active dataset was deleted, reload store to fallback to remote URL
            if ($activeDatasetMetadata?.id === id) {
                reloadStoreTrigger.update(n => n + 1);
            }
        }
    }

    async function handleDeleteCustomDataset(id) {
        if (confirm(`Delete custom dataset "${id}"?`)) {
            await deleteCustomDataset(id);
            await loadAllDownloads();

            if ($activeDatasetMetadata?.id === id) {
                $jsStore = null;
                $activeDatasetMetadata = null;
                setStatus('Dataset deleted. Please select a dataset.', 'fa-hand-pointer', 'info');
            }
        }
    }

    onMount(() => {
        loadAllDownloads();

        const modalEl = document.getElementById('manageDownloadsModal');
        if (modalEl) {
            modalEl.addEventListener('show.bs.modal', loadAllDownloads);
        }
    });
</script>

<!-- Manage Local Data Modal -->
<div class="modal fade" id="manageDownloadsModal" tabindex="-1" aria-labelledby="manageDownloadsModalLabel" aria-hidden="true">
    <div class="modal-dialog modal-lg modal-dialog-scrollable">
        <div class="modal-content border-0 shadow">
            <div class="modal-header bg-light border-bottom py-3 px-4 d-flex align-items-center justify-content-between">
                <div class="d-flex align-items-center gap-2">
                    <i class="fa-solid fa-hard-drive text-brown fs-5"></i>
                    <h5 class="modal-title pane-heading mb-0" id="manageDownloadsModalLabel">Manage Local Data</h5>
                </div>
                <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
            </div>

            <div class="modal-body p-4 d-flex flex-column gap-3">
                {#if totalItemsCount === 0}
                    <div class="p-4 text-center text-muted bg-light rounded-3">
                        <i class="fa-solid fa-hard-drive fa-2x mb-2 text-secondary opacity-50"></i>
                        <p class="mb-0">No engine builds, downloaded datasets, or custom datasets found in local storage.</p>
                    </div>
                {:else}
                    <!-- Engine Versions Section -->
                    <div class="border rounded-3 overflow-hidden bg-white">
                        <div class="p-3 bg-light border-bottom d-flex align-items-center justify-content-between">
                            <div class="d-flex align-items-center gap-2">
                                <i class="fa-solid fa-microchip text-brown fs-6"></i>
                                <strong class="text-dark">Downloaded Engine Builds</strong>
                            </div>
                            <span class="badge bg-secondary-subtle text-dark border">{$localVersions.length}</span>
                        </div>
                        <div class="p-3">
                            {#if $localVersions.length === 0}
                                <div class="text-muted small text-center p-2">No local engine versions.</div>
                            {:else}
                                <ul class="list-group list-group-flush w-100">
                                    {#each $localVersions as v (v.id)}
                                        <li class="list-group-item d-flex justify-content-between align-items-center px-0 py-2">
                                            <div>
                                                <strong>{v.customName || v.id}</strong>
                                                {#if $activeVersionMetadata?.id === v.id}
                                                    <span class="badge bg-success-subtle text-success border border-success-subtle ms-2">Active</span>
                                                {/if}
                                                <div class="small text-muted" style="font-size: 0.8rem;">
                                                    Downloaded on {new Date(v.timestamp).toLocaleString()}
                                                    {#if v.wasmBlob?.size}
                                                        • {((v.wasmBlob.size + (v.jsBlob?.size || 0)) / (1024 * 1024)).toFixed(2)} MB
                                                    {/if}
                                                </div>
                                            </div>
                                            <button 
                                                class="btn btn-sm btn-outline-danger" 
                                                onclick={() => handleDeleteVersion(v.id)} 
                                                title="Delete local version"
                                                aria-label="Delete local engine version">
                                                <i class="fa-solid fa-trash"></i>
                                            </button>
                                        </li>
                                    {/each}
                                </ul>
                            {/if}
                        </div>
                    </div>

                    <!-- Downloaded Datasets Section -->
                    <div class="border rounded-3 overflow-hidden bg-white">
                        <div class="p-3 bg-light border-bottom d-flex align-items-center justify-content-between">
                            <div class="d-flex align-items-center gap-2">
                                <i class="fa-solid fa-database text-brown fs-6"></i>
                                <strong class="text-dark">Downloaded Library Datasets</strong>
                            </div>
                            <span class="badge bg-secondary-subtle text-dark border">{$downloadedDatasets.length}</span>
                        </div>
                        <div class="p-3">
                            {#if $downloadedDatasets.length === 0}
                                <div class="text-muted small text-center p-2">No library datasets downloaded.</div>
                            {:else}
                                <ul class="list-group list-group-flush w-100">
                                    {#each $downloadedDatasets as ds (ds.id)}
                                        <li class="list-group-item d-flex justify-content-between align-items-center px-0 py-2">
                                            <div class="overflow-hidden me-2">
                                                <div class="text-truncate">
                                                    <strong>{ds.name}</strong>
                                                    {#if $activeDatasetMetadata?.id === ds.id}
                                                        <span class="badge bg-success-subtle text-success border border-success-subtle ms-2">Active</span>
                                                    {/if}
                                                </div>
                                                <div class="small text-muted text-break" style="font-size: 0.8rem;">
                                                    {#if ds.timestamp}Downloaded on {new Date(ds.timestamp).toLocaleString()}{:else}{ds.originalUrl || ds.url}{/if}
                                                    {#if (ds.size ?? ds.fileBlob?.size)}
                                                        • {(((ds.size ?? ds.fileBlob?.size ?? 0)) / (1024 * 1024)).toFixed(2)} MB
                                                    {/if}
                                                </div>
                                            </div>
                                            <button 
                                                class="btn btn-sm btn-outline-danger" 
                                                onclick={() => handleDeleteDownloadedDataset(ds.id)} 
                                                title="Delete downloaded dataset"
                                                aria-label="Delete downloaded dataset">
                                                <i class="fa-solid fa-trash"></i>
                                            </button>
                                        </li>
                                    {/each}
                                </ul>
                            {/if}
                        </div>
                    </div>

                    <!-- Custom Uploaded Datasets Section -->
                    {#if $customDatasets.length > 0}
                        <div class="border rounded-3 overflow-hidden bg-white">
                            <div class="p-3 bg-light border-bottom d-flex align-items-center justify-content-between">
                                <div class="d-flex align-items-center gap-2">
                                    <i class="fa-solid fa-file-arrow-up text-brown fs-6"></i>
                                    <strong class="text-dark">Custom User Datasets</strong>
                                </div>
                                <span class="badge bg-secondary-subtle text-dark border">{$customDatasets.length}</span>
                            </div>
                            <div class="p-3">
                                <ul class="list-group list-group-flush w-100">
                                    {#each $customDatasets as ds (ds.id)}
                                        <li class="list-group-item d-flex justify-content-between align-items-center px-0 py-2">
                                            <div class="overflow-hidden me-2">
                                                <div class="text-truncate">
                                                    <strong>{ds.name}</strong>
                                                    {#if $activeDatasetMetadata?.id === ds.id}
                                                        <span class="badge bg-success-subtle text-success border border-success-subtle ms-2">Active</span>
                                                    {/if}
                                                </div>
                                                <div class="small text-muted" style="font-size: 0.8rem;">
                                                    {ds.sourceType === 'file' ? 'Local File' : 'Custom URL'}
                                                    {#if ds.encoding}
                                                        • {ds.encoding}
                                                    {/if}
                                                    {#if ds.sortOrder}
                                                        • {ds.sortOrder}
                                                    {/if}
                                                    {#if (ds.size ?? ds.fileBlob?.size)}
                                                        • {(((ds.size ?? ds.fileBlob?.size ?? 0)) / (1024 * 1024)).toFixed(2)} MB
                                                    {/if}
                                                </div>
                                            </div>
                                            <div class="d-flex align-items-center gap-1 flex-shrink-0">
                                                {#if ds.sourceType === 'file'}
                                                    <button 
                                                        class="btn btn-sm btn-outline-secondary" 
                                                        onclick={() => exportParquetFile(ds.id, ds.name)} 
                                                        title="Export Parquet file"
                                                        aria-label="Export Parquet file">
                                                        <i class="fa-solid fa-file-arrow-down"></i>
                                                    </button>
                                                {/if}
                                                <button 
                                                    class="btn btn-sm btn-outline-danger" 
                                                    onclick={() => handleDeleteCustomDataset(ds.id)} 
                                                    title="Delete custom dataset"
                                                    aria-label="Delete custom dataset">
                                                    <i class="fa-solid fa-trash"></i>
                                                </button>
                                            </div>
                                        </li>
                                    {/each}
                                </ul>
                            </div>
                        </div>
                    {/if}
                {/if}
            </div>

            <div class="modal-footer bg-light border-top py-2 px-4">
                <button type="button" class="btn btn-sm btn-secondary px-3" data-bs-dismiss="modal">Close</button>
            </div>
        </div>
    </div>
</div>
