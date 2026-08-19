<script>
    import { engineSettings } from '../store.js';

    let { 
        settings = $bindable($engineSettings)
    } = $props();

    function addCustomConfig() {
        settings.customConfig = [...(settings.customConfig || []), { key: '', value: '' }];
    }

    function removeCustomConfig(index) {
        settings.customConfig = settings.customConfig.filter((_, i) => i !== index);
    }

    let activeOverridesCount = $derived((settings.customConfig || []).filter(c => c.key?.trim()).length);

    // In the wasm32 build no more than 4 GiB of memory can be used.
    const WASM_MEMORY_LIMIT_MB = 4096; // 4 GiB
    let memoryLimitExceedsWasm = $derived(settings.memoryLimit > WASM_MEMORY_LIMIT_MB);
    let cacheCapacityMb = $derived(((settings.rdfFusion?.dataCacheBlockSizeKb || 0) * (settings.rdfFusion?.dataCacheNumBlocks || 0)) / 1024);
    let cacheExceedsWasm = $derived(settings.rdfFusion?.enableDataCache && cacheCapacityMb > WASM_MEMORY_LIMIT_MB);
</script>

<style>
    .collapse-header {
        cursor: pointer;
        user-select: none;
        transition: background-color 0.15s ease-in-out;
    }
    .collapse-header:hover {
        background-color: #f3ece4 !important;
    }
</style>

<div class="d-flex flex-column gap-3">
    <!-- 1. DataFusion Options (Collapsible) -->
    <div class="border rounded-3 overflow-hidden bg-white shadow-xs">
        <div 
            class="p-3 bg-light d-flex align-items-center justify-content-between collapse-header" 
            data-bs-toggle="collapse" 
            data-bs-target="#settingsDataFusionCollapse" 
            role="button"
            tabindex="0"
            aria-expanded="false" 
            aria-controls="settingsDataFusionCollapse">
            <div>
                <strong class="text-dark">DataFusion Options</strong>
                <span class="text-muted small ms-2 d-none d-sm-inline">({settings.memoryLimit} MB RAM, Filter Pushdown: {settings.dataFusion.enableDynamicFilterPushdown ? 'On' : 'Off'})</span>
            </div>
            <i class="fa-solid fa-chevron-down text-muted small"></i>
        </div>
        <div class="collapse" id="settingsDataFusionCollapse">
            <div class="p-3 border-top bg-white">
                <div class="row g-3 mb-3">
                    <div class="col-md-5">
                        <label class="form-label small fw-bold" for="engine-memory">Memory Limit</label>
                        <div class="input-group input-group-sm">
                            <input id="engine-memory" type="number" class="form-control" bind:value={settings.memoryLimit} step="1">
                            <span class="input-group-text">MB</span>
                        </div>
                        {#if memoryLimitExceedsWasm}
                            <div class="alert alert-warning py-1 px-2 mt-1 mb-0" style="font-size: 0.72rem;">
                                <i class="fa-solid fa-triangle-exclamation me-1"></i>
                                The wasm32 build can use no more than 4 GiB of memory; the configured limit is above this.
                            </div>
                        {/if}
                    </div>
                    <div class="col-md-5">
                        <label class="form-label small fw-bold" for="engine-partitions">Target Partitions</label>
                        <div class="input-group input-group-sm">
                            <input id="engine-partitions" type="number" class="form-control" bind:value={settings.dataFusion.targetPartitions} min="1" step="1">
                        </div>
                        <div class="text-muted mt-1" style="font-size: 0.72rem;">datafusion.execution.target_partitions</div>
                    </div>
                </div>
                <div class="d-flex flex-column gap-2">
                    <div class="form-check form-switch">
                        <input class="form-check-input" type="checkbox" role="switch" id="dfDynamicFilter" bind:checked={settings.dataFusion.enableDynamicFilterPushdown}>
                        <label class="form-check-label small" for="dfDynamicFilter">
                            Enable Dynamic Filter Pushdown
                            <div class="text-muted" style="font-size: 0.75rem;">datafusion.optimizer.enable_dynamic_filter_pushdown</div>
                        </label>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- 2. RDF Fusion Options (Collapsible) -->
    <div class="border rounded-3 overflow-hidden bg-white shadow-xs">
        <div 
            class="p-3 bg-light d-flex align-items-center justify-content-between collapse-header" 
            data-bs-toggle="collapse" 
            data-bs-target="#settingsRdfFusionCollapse" 
            role="button"
            tabindex="0"
            aria-expanded="false" 
            aria-controls="settingsRdfFusionCollapse">
            <div>
                <strong class="text-dark">RDF Fusion Options</strong>
                <span class="text-muted small ms-2 d-none d-sm-inline">
                    (Cache: {settings.rdfFusion.enableDataCache ? `${(((settings.rdfFusion.dataCacheBlockSizeKb || 0) * (settings.rdfFusion.dataCacheNumBlocks || 0)) / 1024).toFixed(0)} MB` : 'Off'})
                </span>
            </div>
            <i class="fa-solid fa-chevron-down text-muted small"></i>
        </div>
        <div class="collapse" id="settingsRdfFusionCollapse">
            <div class="p-3 border-top bg-white">
                <div class="card border bg-light-subtle rounded-3">
                    <div class="card-body p-3">
                        <div class="d-flex justify-content-between align-items-start mb-2">
                            <div>
                                <span class="fw-bold small d-block">Parquet Data Page Cache</span>
                                <span class="text-muted" style="font-size: 0.75rem;">Caches decompressed Parquet data blocks in memory across queries.</span>
                            </div>
                            <div class="form-check form-switch ms-3 mb-0">
                                <input class="form-check-input" type="checkbox" role="switch" id="rfDataCache" bind:checked={settings.rdfFusion.enableDataCache}>
                            </div>
                        </div>
                        
                        <div class="pt-2 border-top mt-2 {settings.rdfFusion.enableDataCache ? '' : 'opacity-50'}">
                            <div class="row g-3">
                                <div class="col-md-6">
                                    <label class="form-label small fw-bold" for="rf-block-size">Block Size</label>
                                    <div class="input-group input-group-sm">
                                        <input id="rf-block-size" type="number" class="form-control" bind:value={settings.rdfFusion.dataCacheBlockSizeKb} min="4" step="64" disabled={!settings.rdfFusion.enableDataCache}>
                                        <span class="input-group-text">KB</span>
                                    </div>
                                    <div class="text-muted mt-1" style="font-size: 0.72rem;">rdf_fusion.storage.parquet.data_cache_block_size</div>
                                </div>
                                <div class="col-md-6">
                                    <label class="form-label small fw-bold" for="rf-num-blocks">Number of Blocks</label>
                                    <div class="input-group input-group-sm">
                                        <input id="rf-num-blocks" type="number" class="form-control" bind:value={settings.rdfFusion.dataCacheNumBlocks} min="1" step="64" disabled={!settings.rdfFusion.enableDataCache}>
                                        <span class="input-group-text">blocks</span>
                                    </div>
                                    <div class="text-muted mt-1" style="font-size: 0.72rem;">rdf_fusion.storage.parquet.data_cache_num_blocks</div>
                                </div>
                            </div>
                            {#if settings.rdfFusion.enableDataCache}
                                <div class="mt-2 text-muted small" style="font-size: 0.75rem;">
                                    <i class="fa-solid fa-memory me-1"></i> Total Cache Capacity: <strong>{(((settings.rdfFusion.dataCacheBlockSizeKb || 0) * (settings.rdfFusion.dataCacheNumBlocks || 0)) / 1024).toFixed(1)} MB</strong>
                                </div>
                                {#if cacheExceedsWasm}
                                    <div class="alert alert-warning py-1 px-2 mt-2 mb-0" style="font-size: 0.72rem;">
                                        <i class="fa-solid fa-triangle-exclamation me-1"></i>
                                        The wasm32 build can use no more than 4 GiB of memory; the configured cache size is above this.
                                    </div>
                                {/if}
                            {/if}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- 3. Metrics & Statistics (Collapsible) -->
    <div class="border rounded-3 overflow-hidden bg-white shadow-xs">
        <div 
            class="p-3 bg-light d-flex align-items-center justify-content-between collapse-header" 
            data-bs-toggle="collapse" 
            data-bs-target="#settingsMetricsCollapse" 
            role="button"
            tabindex="0"
            aria-expanded="false" 
            aria-controls="settingsMetricsCollapse">
            <div>
                <strong class="text-dark">Metrics & Statistics</strong>
                <span class="text-muted small ms-2 d-none d-sm-inline">
                    (Metrics: {settings.metrics.showMetrics ? 'On' : 'Off'}, Statistics: {settings.metrics.showStatistics ? 'On' : 'Off'})
                </span>
            </div>
            <i class="fa-solid fa-chevron-down text-muted small"></i>
        </div>
        <div class="collapse" id="settingsMetricsCollapse">
            <div class="p-3 border-top bg-white">
                <div class="d-flex flex-column gap-2">
                    <div class="form-check form-switch">
                        <input class="form-check-input" type="checkbox" role="switch" id="showMetrics" bind:checked={settings.metrics.showMetrics}>
                        <label class="form-check-label small" for="showMetrics">Show Metrics in Execution Plan</label>
                    </div>
                    <div class="form-check form-switch">
                        <input class="form-check-input" type="checkbox" role="switch" id="showStatistics" bind:checked={settings.metrics.showStatistics}>
                        <label class="form-check-label small" for="showStatistics">Show Statistics in Execution Plans</label>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- 4. Custom Config Overrides (Collapsible) -->
    <div class="border rounded-3 overflow-hidden bg-white shadow-xs">
        <div 
            class="p-3 bg-light d-flex align-items-center justify-content-between collapse-header" 
            data-bs-toggle="collapse" 
            data-bs-target="#settingsCustomConfigCollapse" 
            role="button"
            tabindex="0"
            aria-expanded="false" 
            aria-controls="settingsCustomConfigCollapse">
            <div>
                <strong class="text-dark">Custom Config Overrides</strong>
                <span class="text-muted small ms-2 d-none d-sm-inline">
                    ({activeOverridesCount} {activeOverridesCount === 1 ? 'Override' : 'Overrides'})
                </span>
            </div>
            <i class="fa-solid fa-chevron-down text-muted small"></i>
        </div>
        <div class="collapse" id="settingsCustomConfigCollapse">
            <div class="p-3 border-top bg-white">
                {#if (settings.customConfig || []).length === 0}
                    <div class="text-muted small mb-2">No custom configuration overrides defined.</div>
                {:else}
                    {#each settings.customConfig as conf, i (i)}
                        <div class="d-flex mb-2 gap-2 align-items-center">
                            <input type="text" class="form-control form-control-sm" placeholder="Config key (e.g. datafusion.execution.batch_size)" bind:value={conf.key}>
                            <input type="text" class="form-control form-control-sm" placeholder="Value" bind:value={conf.value}>
                            <button class="btn btn-sm btn-outline-danger" onclick={() => removeCustomConfig(i)} aria-label="Remove Config"><i class="fa-solid fa-xmark"></i></button>
                        </div>
                    {/each}
                {/if}
                <button class="btn btn-sm btn-outline-primary mt-1" onclick={addCustomConfig}><i class="fa-solid fa-plus me-1"></i> Add Custom Config</button>
            </div>
        </div>
    </div>
</div>
