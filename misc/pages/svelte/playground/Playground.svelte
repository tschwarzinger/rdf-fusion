<script>
    import { jsStore, globalAlert, toasts } from './store.js';
    import StatusCard from './components/StatusCard.svelte';
    import Editor from './components/Editor.svelte';
    import Results from './components/Results.svelte';
    
    // Support manual toast dismissal if needed
    function dismissToast(id) {
        toasts.update(t => t.filter(toast => toast.id !== id));
    }

    let isStoreReady = $derived($jsStore !== null);
</script>

<div class="container-fluid px-3 px-md-4 px-xl-5 mt-4 mb-5" style="max-width: 1560px;">
    <!-- Global Error/Warning Alert -->
    {#if $globalAlert}
        <div class="alert alert-{$globalAlert.color} shadow-sm mb-3 d-flex align-items-center gap-3">
            <i class="fa-solid {$globalAlert.icon} fa-2x"></i>
            <div class="flex-grow-1">
                {$globalAlert.text}
                {#if $globalAlert.errorDetails}
                    <button type="button" class="btn btn-link p-0 fw-bold ms-1 text-decoration-underline align-baseline" onclick={() => window.showErrorModal($globalAlert.errorDetails)}>Details</button>
                {/if}
            </div>
            <button type="button" class="btn-close" aria-label="Close" onclick={() => $globalAlert = null}></button>
        </div>
    {/if}

    <StatusCard />

    <!-- Playground Content -->
    <div class="row g-3 mt-1" style="transition: opacity 0.3s ease; opacity: {isStoreReady ? '1' : '0.5'}; pointer-events: {isStoreReady ? 'auto' : 'none'};">
        <!-- Editor Column -->
        <div class="col-12">
            <div class="card shadow-sm border-0 h-100">
                <div class="card-header bg-white border-bottom-0 pt-3 pb-0 d-flex flex-column flex-md-row justify-content-between align-items-start align-items-md-center gap-2">
                    <h5 class="pane-heading mb-0"><i class="fa-solid fa-pen-to-square me-2"></i> Query</h5>
                    <button class="btn btn-sm btn-outline-primary btn-action d-flex align-items-center justify-content-center align-self-stretch align-self-md-auto gap-2 mt-2 mt-md-0" data-bs-toggle="modal" data-bs-target="#queryBrowserModal">
                        <i class="fa-solid fa-wand-magic-sparkles"></i> Load Example Query
                    </button>
                </div>
                <div class="card-body d-flex flex-column" style="min-height: 400px;">
                    <Editor />
                </div>
            </div>
        </div>

        <!-- Results Column -->
        <div class="col-12">
            <div class="card shadow-sm border-0 h-100 bg-light">
                <div class="card-header bg-transparent border-bottom-0 pt-3 pb-0">
                    <h5 class="pane-heading mb-0"><i class="fa-solid fa-table me-2"></i> Results</h5>
                </div>
                <div class="card-body">
                    <Results />
                </div>
            </div>
        </div>
    </div>
</div>

<!-- Global Toasts -->
<div class="toast-container position-fixed bottom-0 end-0 p-3" style="z-index: 1100">
    {#each $toasts as t (t.id)}
        <div class="toast show align-items-center text-bg-{t.color} border-0 mb-2 shadow" role="alert" aria-live="assertive" aria-atomic="true">
            <div class="d-flex">
                <div class="toast-body fw-bold">
                    {#if t.icon}
                        <i class="fa-solid {t.icon} me-2"></i>
                    {/if}
                    {t.text}
                </div>
                <button type="button" class="btn-close btn-close-white me-2 m-auto" onclick={() => dismissToast(t.id)} aria-label="Close"></button>
            </div>
        </div>
    {/each}
</div>
