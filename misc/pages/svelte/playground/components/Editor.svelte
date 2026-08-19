<script>
    import { onMount, onDestroy, tick } from 'svelte';
    import { createEditor } from '../editor.js';
    import { jsStore, wasmModule, queryResults, activeQueryText, emptyQueryResults } from '../store.js';
    import QueryBrowser from './QueryBrowser.svelte';

    let editorContainer = $state();
    let editorView = $state();
    let unsubscribe;

    let globalErrorListener = (e) => {
        if ($queryResults.isExecuting) {
            let msg;
            if (e.reason) msg = e.reason.message || String(e.reason);
            else if (e.error) msg = e.error.message || String(e.error);
            else msg = e.message || String(e);
            
            // It might be a Wasm panic
            if (msg.includes("unreachable") || msg.includes("RuntimeError")) {
                msg += " (Possible Engine Panic - check browser console)";
            }
            
            queryResults.set({ ...emptyQueryResults(), error: "Fatal Async Error: " + msg, isExecuting: false });
        }
    };

    onMount(() => {
        window.addEventListener('error', globalErrorListener);
        window.addEventListener('unhandledrejection', globalErrorListener);
        
        editorView = createEditor(editorContainer, { doc: $activeQueryText });

        // Watch for changes to the query from the Dataset Selector
        unsubscribe = activeQueryText.subscribe(text => {
            if (editorView && text !== editorView.state.doc.toString()) {
                editorView.dispatch({
                    changes: {from: 0, to: editorView.state.doc.length, insert: text}
                });
            }
        });
    });

    onDestroy(() => {
        window.removeEventListener('error', globalErrorListener);
        window.removeEventListener('unhandledrejection', globalErrorListener);
        if (unsubscribe) unsubscribe();
        if (editorView) editorView.destroy();
    });

    let currentExecutionId = 0;

    async function runQuery() {
        if (!$jsStore) return;
        const query = editorView.state.doc.toString();
        const executionId = ++currentExecutionId;

        // Immediately invalidate previous query results
        queryResults.set({ ...emptyQueryResults(), isExecuting: true });
        // Flush the "Executing..." state to the DOM before running the query, so it is
        // visible even for fast queries (otherwise Svelte batches it away with the result).
        await tick();

        try {
            const res = await $wasmModule.runQuery(query);
            if (executionId !== currentExecutionId) return; // Invalidate stale response
            const totalSeconds = ((res.elapsedMs != null ? res.elapsedMs : 0) / 1000).toFixed(2);

            const results = res.results;
            const explanation = res.explanation;

            queryResults.set({
                ...emptyQueryResults(),
                data: results,
                results,
                logicalPlan: explanation?.initial_logical_plan || "Logical Plan not available.",
                optimizedPlan: explanation?.optimized_logical_plan || "Optimized Plan not available.",
                executionPlan: explanation?.execution_plan || "Execution Plan not available.",
                totalSeconds,
                planningLatencyMs: explanation?.planning_latency_ms !== undefined ? explanation.planning_latency_ms.toFixed(2) : null,
                planningComputeMs: explanation?.planning_compute_ms !== undefined ? explanation.planning_compute_ms.toFixed(2) : null
            });
        } catch (e) {
            if (executionId !== currentExecutionId) return; // Invalidate stale response
            console.error("Query execution error:", e);
            queryResults.set({ ...emptyQueryResults(), error: e.message || String(e) });
        }
    }

    // Cancels the in-flight query. The pending promise is invalidated so its
    // "Query cancelled" rejection is not surfaced as an error. We await the
    // acknowledged cancellation so a subsequent Run is sequenced after teardown.
    async function cancelQuery() {
        await $wasmModule?.cancelQuery();
        currentExecutionId += 1;
        queryResults.set({ ...emptyQueryResults() });
    }
</script>

<style>
    :global(.cm-editor) {
        flex: 1;
        height: 100%;
        min-height: 300px;
        border: 1px solid rgba(230, 204, 178, 0.5);
        border-radius: 0.375rem;
    }
    :global(.cm-scroller) {
        overflow: auto;
        height: 100%;
    }
</style>

<div class="h-100 d-flex flex-column">
    <div class="flex-grow-1 d-flex flex-column" bind:this={editorContainer}></div>
    <div class="mt-3 text-end">
        {#if $queryResults.isExecuting}
            <button class="btn btn-danger px-4 btn-action" onclick={cancelQuery}>
                <i class="fa-solid fa-stop me-1"></i> Cancel
            </button>
        {:else}
            <button class="btn btn-primary px-4 btn-action" onclick={runQuery} disabled={!$jsStore}>
                <i class="fa-solid fa-play me-1"></i> Run
            </button>
        {/if}
    </div>
</div>

<QueryBrowser />
