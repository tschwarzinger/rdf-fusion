<script>
    import { activeQueryText, clearQueryResults } from '../store.js';
    import { onMount, onDestroy } from 'svelte';
    import { createEditor } from '../editor.js';

    import { queryLibrary } from '../data_queries.js';

    let families = Object.keys(queryLibrary);
    let selectedOption = $state(`${families[0]}|0`);
    
    let activeFamily = $derived(selectedOption.split('|')[0]);
    let activeQueryIdx = $derived(parseInt(selectedOption.split('|')[1], 10));
    
    let queries = $derived(queryLibrary[activeFamily]);
    let activeQuery = $derived(queries[activeQueryIdx]);

    let editorContainer = $state();
    let editorView = $state();

    $effect(() => {
        if (editorView && activeQuery) {
            const currentDoc = editorView.state.doc.toString();
            if (currentDoc !== activeQuery.query) {
                editorView.dispatch({
                    changes: {from: 0, to: currentDoc.length, insert: activeQuery.query}
                });
            }
        }
    });

    onMount(() => {
        editorView = createEditor(editorContainer, { doc: activeQuery ? activeQuery.query : "", readonly: true });
        document.addEventListener('click', onClickOutside);
    });

    onDestroy(() => {
        if (editorView) editorView.destroy();
        document.removeEventListener('click', onClickOutside);
    });

    let dropdownEl = $state();
    let dropdownOpen = $state(false);

    function toggleDropdown() {
        dropdownOpen = !dropdownOpen;
    }

    function pick(family, idx) {
        selectedOption = `${family}|${idx}`;
        dropdownOpen = false;
    }

    function onClickOutside(ev) {
        if (dropdownEl && !dropdownEl.contains(ev.target)) dropdownOpen = false;
    }

    function loadQuery() {
        if (activeQuery) {
            clearQueryResults();
            $activeQueryText = activeQuery.query;
        }
    }
</script>

<style>
    .preview-container :global(.cm-editor) {
        flex: 1;
        height: 100%;
        min-height: 250px;
        background-color: white;
        border: 1px solid #dee2e6;
        border-radius: 0.375rem;
        overflow: hidden;
    }
    .preview-container :global(.cm-scroller) {
        overflow: auto !important;
        height: 100%;
    }
</style>

<div class="modal fade" id="queryBrowserModal" tabindex="-1" aria-hidden="true">
  <div class="modal-dialog modal-xl modal-dialog-scrollable" style="max-width: min(1400px, 96vw);">
    <div class="modal-content border-0 shadow">
      <div class="modal-header bg-white border-bottom pt-3 pb-2">
        <h5 class="modal-title pane-heading"><i class="fa-solid fa-book-open me-2"></i> Example Queries</h5>
        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
      </div>
      <div class="modal-body p-4 bg-light d-flex flex-column overflow-hidden" style="height: 70vh;">
          <div class="mb-3 d-flex flex-column flex-sm-row align-items-start align-items-sm-center gap-2 flex-shrink-0">
              <span class="fw-bold text-nowrap">Select Query:</span>
              <div class="dropdown" bind:this={dropdownEl} style="max-width: 560px; width: 100%;">
                  <button type="button" class="form-select form-select-sm text-start text-truncate"
                          onclick={toggleDropdown}
                          aria-haspopup="listbox" aria-expanded={dropdownOpen}>
                      {activeQuery?.name}
                  </button>
                  {#if dropdownOpen}
                      <div class="dropdown-menu show w-100" role="listbox" style="max-height: 24rem; min-width: 100%; width: max-content; max-width: 100%; overflow-y: auto;">
                          {#each families as family (family)}
                              <h6 class="dropdown-header">{family}</h6>
                              {#each queryLibrary[family] as q, idx (idx)}
                                  <button type="button" class="dropdown-item text-truncate"
                                          role="option"
                                          aria-selected={activeFamily === family && activeQueryIdx === idx}
                                          class:active={activeFamily === family && activeQueryIdx === idx}
                                          onclick={() => pick(family, idx)}>
                                      {q.name}
                                  </button>
                              {/each}
                          {/each}
                      </div>
                  {/if}
              </div>
          </div>
          <div class="flex-grow-1 preview-container d-flex flex-column overflow-auto" bind:this={editorContainer}></div>
      </div>
      <div class="modal-footer bg-white border-top py-2 px-4">
          <button class="btn btn-secondary me-2" data-bs-dismiss="modal">Cancel</button>
          <button class="btn btn-primary px-4 btn-action" data-bs-dismiss="modal" onclick={loadQuery}>
              <i class="fa-solid fa-check me-1"></i> Use Query
          </button>
      </div>
    </div>
  </div>
</div>
