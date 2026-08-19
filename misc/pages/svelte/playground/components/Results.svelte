<script>
    import { queryResults, activeQueryText } from '../store.js';
    
    let activeTab = $state('data');
    let resultsData = $derived($queryResults.results ?? $queryResults.data);

    const DEFAULT_PREFIXES = {
        'rdf:': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'rdfs:': 'http://www.w3.org/2000/01/rdf-schema#',
        'xsd:': 'http://www.w3.org/2001/XMLSchema#'
    };

    // Only render IRIs as links when the scheme is known-safe (avoids javascript: etc.)
    function isSafeIri(iri) {
        return /^(https?|urn):/i.test(iri);
    }

    function extractPrefixes(queryText) {
        const prefixes = { ...DEFAULT_PREFIXES };
        if (!queryText) return prefixes;

        const regex = /(?:PREFIX|prefix)\s+([a-zA-Z0-9_.-]*:)\s*<([^>]+)>/g;
        let match;
        while ((match = regex.exec(queryText)) !== null) {
            prefixes[match[1]] = match[2];
        }
        return prefixes;
    }

    let prefixes = $derived(extractPrefixes($activeQueryText));

    function formatIri(iri, currentPrefixes) {
        const cleanIri = iri.startsWith('<') && iri.endsWith('>') ? iri.slice(1, -1) : iri;

        for (const [prefix, uri] of Object.entries(currentPrefixes)) {
            if (cleanIri.startsWith(uri)) {
                const localName = cleanIri.slice(uri.length);
                return {
                    fullIri: cleanIri,
                    display: `${prefix}${localName}`
                };
            }
        }

        let lastSegment = cleanIri;
        const hashIdx = cleanIri.lastIndexOf('#');
        if (hashIdx !== -1 && hashIdx < cleanIri.length - 1) {
            lastSegment = cleanIri.slice(hashIdx + 1);
        } else {
            const slashIdx = cleanIri.lastIndexOf('/');
            if (slashIdx !== -1 && slashIdx < cleanIri.length - 1) {
                lastSegment = cleanIri.slice(slashIdx + 1);
            }
        }

        return {
            fullIri: cleanIri,
            display: lastSegment || cleanIri
        };
    }

    function parseRdfTerm(cell, currentPrefixes) {
        if (cell === null || cell === undefined) {
            return { type: 'null' };
        }
        if (typeof cell !== 'string') {
            return { type: 'raw', value: String(cell) };
        }

        const str = cell.trim();
        if (!str) {
            return { type: 'raw', value: '' };
        }

        // 1. IRI in angle brackets: <http://...>
        if (str.startsWith('<') && str.endsWith('>')) {
            const iri = formatIri(str, currentPrefixes);
            if (!isSafeIri(iri.fullIri)) {
                return { type: 'raw', value: str };
            }
            return {
                type: 'iri',
                fullIri: iri.fullIri,
                display: iri.display
            };
        }

        // 2. Typed literal: "..."^^<...>
        const typedMatch = str.match(/^"([\s\S]*)"\^\^<([^>]+)>$/);
        if (typedMatch) {
            const value = typedMatch[1];
            const dtIri = formatIri(typedMatch[2], currentPrefixes);
            if (!isSafeIri(dtIri.fullIri)) {
                return { type: 'raw', value: str };
            }
            return {
                type: 'typed_literal',
                value,
                datatypeFullIri: dtIri.fullIri,
                datatypeDisplay: dtIri.display
            };
        }

        // 3. Language tagged literal: "..."@lang
        const langMatch = str.match(/^"([\s\S]*)"@([a-zA-Z0-9-]+)$/);
        if (langMatch) {
            return {
                type: 'lang_literal',
                value: langMatch[1],
                lang: langMatch[2]
            };
        }

        // 4. Plain quoted literal: "..."
        const plainMatch = str.match(/^"([\s\S]*)"$/);
        if (plainMatch) {
            return {
                type: 'literal',
                value: plainMatch[1]
            };
        }

        // 5. Blank node: _:...
        if (str.startsWith('_:')) {
            return {
                type: 'bnode',
                value: str
            };
        }

        // 6. Bare URI: http://... or https://...
        if (str.startsWith('http://') || str.startsWith('https://')) {
            const iri = formatIri(str, currentPrefixes);
            return {
                type: 'iri',
                fullIri: iri.fullIri,
                display: iri.display
            };
        }

        return {
            type: 'raw',
            value: str
        };
    }

    let visibleRows = $derived(
        Array.isArray(resultsData?.solutions)
            ? resultsData.solutions.slice(0, 100).map(row => 
                row.map(cell => parseRdfTerm(cell, prefixes))
            )
            : []
    );
</script>

<div class="h-100 d-flex flex-column">
    {#if $queryResults.isExecuting}
        <div class="text-center text-muted mt-4">
            <i class="fa-solid fa-circle-notch fa-spin fa-2x"></i><br>
            Executing...
        </div>
    {:else if $queryResults.error}
        <div class="alert alert-danger">
            <i class="fa-solid fa-triangle-exclamation me-2"></i> Error executing query:<br>
            <span class="font-monospace small">{$queryResults.error}</span>
        </div>
    {:else if resultsData}
        <div class="d-flex flex-column flex-md-row justify-content-between align-items-start align-items-md-end mb-3 gap-2">
            <div class="d-flex bg-light rounded-3 p-1 overflow-x-auto" style="border: 1px solid #dee2e6; max-width: 100%; scrollbar-width: none;">
                <button class="btn btn-sm text-nowrap {activeTab === 'data' ? 'bg-brown text-white shadow-sm' : 'text-muted'} border-0 rounded-2 pane-heading py-1 flex-shrink-0" onclick={() => activeTab = 'data'}><i class="fa-solid fa-table me-1"></i> Data</button>
                <button class="btn btn-sm text-nowrap {activeTab === 'logical' ? 'bg-brown text-white shadow-sm' : 'text-muted'} border-0 rounded-2 pane-heading py-1 flex-shrink-0" onclick={() => activeTab = 'logical'}><i class="fa-solid fa-code-branch me-1"></i> Logical Plan</button>
                <button class="btn btn-sm text-nowrap {activeTab === 'optimized' ? 'bg-brown text-white shadow-sm' : 'text-muted'} border-0 rounded-2 pane-heading py-1 flex-shrink-0" onclick={() => activeTab = 'optimized'}><i class="fa-solid fa-wand-magic-sparkles me-1"></i> Optimized Plan</button>
                <button class="btn btn-sm text-nowrap {activeTab === 'execution' ? 'bg-brown text-white shadow-sm' : 'text-muted'} border-0 rounded-2 pane-heading py-1 flex-shrink-0" onclick={() => activeTab = 'execution'}><i class="fa-solid fa-microchip me-1"></i> Execution Plan</button>
            </div>
            
            {#if $queryResults.totalSeconds}
                <div class="text-muted small pb-2 pe-2 d-flex align-items-center flex-wrap gap-3">
                    {#if resultsData.solutions}
                        <span title="Total number of rows the query returned (rows beyond the shown set are evaluated but not transferred to the UI).">
                            <strong><i class="fa-solid fa-list-ol me-1"></i> {resultsData.total_count ?? resultsData.solutions.length}</strong> rows{#if resultsData.total_count != null && resultsData.total_count > resultsData.solutions.length}&nbsp;<span class="fst-italic text-secondary">(first {resultsData.solutions.length} shown)</span>{/if}
                        </span>
                    {/if}
                    <span title="Query evaluation + conversion to a JSON object, measured in the worker. Does not include time for copying the results to the UI or rendering them, so the observed latency can be higher.">
                        <i class="fa-regular fa-clock me-1"></i> {$queryResults.totalSeconds}s
                    </span>
                </div>
            {/if}
        </div>
        
        <div class="flex-grow-1 overflow-auto">
            {#if activeTab === 'data'}
                {#if resultsData.variables && Array.isArray(resultsData.solutions)}
                    {#if resultsData.solutions.length > 0}
                        <table class="table table-sm table-striped table-hover table-bordered text-nowrap mb-0 bg-white" style="font-size: 0.875rem;">
                            <thead class="table-light sticky-top">
                                <tr>
                                    <th scope="col" style="width: 45px;" class="text-center text-muted">#</th>
                                    {#each resultsData.variables as variable (variable)}
                                        <th scope="col">{variable}</th>
                                    {/each}
                                </tr>
                            </thead>
                            <tbody>
                                {#each visibleRows as row, rowIdx (rowIdx)}
                                    <tr>
                                        <td class="text-center text-muted small" style="width: 45px;">{rowIdx + 1}</td>
                                        {#each row as term, colIdx (colIdx)}
                                            <td>
                                                {#if term.type === 'iri'}
                                                    <a href={term.fullIri} target="_blank" rel="noopener noreferrer" class="text-decoration-none text-primary" title={term.fullIri}>{term.display}</a>
                                                {:else if term.type === 'typed_literal'}
                                                    <span>"{term.value}"</span><sup class="ms-1" style="font-size: 0.75em;"><a href={term.datatypeFullIri} target="_blank" rel="noopener noreferrer" class="text-secondary text-decoration-none" title={term.datatypeFullIri}>^^{term.datatypeDisplay}</a></sup>
                                                {:else if term.type === 'lang_literal'}
                                                    <span>"{term.value}"</span><sup class="ms-1 text-muted" style="font-size: 0.75em;">@{term.lang}</sup>
                                                {:else if term.type === 'literal'}
                                                    <span>"{term.value}"</span>
                                                {:else if term.type === 'bnode'}
                                                    <span class="badge bg-light text-secondary border font-monospace fw-normal" style="font-size: 0.8em;">{term.value}</span>
                                                {:else if term.type === 'null'}
                                                    <span class="text-muted fst-italic small">-</span>
                                                {:else}
                                                    {term.value}
                                                {/if}
                                            </td>
                                        {/each}
                                    </tr>
                                {/each}
                            </tbody>
                        </table>
                        {#if resultsData.solutions.length > 100}
                            <div class="text-center text-muted small p-2 bg-light border-bottom border-start border-end rounded-bottom mb-2">
                                <i class="fa-solid fa-eye-slash me-1"></i> Showing first 100 of {resultsData.solutions.length} rows.
                            </div>
                        {/if}
                    {:else}
                        <div class="p-3 text-muted bg-white border rounded h-100 d-flex align-items-center justify-content-center">
                            <span><i class="fa-solid fa-circle-info me-2"></i> Query returned 0 rows.</span>
                        </div>
                    {/if}
                {:else if resultsData.boolean !== undefined}
                    <div class="p-4 bg-white border rounded h-100 d-flex flex-column justify-content-center align-items-center gap-3">
                        <h6 class="text-muted text-uppercase mb-0 tracking-wider">ASK Query Result</h6>
                        {#if resultsData.boolean}
                            <div class="badge bg-success fs-5 px-4 py-2"><i class="fa-solid fa-check me-2"></i> TRUE</div>
                        {:else}
                            <div class="badge bg-danger fs-5 px-4 py-2"><i class="fa-solid fa-xmark me-2"></i> FALSE</div>
                        {/if}
                    </div>
                {:else}
                    <pre class="bg-white border rounded p-3 text-dark font-monospace h-100 mb-0" style="white-space: pre; font-size: 0.875rem;">{typeof resultsData === 'string' ? resultsData : JSON.stringify(resultsData, null, 2)}</pre>
                {/if}
            {:else if activeTab === 'logical'}
                <pre class="bg-white border rounded p-3 text-dark font-monospace h-100 mb-0" style="white-space: pre; font-size: 0.875rem;">{$queryResults.logicalPlan}</pre>
            {:else if activeTab === 'optimized'}
                <pre class="bg-white border rounded p-3 text-dark font-monospace h-100 mb-0" style="white-space: pre; font-size: 0.875rem;">{$queryResults.optimizedPlan}</pre>
            {:else if activeTab === 'execution'}
                <pre class="bg-white border rounded p-3 text-dark font-monospace h-100 mb-0" style="white-space: pre; font-size: 0.875rem;">{$queryResults.executionPlan}</pre>
            {/if}
        </div>
    {:else}
        <div class="text-center text-muted mt-5 opacity-50">
            <i class="fa-solid fa-table fa-3x mb-3"></i><br>
            Run a query to see results here
        </div>
    {/if}
</div>
