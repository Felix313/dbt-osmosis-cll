(function() {
    let graphInstance = null;
    let exploreController = null;

    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text == null ? '' : String(text);
        return div.innerHTML;
    }

    // Where this session's lineage came from. A cached source can be scoped to a
    // selector or predate the running code, so say so rather than letting the graph
    // look like the whole, current project.
    function renderSourceBadge() {
        const container = document.getElementById('lineageSourceBadge');
        if (!container) return;

        fetch('/api/context')
            .then(response => response.json())
            .then(context => {
                if (!context || context.mode === 'live') return;

                const parts = [];
                if (context.mode === 'selector') {
                    const selectors = (context.selectors || []).join(' ');
                    parts.push(`Scoped to <strong>${escapeHtml(selectors || 'a selector')}</strong>`);
                    parts.push(`${context.models} model(s) — the rest of the project is not shown`);
                } else {
                    parts.push(`Cached lineage — <strong>${context.models}</strong> model(s)`);
                }
                if (context.stale_models && context.stale_models.length) {
                    parts.push(`${context.stale_models.length} model(s) changed since the cache was written and are re-parsed on access`);
                }
                if (context.has_sql_expressions === false) {
                    parts.push('SQL expressions unavailable (cache predates schema v5)');
                }

                container.innerHTML = `
                    <div class="lineage-source-badge">
                        <div class="lineage-source-badge-title">${context.mode === 'selector' ? 'Selector scope' : 'Cached source'}</div>
                        <div class="lineage-source-badge-body">${parts.join('<br>')}</div>
                        <div class="lineage-source-badge-path" title="${escapeHtml(context.source || '')}">${escapeHtml((context.source || '').split(/[\\/]/).pop())}</div>
                    </div>`;
            })
            .catch(() => { /* badge is informational — never block the explorer on it */ });
    }

    function init(initialData, isExploreMode) {
        graphInstance = initGraph(initialData);

        if (isExploreMode) {
            renderSourceBadge();
            const graphInstanceRef = {
                get current() { return graphInstance; },
                set current(value) { graphInstance = value; }
            };
            exploreController = ExploreModule.init(initialData, graphInstanceRef);
            ImpactModule.init();
            ModelDetailsModule.init();
        }
    }

    window.app = {
        init,
        getGraphInstance: () => graphInstance,
        setGraphInstance: (instance) => { graphInstance = instance; },
        getExploreController: () => exploreController
    };
})();
