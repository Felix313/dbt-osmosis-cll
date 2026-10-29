// UI of the data-product column-requirement report (data: CLL_REPORT_DATA, logic: CllEngine).
(function () {
  "use strict";

  var I18N = {
    en: {
      title: "Data-product column requirements",
      generated: "Generated", selectors: "Scope",
      dpHeading: "Data products",
      dpHint: "Lineage stops at data products: they become the origin for everything downstream. Every column of a data product must be rebuildable.",
      dpAdd: "Add a model as data product…", reset: "Reset to configured",
      exportCfg: ".osmosis-cll snippet", exportCsv: "CSV",
      notInScope: "not in the analysed scope",
      tabs: { overview: "Overview", waves: "Build order", models: "Models", origins: "Origins", sources: "Sources", quality: "Quality" },
      coverageOk: "Lineage coverage: every analysed column is traced.",
      coverageWarn: "Lineage coverage is incomplete — affected models may keep or drop the wrong columns:",
      unresolved: "columns whose SQL could not be traced",
      dangling: "references to columns the upstream model does not output (lineage attribution errors)",
      noColumns: "relations without known columns",
      docOnly: "columns documented in YAML but not produced by the SQL (ignored)",
      tiles: { dps: "data products", waves: "build waves", needed: "interim models needed",
        unneeded: "interim models not needed", req: "interim columns required",
        drop: "interim columns droppable", src: "source columns required" },
      wave: "Wave", reads: "reads", columns: "columns", noDeps: "only interim models / sources",
      dbtOnly: "dbt dependency without column use", cycle: "cycle",
      model: "Model", role: "Role", layer: "Layer", mat: "Materialization", cols: "Columns",
      required: "required", droppable: "droppable", unused: "unused", neededBy: "Needed by",
      column: "Column", status: "Status", category: "Kind", usages: "Used as", origin: "Origin",
      roles: { data_product: "data product", interim: "interim", source: "source" },
      cats: { data: "data", logic: "logic" },
      search: "Search…", allRoles: "all roles", allLayers: "all layers",
      onlyDrop: "only models with droppable columns", onlyNeeded: "only needed",
      showing: "showing {n} of {m}", more: "show more",
      dp: "Data product", kind: "Kind", fromDp: "data product", fromSource: "source",
      constant: "constant / no column input",
      sourceRel: "Source relation", documented: "documented",
      gapsUnresolved: "Columns without traced SQL", gapsNoInput: "Computed without column input (complete)",
      danglingRefs: "References to columns not output upstream", docOnlyCols: "Documented, not produced",
      legend: "data = flows into a data-product column · logic = only needed for joins, filters, QUALIFY, windows, GROUP BY or compile-time queries"
    },
    de: {
      title: "Spaltenbedarf der Datenprodukte",
      generated: "Erstellt", selectors: "Umfang",
      dpHeading: "Datenprodukte",
      dpHint: "Die Lineage endet an Datenprodukten: Sie sind die Herkunft für alles Nachgelagerte. Jede Spalte eines Datenprodukts muss wieder aufbaubar sein.",
      dpAdd: "Modell als Datenprodukt hinzufügen…", reset: "Auf Konfiguration zurücksetzen",
      exportCfg: ".osmosis-cll-Ausschnitt", exportCsv: "CSV",
      notInScope: "nicht im analysierten Umfang",
      tabs: { overview: "Übersicht", waves: "Aufbaureihenfolge", models: "Modelle", origins: "Herkunft", sources: "Quellen", quality: "Qualität" },
      coverageOk: "Lineage-Abdeckung: Jede analysierte Spalte ist nachverfolgt.",
      coverageWarn: "Die Lineage-Abdeckung ist unvollständig – betroffene Modelle können falsche Spalten behalten oder verwerfen:",
      unresolved: "Spalten, deren SQL nicht nachverfolgt werden konnte",
      dangling: "Verweise auf Spalten, die das Upstream-Modell nicht ausgibt (Zuordnungsfehler der Lineage)",
      noColumns: "Relationen ohne bekannte Spalten",
      docOnly: "im YAML dokumentierte, vom SQL nicht erzeugte Spalten (ignoriert)",
      tiles: { dps: "Datenprodukte", waves: "Aufbauwellen", needed: "benötigte Interims-Modelle",
        unneeded: "nicht benötigte Interims-Modelle", req: "benötigte Interims-Spalten",
        drop: "entbehrliche Interims-Spalten", src: "benötigte Quellspalten" },
      wave: "Welle", reads: "liest", columns: "Spalten", noDeps: "nur Interims-Modelle / Quellen",
      dbtOnly: "dbt-Abhängigkeit ohne Spaltennutzung", cycle: "Zyklus",
      model: "Modell", role: "Rolle", layer: "Schicht", mat: "Materialisierung", cols: "Spalten",
      required: "benötigt", droppable: "entbehrlich", unused: "ungenutzt", neededBy: "Benötigt von",
      column: "Spalte", status: "Status", category: "Art", usages: "Genutzt als", origin: "Herkunft",
      roles: { data_product: "Datenprodukt", interim: "Interim", source: "Quelle" },
      cats: { data: "Daten", logic: "Logik" },
      search: "Suchen…", allRoles: "alle Rollen", allLayers: "alle Schichten",
      onlyDrop: "nur Modelle mit entbehrlichen Spalten", onlyNeeded: "nur benötigte",
      showing: "{n} von {m} angezeigt", more: "mehr anzeigen",
      dp: "Datenprodukt", kind: "Art", fromDp: "Datenprodukt", fromSource: "Quelle",
      constant: "Konstante / ohne Spalteneingang",
      sourceRel: "Quellrelation", documented: "dokumentiert",
      gapsUnresolved: "Spalten ohne nachverfolgtes SQL", gapsNoInput: "Ohne Spalteneingang berechnet (vollständig)",
      danglingRefs: "Verweise auf upstream nicht ausgegebene Spalten", docOnlyCols: "Dokumentiert, nicht erzeugt",
      legend: "Daten = fließt in eine Datenprodukt-Spalte · Logik = nur für Joins, Filter, QUALIFY, Fenster, GROUP BY oder Compile-Zeit-Abfragen nötig"
    }
  };
  var USAGE = {
    en: { join_key: "join key", join_filter: "join filter", filter: "filter", qualify: "QUALIFY",
      window: "window", group_by: "GROUP BY", having: "HAVING", compile_time: "compile-time query" },
    de: { join_key: "Join-Schlüssel", join_filter: "Join-Filter", filter: "Filter", qualify: "QUALIFY",
      window: "Fenster", group_by: "GROUP BY", having: "HAVING", compile_time: "Compile-Zeit-Abfrage" }
  };

  var blob = CLL_REPORT_DATA;
  var lang = I18N[blob.lang] ? blob.lang : "en";
  var T = I18N[lang], U = USAGE[lang];
  var graph = CllEngine.expand(blob);
  var ix = CllEngine.index(graph);
  var state = { dps: graph.dataProducts.slice(), tab: "overview", result: null, status: null,
    modelFilter: { q: "", role: "", layer: "", onlyDrop: false, limit: 200 }, originDp: null,
    open: {} };

  function $(id) { return document.getElementById(id); }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }
  function up(s) { return esc(String(s).toUpperCase()); }
  function fmt(n) { return Number(n).toLocaleString(lang === "de" ? "de-DE" : "en-US"); }
  function chip(text, cls) { return '<span class="chip ' + (cls || "") + '">' + esc(text) + "</span>"; }

  function recompute() {
    state.result = CllEngine.analyse(ix, state.dps);
    state.status = CllEngine.columnStatus(ix, state.result);
    var perModel = {};
    state.status.forEach(function (r) {
      var m = perModel[r.model] = perModel[r.model] || { model: r.model, role: r.role, layer: r.layer,
        cols: 0, req: 0, drop: 0, neededBy: {}, rows: [] };
      m.cols++;
      if (r.status === "required") m.req++;
      if (r.status === "droppable") m.drop++;
      r.neededBy.forEach(function (d) { m.neededBy[d] = true; });
      m.rows.push(r);
    });
    state.perModel = perModel;
    render();
  }

  // ---------------------------------------------------------------- sidebar
  function renderSidebar() {
    $("dp-heading").textContent = T.dpHeading;
    $("dp-hint").textContent = T.dpHint;
    $("dp-add").placeholder = T.dpAdd;
    $("dp-reset").textContent = T.reset;
    $("dp-export").textContent = T.exportCfg;
    $("csv-export").textContent = T.exportCsv;
    var missing = {};
    state.result.missing.forEach(function (d) { missing[d] = true; });
    $("dp-list").innerHTML = state.dps.map(function (d) {
      return '<li class="' + (missing[d] ? "missing" : "") + '"><span>' + up(d) +
        (missing[d] ? ' <span class="hint">(' + esc(T.notInScope) + ")</span>" : "") +
        '</span><button class="icon" data-remove="' + esc(d) + '" title="×">✕</button></li>';
    }).join("");
    var chosen = {};
    state.dps.forEach(function (d) { chosen[d] = true; });
    $("dp-candidates").innerHTML = Object.keys(ix.colsOf).sort().filter(function (m) {
      return !chosen[m];
    }).map(function (m) { return '<option value="' + up(m) + '">'; }).join("");
  }

  function addDp(name) {
    name = String(name || "").trim().toLowerCase();
    if (!name || state.dps.indexOf(name) >= 0) return;
    state.dps.push(name);
    recompute();
  }

  // ----------------------------------------------------------------- banner
  function renderBanner() {
    var unresolved = graph.gaps.filter(function (g) { return g[2] === "unresolved"; }).length;
    var dangling = state.result.dangling.length;
    var noCols = (graph.coverage.relations_without_columns || []).length;
    var items = [];
    if (unresolved) items.push(fmt(unresolved) + " " + T.unresolved);
    if (dangling) items.push(fmt(dangling) + " " + T.dangling);
    if (noCols) items.push(fmt(noCols) + " " + T.noColumns);
    var info = graph.documentedOnly.length
      ? '<div class="hint">' + fmt(graph.documentedOnly.length) + " " + esc(T.docOnly) + "</div>" : "";
    $("banner").innerHTML = items.length
      ? '<div class="banner"><strong>' + esc(T.coverageWarn) + "</strong><ul>" +
        items.map(function (i) { return "<li>" + esc(i) + "</li>"; }).join("") + "</ul>" + info + "</div>"
      : '<div class="banner ok">' + esc(T.coverageOk) + info + "</div>";
  }

  // ------------------------------------------------------------------- tabs
  var TABS = ["overview", "waves", "models", "origins", "sources", "quality"];
  function renderTabs() {
    $("tabs").innerHTML = TABS.map(function (t) {
      return '<button data-tab="' + t + '" class="' + (state.tab === t ? "active" : "") + '">' +
        esc(T.tabs[t]) + "</button>";
    }).join("");
  }

  function interimModels() {
    return Object.keys(state.perModel).map(function (k) { return state.perModel[k]; })
      .filter(function (m) { return m.role === "interim"; });
  }

  function viewOverview() {
    var interim = interimModels();
    var needed = interim.filter(function (m) { return m.req > 0; }).length;
    var req = 0, drop = 0, src = 0;
    state.status.forEach(function (r) {
      if (r.role === "interim") { if (r.status === "required") req++; else drop++; }
      if (r.role === "source" && r.status === "required") src++;
    });
    var waves = 0;
    Object.keys(state.result.waves).forEach(function (d) { waves = Math.max(waves, state.result.waves[d]); });
    var tiles = [[state.result.dataProducts.length, T.tiles.dps], [waves, T.tiles.waves],
      [needed, T.tiles.needed], [interim.length - needed, T.tiles.unneeded],
      [req, T.tiles.req], [drop, T.tiles.drop], [src, T.tiles.src]];
    return '<div class="tiles">' + tiles.map(function (t) {
      return '<div class="tile"><div class="v">' + fmt(t[0]) + '</div><div class="l">' + esc(t[1]) + "</div></div>";
    }).join("") + '</div><section class="panel" style="margin-top:14px">' + viewWavesInner() +
      '</section><div class="legend">' + esc(T.legend) + "</div>";
  }

  function viewWavesInner() {
    var r = state.result, byWave = {};
    r.dataProducts.forEach(function (d) { (byWave[r.waves[d]] = byWave[r.waves[d]] || []).push(d); });
    var keys = Object.keys(byWave).map(Number).sort(function (a, b) { return a - b; });
    return '<div class="waves">' + keys.map(function (w) {
      return '<div class="wave"><h4>' + esc(T.wave) + " " + w + "</h4>" + byWave[w].sort().map(function (d) {
        var ups = r.anchors[d] || {};
        var lines = Object.keys(ups).sort().map(function (u) {
          var cols = Object.keys(ups[u]).map(function (k) { return ups[u][k]; });
          var nData = cols.filter(function (c) { return c.categories.data; }).length;
          return "<li>" + esc(T.reads) + " <span class=mono>" + up(u) + "</span>: " + fmt(cols.length) +
            " " + esc(T.columns) + " " + chip(T.cats.data + " " + nData, "data") +
            chip(T.cats.logic + " " + (cols.length - nData), "logic") + "</li>";
        });
        (r.dbtOnly[d] || []).forEach(function (u) {
          lines.push("<li>" + chip(T.dbtOnly, "warn") + " <span class=mono>" + up(u) + "</span></li>");
        });
        if (r.cycles.indexOf(d) >= 0) lines.push("<li>" + chip(T.cycle, "warn") + "</li>");
        return '<div class="card"><div class="name">' + up(d) + "</div><div class=hint>" +
          fmt(ix.colsOf[d].length) + " " + esc(T.columns) + "</div><ul>" +
          (lines.length ? lines.join("") : "<li class=hint>" + esc(T.noDeps) + "</li>") + "</ul></div>";
      }).join("") + "</div>";
    }).join("") + "</div>";
  }

  function viewWaves() { return '<section class="panel">' + viewWavesInner() + "</section>"; }

  function columnTable(rows, withNeeded) {
    return "<table><thead><tr><th>" + esc(T.column) + "</th><th>" + esc(T.status) + "</th><th>" +
      esc(T.category) + "</th><th>" + esc(T.usages) + "</th>" +
      (withNeeded ? "<th>" + esc(T.neededBy) + "</th>" : "") + "</tr></thead><tbody>" +
      rows.map(function (r) {
        var st = r.status === "required" ? chip(T.required, "data")
          : chip(r.status === "droppable" ? T.droppable : T.unused, "drop");
        return "<tr><td class=mono>" + up(r.column) + "</td><td>" + st + "</td><td>" +
          (r.category ? chip(T.cats[r.category], r.category) : "") + "</td><td>" +
          r.usages.map(function (u) { return chip(U[u] || u); }).join("") + "</td>" +
          (withNeeded ? "<td>" + r.neededBy.map(function (d) { return chip(d.toUpperCase(), "dp"); }).join("") + "</td>" : "") +
          "</tr>";
      }).join("") + "</tbody></table>";
  }

  function viewModels() {
    var f = state.modelFilter, layers = {};
    var all = Object.keys(state.perModel).map(function (k) { return state.perModel[k]; });
    all.forEach(function (m) { if (m.layer) layers[m.layer] = true; });
    var q = f.q.toLowerCase();
    var list = all.filter(function (m) {
      return (!q || m.model.indexOf(q) >= 0) && (!f.role || m.role === f.role) &&
        (!f.layer || m.layer === f.layer) && (!f.onlyDrop || m.drop > 0);
    }).sort(function (a, b) { return b.drop - a.drop || (a.model < b.model ? -1 : 1); });
    var shown = list.slice(0, f.limit);
    var html = '<section class="panel"><div class="filters">' +
      '<input type="text" id="mf-q" placeholder="' + esc(T.search) + '" value="' + esc(f.q) + '">' +
      '<select id="mf-role"><option value="">' + esc(T.allRoles) + "</option>" +
      ["data_product", "interim", "source"].map(function (r) {
        return '<option value="' + r + '"' + (f.role === r ? " selected" : "") + ">" + esc(T.roles[r]) + "</option>";
      }).join("") + '</select><select id="mf-layer"><option value="">' + esc(T.allLayers) + "</option>" +
      Object.keys(layers).sort().map(function (l) {
        return '<option value="' + esc(l) + '"' + (f.layer === l ? " selected" : "") + ">" + esc(l) + "</option>";
      }).join("") + '</select><label><input type="checkbox" id="mf-drop"' + (f.onlyDrop ? " checked" : "") +
      "> " + esc(T.onlyDrop) + "</label></div><table><thead><tr><th>" + esc(T.model) + "</th><th>" +
      esc(T.role) + "</th><th>" + esc(T.layer) + '</th><th class="num">' + esc(T.cols) +
      '</th><th class="num">' + esc(T.required) + '</th><th class="num">' + esc(T.droppable) +
      "</th><th></th><th>" + esc(T.neededBy) + "</th></tr></thead><tbody>";
    shown.forEach(function (m) {
      var pctReq = m.cols ? 100 * m.req / m.cols : 0, pctDrop = m.cols ? 100 * m.drop / m.cols : 0;
      html += '<tr class="clickable" data-open="' + esc(m.model) + '"><td class="mono">' + up(m.model) +
        "</td><td>" + chip(T.roles[m.role], m.role === "data_product" ? "dp" : "") + "</td><td>" +
        esc(m.layer) + '</td><td class="num">' + fmt(m.cols) + '</td><td class="num">' + fmt(m.req) +
        '</td><td class="num">' + fmt(m.drop) + '</td><td><div class="bar"><span class="req" style="width:' +
        pctReq + '%"></span><span class="drop" style="width:' + pctDrop + '%"></span></div></td><td>' +
        Object.keys(m.neededBy).sort().map(function (d) { return chip(d.toUpperCase(), "dp"); }).join("") +
        "</td></tr>";
      if (state.open[m.model]) {
        html += '<tr class="detail"><td colspan="8">' + columnTable(m.rows, true) + "</td></tr>";
      }
    });
    html += "</tbody></table><div class=pager>" + esc(T.showing.replace("{n}", fmt(shown.length)).replace("{m}", fmt(list.length))) +
      (list.length > shown.length ? ' <button id="mf-more">' + esc(T.more) + "</button>" : "") +
      '</div><div class="legend">' + esc(T.legend) + "</div></section>";
    return html;
  }

  function viewOrigins() {
    var dps = state.result.dataProducts;
    if (!dps.length) return "";
    if (dps.indexOf(state.originDp) < 0) state.originDp = dps[0];
    var dp = state.originDp, origins = state.result.origins[dp];
    var html = '<section class="panel"><div class="filters"><label>' + esc(T.dp) + ' <select id="of-dp">' +
      dps.map(function (d) {
        return '<option value="' + esc(d) + '"' + (d === dp ? " selected" : "") + ">" + up(d) + "</option>";
      }).join("") + "</select></label></div><table><thead><tr><th>" + esc(T.column) + "</th><th>" +
      esc(T.origin) + "</th></tr></thead><tbody>";
    Object.keys(origins).forEach(function (c) {
      var list = origins[c];
      html += "<tr><td class=mono>" + up(c) + "</td><td>" + (list.length ? list.map(function (o) {
        return chip(o[0].toUpperCase() + "." + o[1].toUpperCase(), o[2] === "data_product" ? "dp" : "");
      }).join("") : '<span class="hint">' + esc(T.constant) + "</span>") + "</td></tr>";
    });
    return html + "</tbody></table></section>";
  }

  function viewSources() {
    var list = Object.keys(state.perModel).map(function (k) { return state.perModel[k]; })
      .filter(function (m) { return m.role === "source" && m.req > 0; })
      .sort(function (a, b) { return a.model < b.model ? -1 : 1; });
    var html = '<section class="panel"><table><thead><tr><th>' + esc(T.sourceRel) + "</th><th>" +
      esc(T.layer) + '</th><th class="num">' + esc(T.required) + '</th><th class="num">' +
      esc(T.documented) + "</th><th>" + esc(T.neededBy) + "</th></tr></thead><tbody>";
    list.forEach(function (m) {
      var documented = (graph.models[m.model].columns || []).length;
      html += '<tr class="clickable" data-open="' + esc(m.model) + '"><td class="mono">' + up(m.model) +
        "</td><td>" + esc(graph.models[m.model].kind) + '</td><td class="num">' + fmt(m.req) +
        '</td><td class="num">' + fmt(documented) + "</td><td>" +
        Object.keys(m.neededBy).sort().map(function (d) { return chip(d.toUpperCase(), "dp"); }).join("") + "</td></tr>";
      if (state.open[m.model]) {
        html += '<tr class="detail"><td colspan="5">' + columnTable(m.rows.filter(function (r) {
          return r.status === "required";
        }), true) + "</td></tr>";
      }
    });
    return html + "</tbody></table></section>";
  }

  function pairTable(title, pairs) {
    return '<section class="panel"><h3>' + esc(title) + " (" + fmt(pairs.length) + ")</h3>" +
      (pairs.length ? '<table><tbody>' + pairs.slice(0, 2000).map(function (p) {
        return "<tr><td class=mono>" + up(p[0]) + "</td><td class=mono>" + up(p[1]) + "</td></tr>";
      }).join("") + "</tbody></table>" : "") + "</section>";
  }

  function viewQuality() {
    return pairTable(T.gapsUnresolved, graph.gaps.filter(function (g) { return g[2] === "unresolved"; })) +
      pairTable(T.danglingRefs, state.result.dangling) +
      pairTable(T.noColumns, (graph.coverage.relations_without_columns || []).map(function (r) { return [r, ""]; })) +
      pairTable(T.gapsNoInput, graph.gaps.filter(function (g) { return g[2] === "no_input"; })) +
      pairTable(T.docOnlyCols, graph.documentedOnly);
  }

  var VIEWS = { overview: viewOverview, waves: viewWaves, models: viewModels, origins: viewOrigins,
    sources: viewSources, quality: viewQuality };

  function render() {
    document.title = T.title;
    $("title").textContent = T.title;
    $("meta").textContent = T.generated + " " + blob.generated + " · " + T.selectors + ": " +
      (graph.selectors || []).join(" ");
    renderSidebar();
    renderBanner();
    renderTabs();
    $("view").innerHTML = VIEWS[state.tab]();
  }

  // ----------------------------------------------------------------- export
  function csvCell(v) {
    v = Array.isArray(v) ? v.join(" ") : String(v == null ? "" : v);
    return /[",;\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v;
  }
  function exportCsv() {
    var head = ["model", "column", "role", "layer", "status", "category", "usages", "needed_by"];
    var lines = [head.join(",")].concat(state.status.map(function (r) {
      return [r.model.toUpperCase(), r.column.toUpperCase(), r.role, r.layer, r.status, r.category,
        r.usages, r.neededBy.map(function (d) { return d.toUpperCase(); })].map(csvCell).join(",");
    }));
    var a = document.createElement("a");
    a.href = URL.createObjectURL(new Blob(["﻿" + lines.join("\n")], { type: "text/csv" }));
    a.download = "column_requirements.csv";
    a.click();
  }
  function exportSnippet() {
    var box = $("snippet");
    box.value = "[data-products]\nmodels =\n" + state.dps.map(function (d) {
      return "    " + d.toUpperCase();
    }).join("\n") + "\n";
    box.style.display = "block";
    box.select();
  }

  // ----------------------------------------------------------------- events
  document.addEventListener("click", function (ev) {
    var t = ev.target.closest("[data-remove],[data-tab],[data-open],#dp-reset,#dp-export,#csv-export,#mf-more");
    if (!t) return;
    if (t.dataset.remove) { state.dps = state.dps.filter(function (d) { return d !== t.dataset.remove; }); recompute(); }
    else if (t.dataset.tab) { state.tab = t.dataset.tab; render(); }
    else if (t.dataset.open) { state.open[t.dataset.open] = !state.open[t.dataset.open]; render(); }
    else if (t.id === "dp-reset") { state.dps = graph.dataProducts.slice(); recompute(); }
    else if (t.id === "dp-export") exportSnippet();
    else if (t.id === "csv-export") exportCsv();
    else if (t.id === "mf-more") { state.modelFilter.limit += 500; render(); }
  });
  document.addEventListener("change", function (ev) {
    var id = ev.target.id, f = state.modelFilter;
    if (id === "dp-add") { addDp(ev.target.value); ev.target.value = ""; return; }
    if (id === "mf-role") f.role = ev.target.value;
    else if (id === "mf-layer") f.layer = ev.target.value;
    else if (id === "mf-drop") f.onlyDrop = ev.target.checked;
    else if (id === "of-dp") state.originDp = ev.target.value;
    else return;
    f.limit = 200;
    render();
  });
  var typing;
  document.addEventListener("input", function (ev) {
    if (ev.target.id !== "mf-q") return;
    clearTimeout(typing);
    var value = ev.target.value;
    typing = setTimeout(function () {
      state.modelFilter.q = value;
      state.modelFilter.limit = 200;
      render();
      var box = $("mf-q");
      box.focus();
      box.setSelectionRange(value.length, value.length);
    }, 200);
  });
  $("dp-add").addEventListener("keydown", function (ev) {
    if (ev.key === "Enter") { addDp(ev.target.value); ev.target.value = ""; }
  });

  recompute();
})();
