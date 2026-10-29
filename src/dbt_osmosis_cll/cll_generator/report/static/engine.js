// Data-product requirement analysis — browser port of report/analysis.py.
// Keep in step with analyse() / column_status(); tests/core/test_cll_report.py
// runs both on the same graph and compares the results.
/* exported CllEngine */
var CllEngine = (function () {
  "use strict";

  // Expand the compact embedded graph (model indices) into name-keyed structures.
  function expand(blob) {
    var names = blob.models.map(function (m) { return m[0]; });
    var models = {};
    blob.models.forEach(function (m, i) {
      models[m[0]] = {
        kind: m[1], layer: m[2], materialized: m[3], analysed: !!m[4], path: m[5],
        columns: (blob.mcols[i] || []).slice()
      };
    });
    var rows = blob.rows.map(function (r) {
      return [names[r[0]], r[1], blob.types[r[2]],
        r[3].map(function (p) { return [names[p[0]], p[1]]; })];
    });
    var usages = blob.usages.map(function (u) {
      return [names[u[0]], blob.usageKinds[u[1]], names[u[2]], u[3], u[4], u[5]];
    });
    var deps = {};
    blob.deps.forEach(function (d) {
      deps[names[d[0]]] = d[1].map(function (i) { return names[i]; });
    });
    return {
      models: models, rows: rows, usages: usages, deps: deps,
      gaps: blob.gaps.map(function (g) { return [names[g[0]], g[1], g[2]]; }),
      documentedOnly: blob.documentedOnly.map(function (g) { return [names[g[0]], g[1]]; }),
      coverage: blob.coverage, selectors: blob.selectors, dataProducts: blob.dataProducts
    };
  }

  function key(m, c) { return m + "\u0000" + c; }

  // Precompute lookups that do not depend on the selection.
  function index(graph) {
    var rows = {}, colsOf = {}, usagesOf = {};
    graph.rows.forEach(function (r) {
      rows[key(r[0], r[1])] = r;
      (colsOf[r[0]] = colsOf[r[0]] || []).push(r[1]);
    });
    graph.rows.forEach(function (r) {
      if (!graph.models[r[0]]) return;
      graph.models[r[0]].columns = colsOf[r[0]];
    });
    graph.usages.forEach(function (u) { (usagesOf[u[0]] = usagesOf[u[0]] || []).push(u); });
    return { graph: graph, rows: rows, colsOf: colsOf, usagesOf: usagesOf };
  }

  function analyse(ix, dataProducts) {
    var rows = ix.rows, colsOf = ix.colsOf, usagesOf = ix.usagesOf, deps = ix.graph.deps;
    var selected = [];
    dataProducts.forEach(function (d) {
      d = d.toLowerCase();
      if (selected.indexOf(d) < 0) selected.push(d);
    });
    var dps = selected.filter(function (d) { return colsOf[d]; });
    var missing = selected.filter(function (d) { return !colsOf[d]; });
    var anchorSet = {};
    selected.forEach(function (d) { anchorSet[d] = true; });

    var required = {};  // key -> {model, column, categories{}, usages{}, neededBy{}}
    var anchors = {};   // dp -> upstream dp -> key -> {categories}
    var dangling = {};
    dps.forEach(function (dp) {
      var seen = {}, neededModels = {}, dpAnchors = {}, stack = [];
      function need(model, col, category, usage) {
        var k = key(model, col);
        if (anchorSet[model] && model !== dp) {
          var up = dpAnchors[model] = dpAnchors[model] || {};
          (up[k] = up[k] || { column: col, categories: {} }).categories[category] = true;
          return;
        }
        var e = required[k];
        if (!e) e = required[k] = { model: model, column: col, categories: {}, usages: {}, neededBy: {} };
        e.neededBy[dp] = true;
        if (usage) e.usages[usage] = true;
        var s = seen[k] = seen[k] || {};
        if (s[category]) return;
        s[category] = true;
        e.categories[category] = true;
        stack.push([model, col, category]);
      }
      colsOf[dp].forEach(function (c) { need(dp, c, "data"); });
      while (stack.length) {
        var item = stack.pop(), model = item[0], col = item[1], category = item[2];
        if (!colsOf[model]) continue;
        if (!neededModels[model]) {
          neededModels[model] = true;
          (usagesOf[model] || []).forEach(function (u) { need(u[2], u[3], "logic", u[1]); });
        }
        var row = rows[key(model, col)];
        if (!row) { dangling[key(model, col)] = [model, col]; continue; }
        row[3].forEach(function (p) {
          if (p[0] !== model || p[1] !== col) need(p[0], p[1], category);
        });
      }
      anchors[dp] = dpAnchors;
    });

    var waves = {}, state = {}, cycles = [];
    function wave(dp) {
      if (waves[dp] !== undefined) return waves[dp];
      if (state[dp] === 1) { if (cycles.indexOf(dp) < 0) cycles.push(dp); return 0; }
      state[dp] = 1;
      var best = 0;
      Object.keys(anchors[dp] || {}).forEach(function (u) {
        if (anchors[u]) best = Math.max(best, wave(u));
      });
      waves[dp] = best + 1;
      state[dp] = 2;
      return waves[dp];
    }
    dps.forEach(wave);

    var dbtOnly = {};
    dps.forEach(function (dp) {
      var reach = {}, todo = (deps[dp] || []).slice();
      while (todo.length) {
        var m = todo.pop();
        if (reach[m]) continue;
        reach[m] = true;
        if (!anchorSet[m]) todo.push.apply(todo, deps[m] || []);
      }
      var extra = Object.keys(reach).filter(function (m) {
        return anchorSet[m] && !anchors[dp][m] && m !== dp;
      }).sort();
      if (extra.length) dbtOnly[dp] = extra;
    });

    var memo = {};
    function resolve(model, col, trail) {
      var k = key(model, col);
      if (anchorSet[model]) return [[model, col, "data_product"]];
      if (!rows[k]) return [[model, col, "source"]];
      if (memo[k]) return memo[k];
      if (trail[k]) return [];
      trail[k] = true;
      var out = {}, list = [];
      rows[k][3].forEach(function (p) {
        resolve(p[0], p[1], trail).forEach(function (o) {
          var ok = o.join("\u0000");
          if (!out[ok]) { out[ok] = true; list.push(o); }
        });
      });
      delete trail[k];
      memo[k] = list;
      return list;
    }
    var origins = {};
    dps.forEach(function (dp) {
      var perCol = {};
      colsOf[dp].forEach(function (c) {
        var found = {}, list = [], trail = {};
        trail[key(dp, c)] = true;
        rows[key(dp, c)][3].forEach(function (p) {
          if (p[0] === dp) return;
          resolve(p[0], p[1], trail).forEach(function (o) {
            var ok = o.join("\u0000");
            if (!found[ok]) { found[ok] = true; list.push(o); }
          });
        });
        list.sort(function (a, b) { return a.join("\u0000") < b.join("\u0000") ? -1 : 1; });
        perCol[c] = list;
      });
      origins[dp] = perCol;
    });

    return {
      dataProducts: dps, missing: missing, required: required, anchors: anchors,
      waves: waves, cycles: cycles.sort(), dbtOnly: dbtOnly, origins: origins,
      dangling: Object.keys(dangling).sort().map(function (k) { return dangling[k]; })
    };
  }

  function columnStatus(ix, result) {
    var dps = {};
    result.dataProducts.forEach(function (d) { dps[d] = true; });
    var extraOf = {};
    Object.keys(result.required).forEach(function (k) {
      var e = result.required[k];
      (extraOf[e.model] = extraOf[e.model] || []).push(e.column);
    });
    var out = [];
    Object.keys(ix.graph.models).sort().forEach(function (model) {
      var meta = ix.graph.models[model];
      var role = dps[model] ? "data_product" : (meta.analysed ? "interim" : "source");
      var cols = (meta.columns || []).slice();
      var have = {};
      cols.forEach(function (c) { have[c] = true; });
      (extraOf[model] || []).filter(function (c) { return !have[c]; }).sort()
        .forEach(function (c) { cols.push(c); });
      cols.forEach(function (col) {
        var req = result.required[key(model, col)];
        var status = (role === "data_product" || req) ? "required"
          : (role === "interim" ? "droppable" : "unused");
        out.push({
          model: model, column: col, role: role, layer: meta.layer || "", status: status,
          category: (role === "data_product" || (req && req.categories.data)) ? "data"
            : (req ? "logic" : ""),
          usages: req ? Object.keys(req.usages).sort() : [],
          neededBy: req ? Object.keys(req.neededBy).sort() : []
        });
      });
    });
    return out;
  }

  return { expand: expand, index: index, analyse: analyse, columnStatus: columnStatus };
})();

if (typeof module !== "undefined") module.exports = CllEngine;
