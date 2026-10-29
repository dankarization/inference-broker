/**
 * TableSort — кликовая сортировка таблиц.
 *
 * Один и тот же цикл, что на AI Usage Dashboard:
 *   первый клик по столбцу → по убыванию,
 *   второй → по возрастанию,
 *   третий → сброс к порядку по умолчанию.
 *
 * Подключение к готовой HTML-таблице:
 *   TableSort.bind(table, { defaultKey: "wait" });
 * В каждом <th> укажите data-sort-key. Если внутри ещё нет кнопки,
 * модуль сам обернёт заголовок.
 *
 * Если строки рисуются из JS-данных (как таблица моделей), bind не нужен:
 *   state = TableSort.nextState(state, key, "total");
 *   rows = TableSort.sortRows(rows, direction, row => value);
 *
 * Пустые значения («—», пустая ячейка) всегда в конце.
 * Числа сравниваются как числа, текст — без учёта регистра.
 * Равные строки сохраняют исходный порядок.
 */
(function (root) {
  "use strict";

  function isBlank(text) {
    return !text || text === "—" || text === "-" || text === "None" || text === "None queued";
  }

  function parseValue(raw) {
    if (raw == null) return null;
    var text = String(raw).replace(/\s+/g, " ").trim();
    if (isBlank(text) || /^[—–−-]+$/.test(text)) return null;
    var money = text.replace(/[$,]/g, "");
    if (/[$,]/.test(text) && /^-?\d+(\.\d+)?$/.test(money)) return Number(money);
    var compact = text.match(/^(-?\d+(?:\.\d+)?)([KMB])\b/i);
    if (compact) {
      var mul = { K: 1e3, M: 1e6, B: 1e9 }[compact[2].toUpperCase()];
      return Number(compact[1]) * mul;
    }
    var seconds = text.match(/^(-?\d+(?:\.\d+)?)s$/i);
    if (seconds) return Number(seconds[1]);
    var numeric = text.replace(/,/g, "");
    if (/^-?\d+(\.\d+)?$/.test(numeric)) return Number(numeric);
    var parsed = Date.parse(text);
    if (!isNaN(parsed) && /[-/:]/.test(text)) return parsed;
    return text;
  }

  function cellValue(td) {
    if (!td) return null;
    if (td.getAttribute("data-sort-value") != null && td.getAttribute("data-sort-value") !== "") {
      return parseValue(td.getAttribute("data-sort-value"));
    }
    var select = td.querySelector("select");
    if (select) return parseValue(select.options[select.selectedIndex] ? select.options[select.selectedIndex].text : select.value);
    return parseValue(td.textContent);
  }

  function compareValues(a, b) {
    if (a == null && b == null) return 0;
    if (a == null) return 1;
    if (b == null) return -1;
    if (typeof a === "number" && typeof b === "number") return a - b;
    return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: "base" });
  }

  function nextState(state, key, defaultKey) {
    state = state || { key: defaultKey, direction: "default" };
    if (key !== state.key || state.direction === "default") {
      return { key: key, direction: "descending" };
    }
    if (state.direction === "descending") {
      return { key: key, direction: "ascending" };
    }
    return { key: defaultKey, direction: "default" };
  }

  function headerView(state, key, options) {
    options = options || {};
    var defaultKey = options.defaultKey;
    var defaultDirection = options.defaultDirection || "descending";
    var resetTitle = options.resetTitle || "Reset to default";
    var activeKey = state.direction === "default" ? defaultKey : state.key;
    var shown = state.direction === "default" ? defaultDirection : state.direction;
    var direction = key === activeKey && activeKey ? shown : "none";
    var mark = direction === "none" ? "↕" : direction === "ascending" ? "↑" : "↓";
    var title = "Sort descending";
    if (key === state.key && state.direction === "descending") title = "Sort ascending";
    else if (key === state.key && state.direction === "ascending") title = resetTitle;
    return {
      direction: direction,
      mark: mark,
      title: title,
      ariaSort: direction === "none" ? "none" : direction
    };
  }

  function sortRows(items, direction, getValue) {
    var dir = direction === "ascending" ? 1 : -1;
    return items.map(function (item, index) {
      return { item: item, index: index, value: getValue(item, index) };
    }).sort(function (a, b) {
      if (a.value == null && b.value == null) return a.index - b.index;
      if (a.value == null) return 1;
      if (b.value == null) return -1;
      var cmp = compareValues(a.value, b.value);
      return cmp ? cmp * dir : a.index - b.index;
    }).map(function (entry) { return entry.item; });
  }

  function ensureButton(th) {
    var btn = th.querySelector(".sort-btn") || th.querySelector("button");
    if (btn) {
      if (!th.querySelector(".sort-mark")) {
        var extra = document.createElement("span");
        extra.className = "sort-mark";
        extra.setAttribute("aria-hidden", "true");
        extra.textContent = "↕";
        btn.appendChild(extra);
      }
      return btn;
    }
    btn = document.createElement("button");
    btn.type = "button";
    btn.className = "sort-btn";
    var mark = document.createElement("span");
    mark.className = "sort-mark";
    mark.setAttribute("aria-hidden", "true");
    mark.textContent = "↕";
    while (th.firstChild) btn.appendChild(th.firstChild);
    btn.appendChild(mark);
    th.appendChild(btn);
    return btn;
  }

  function bind(table, options) {
    options = options || {};
    var defaultKey = options.defaultKey || null;
    var defaultDirection = options.defaultDirection || "descending";
    var state = { key: defaultKey, direction: "default" };
    var tbody = table.tBodies[0];
    var heads = table.querySelectorAll("thead th[data-sort-key]");
    var original = tbody ? Array.prototype.slice.call(tbody.rows) : [];

    function visibleRows() {
      return Array.prototype.filter.call(tbody.rows, function (tr) {
        return !tr.hasAttribute("data-sort-ignore");
      });
    }

    function applyHeaders() {
      Array.prototype.forEach.call(heads, function (th) {
        var key = th.getAttribute("data-sort-key");
        var view = headerView(state, key, {
          defaultKey: defaultKey,
          defaultDirection: defaultDirection,
          resetTitle: options.resetTitle
        });
        th.setAttribute("aria-sort", view.ariaSort);
        var mark = th.querySelector(".sort-mark");
        var btn = th.querySelector(".sort-btn") || th.querySelector("button");
        if (mark) mark.textContent = view.mark;
        if (btn) btn.title = view.title;
      });
    }

    function applySort() {
      if (!tbody) return;
      if (state.direction === "default") {
        original.forEach(function (tr) { tbody.appendChild(tr); });
        return;
      }
      var key = state.key;
      var col = -1;
      Array.prototype.forEach.call(heads, function (th, index) {
        if (th.getAttribute("data-sort-key") === key) col = index;
      });
      if (col < 0) return;
      sortRows(visibleRows(), state.direction, function (tr) {
        return cellValue(tr.cells[col]);
      }).forEach(function (tr) { tbody.appendChild(tr); });
    }

    Array.prototype.forEach.call(heads, function (th) {
      var key = th.getAttribute("data-sort-key");
      var btn = ensureButton(th);
      btn.addEventListener("click", function () {
        state = nextState(state, key, defaultKey);
        applyHeaders();
        applySort();
      });
    });
    applyHeaders();

    table._tableSort = {
      addRow: function (tr) {
        if (original.indexOf(tr) < 0) original.push(tr);
        applySort();
      },
      resort: function () { applySort(); }
    };
    return table._tableSort;
  }

  root.TableSort = {
    parseValue: parseValue,
    compareValues: compareValues,
    nextState: nextState,
    headerView: headerView,
    sortRows: sortRows,
    bind: bind
  };
})(typeof window !== "undefined" ? window : this);
