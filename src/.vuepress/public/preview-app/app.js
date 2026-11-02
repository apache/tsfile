/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

(function () {
  "use strict";

  // The whole file is copied into WebAssembly memory, so cap what this page
  // accepts and send larger files to the standalone viewer instead.
  var MAX_BYTES = 100 * 1024 * 1024;
  var MIN_TIME = "-9223372036854775808";
  var MAX_TIME = "9223372036854775807";

  var COLORS = [
    "#6034b1", "#c0392b", "#2e8b57", "#b8860b", "#1f6f8b",
    "#8e44ad", "#d35400", "#2c3e50",
  ];

  var STRINGS = {
    en: {
      notice:
        "<strong>File size:</strong> this page holds the whole file in memory " +
        "and works best for files <strong>under 100 MB</strong>. Larger files " +
        'need the standalone <a href="/UserGuide/latest/Tools/Tsfile-Viewer" ' +
        'target="_top">TsFile Viewer</a>.',
      dropTitle: "Drop a .tsfile here, or click to choose",
      dropHint: "Nothing is uploaded — decoding runs in your browser.",
      sample: "Load sample data",
      tableLabel: "Table",
      deviceLabel: "Device",
      measurementsLabel: "Measurements",
      timeRangeLabel: "Time range",
      fullRange: "Full range",
      loadFirst: "Load a file first",
      noDevices: "No devices",
      noMeasures: "No measurements",
      unitLabel: "Detected unit: {unit}",
      emptyDefault: "No data to plot yet",
      emptyNoSelection: "Select at least one measurement",
      emptyNoData: "No rows in the selected time range",
      emptyAllNull: "This series has no values in the selected range",
      meta: "{points} points · {series} series · {from} → {to}",
      errTooLarge:
        "This file is {size}. The in-page preview handles files up to 100 MB; " +
        'larger files need the standalone ' +
        '<a href="/UserGuide/latest/Tools/Tsfile-Viewer" target="_top">' +
        "TsFile Viewer</a>.",
      errOpen: "Open failed: {reason}",
      errRead: "Could not read the file.",
      errFetch: "Could not load {url}: {reason}",
      errTableModel: "Table-model file: this page reads tree-model series only.",
      errNoSeries: "Series not found.",
      errNoFile: "No file is open.",
      errInteger: "Time values must be whole numbers.",
      errOrder: "Start time must not be after the end time.",
    },
    zh: {
      notice:
        "<strong>文件大小：</strong>本页会把整个文件载入内存，建议用于 " +
        "<strong>小于 100 MB</strong> 的文件；更大的文件请使用独立部署的 " +
        '<a href="/zh/UserGuide/latest/Tools/Tsfile-Viewer" ' +
        'target="_top">TsFile Viewer</a>。',
      dropTitle: "拖入 .tsfile 文件，或点击选择",
      dropHint: "文件在浏览器内解码，不会上传。",
      sample: "载入示例数据",
      tableLabel: "表名",
      deviceLabel: "设备名",
      measurementsLabel: "测点",
      timeRangeLabel: "时间范围",
      fullRange: "完整范围",
      loadFirst: "请先加载文件",
      noDevices: "没有可用设备",
      noMeasures: "没有可用测点",
      unitLabel: "识别到的时间单位：{unit}",
      emptyDefault: "暂无可绘制的数据",
      emptyNoSelection: "请至少选择一个测点",
      emptyNoData: "所选时间范围内没有数据",
      emptyAllNull: "所选范围内该序列没有值",
      meta: "{points} 个点 · {series} 条序列 · {from} → {to}",
      errTooLarge:
        "文件大小 {size}。页面内预览仅支持 100 MB 以内的文件，更大的文件请使用独立部署的 " +
        '<a href="/zh/UserGuide/latest/Tools/Tsfile-Viewer" target="_top">' +
        "TsFile Viewer</a>。",
      errOpen: "打开失败：{reason}",
      errRead: "读取文件失败。",
      errFetch: "无法加载 {url}：{reason}",
      errTableModel: "表模型文件：本页面暂不支持。",
      errNoSeries: "未找到该序列。",
      errNoFile: "尚未打开文件。",
      errInteger: "时间必须是整数。",
      errOrder: "开始时间不能晚于结束时间。",
    },
  };

  function detectLang() {
    var fromQuery = new URLSearchParams(window.location.search).get("lang");
    if (fromQuery && STRINGS[fromQuery]) {
      return fromQuery;
    }
    try {
      if (window.parent !== window && window.parent.document) {
        var lang = (window.parent.document.documentElement.lang || "").toLowerCase();
        if (lang) {
          return lang.indexOf("zh") === 0 ? "zh" : "en";
        }
      }
    } catch (error) {
      // Cross-origin parent: fall through to the browser preference.
    }
    return (navigator.language || "en").toLowerCase().indexOf("zh") === 0
      ? "zh"
      : "en";
  }

  var LANG = detectLang();
  var TEXT = STRINGS[LANG];

  function t(key, values) {
    var template = TEXT[key] || STRINGS.en[key] || key;
    return template.replace(/\{(\w+)\}/g, function (match, name) {
      return values && name in values ? String(values[name]) : match;
    });
  }

  // The reader reports failures in English; map the ones users can hit.
  function translateError(message) {
    if (!message || LANG === "en") {
      return message;
    }
    var starts = function (prefix) {
      return message.indexOf(prefix) === 0;
    };
    if (starts("table-model file")) {
      return t("errTableModel");
    }
    if (starts("series not found")) {
      return t("errNoSeries");
    }
    if (starts("no file is open")) {
      return t("errNoFile");
    }
    if (starts("open failed")) {
      return t("errOpen", { reason: message.replace(/^open failed:?\s*/, "") });
    }
    if (starts("query failed")) {
      return t("errOpen", { reason: message.replace(/^query failed:?\s*/, "") });
    }
    return message;
  }

  var module = null;
  var model = null;
  var selected = { table: "", device: "" };
  var measurements = [];
  var checked = [];
  var fullRange = { start: "", end: "" };
  var unit = "ms";
  var lastData = null;

  var dropEl = document.getElementById("drop");
  var fileEl = document.getElementById("file");
  var sampleEl = document.getElementById("sample");
  var tableField = document.getElementById("tableField");
  var tableEl = document.getElementById("table");
  var deviceEl = document.getElementById("device");
  var measuresEl = document.getElementById("measures");
  var startEl = document.getElementById("start");
  var endEl = document.getElementById("end");
  var resetEl = document.getElementById("resetRange");
  var unitEl = document.getElementById("unit");
  var metaEl = document.getElementById("meta");
  var errorEl = document.getElementById("error");
  var canvas = document.getElementById("chart");
  var emptyEl = document.getElementById("empty");
  var legendEl = document.getElementById("legend");
  var chartWrap = document.getElementById("chartWrap");
  var selectorCard = document.getElementById("selectorCard");

  function showError(message) {
    errorEl.textContent = message ? translateError(message) : "";
  }

  // Only used for messages this module builds itself (they carry links).
  function showErrorHtml(html) {
    errorEl.innerHTML = html;
  }

  function applyStrings() {
    document.documentElement.lang = LANG === "zh" ? "zh-CN" : "en";
    document.title = LANG === "zh" ? "TsFile 在线预览" : "TsFile Preview";
    var notice = document.querySelector(".notice");
    if (notice) {
      notice.innerHTML = t("notice");
    }
    var dropTitle = dropEl.querySelector("strong");
    if (dropTitle) {
      dropTitle.textContent = t("dropTitle");
    }
    var hint = document.getElementById("dropHint");
    if (hint) {
      hint.textContent = t("dropHint");
    }
    if (sampleEl) {
      sampleEl.textContent = t("sample");
    }
    document.querySelector('label[for="table"]').textContent = t("tableLabel");
    document.querySelector('label[for="device"]').textContent = t("deviceLabel");
    measuresEl.parentNode.querySelector("label").textContent =
      t("measurementsLabel");
    startEl.parentNode.parentNode.querySelector("label").textContent =
      t("timeRangeLabel");
    resetEl.textContent = t("fullRange");
    emptyEl.textContent = t("emptyDefault");
    if (!module) {
      measuresEl.innerHTML = "";
      var idle = document.createElement("span");
      idle.className = "empty-hint";
      idle.textContent = t("loadFirst");
      measuresEl.appendChild(idle);
    }
  }

  applyStrings();

  function copyDoubles(vec) {
    var out = new Float64Array(vec.size());
    for (var i = 0; i < vec.size(); i++) {
      out[i] = vec.get(i);
    }
    return out;
  }

  function copyBytes(vec) {
    var out = new Uint8Array(vec.size());
    for (var i = 0; i < vec.size(); i++) {
      out[i] = vec.get(i);
    }
    return out;
  }

  function copyStrings(vec) {
    var out = [];
    for (var i = 0; i < vec.size(); i++) {
      out.push(vec.get(i));
    }
    return out;
  }

  function takeStrings(vec) {
    var out = copyStrings(vec);
    vec.delete();
    return out;
  }

  // --- time unit detection -------------------------------------------------

  function detectUnit(maxAbs) {
    var n = Math.abs(maxAbs);
    if (n < 1e11) return "s";
    if (n < 1e14) return "ms";
    if (n < 1e17) return "us";
    return "ns";
  }

  var UNIT_MS = { s: 1000, ms: 1, us: 0.001, ns: 0.000001 };

  function toMillis(value, name) {
    var asNumber = Number(value);
    if (!isFinite(asNumber)) {
      return NaN;
    }
    return asNumber * UNIT_MS[name || unit];
  }

  function formatTime(value) {
    var ms = toMillis(value);
    if (!isFinite(ms)) {
      return String(value);
    }
    var date = new Date(Math.round(ms));
    if (isNaN(date.getTime())) {
      return String(value);
    }
    var iso = date.toISOString();
    var base = iso.slice(0, 19).replace("T", " ");
    var millis = iso.slice(20, 23);
    return millis === "000" ? base : base + "." + millis;
  }

  // --- selection -----------------------------------------------------------

  function fillSelect(select, values, emptyLabel) {
    select.innerHTML = "";
    if (!values.length) {
      var option = document.createElement("option");
      option.textContent = emptyLabel;
      select.appendChild(option);
      select.disabled = true;
      return;
    }
    values.forEach(function (value) {
      var option = document.createElement("option");
      option.value = value;
      option.textContent = value;
      select.appendChild(option);
    });
    select.disabled = false;
  }

  function loadTables() {
    var tables = takeStrings(module.list_tables());
    model = tables.length ? "table" : "tree";
    // The range/device selectors only make sense once a file is decoded.
    selectorCard.classList.remove("hidden");
    tableField.classList.toggle("hidden", model !== "table");
    if (model === "table") {
      fillSelect(tableEl, tables, t("noDevices"));
      selected.table = tables[0] || "";
    } else {
      selected.table = "";
    }
    loadDevices();
  }

  function loadDevices() {
    var devices = takeStrings(module.list_devices(selected.table));
    fillSelect(deviceEl, devices, t("noDevices"));
    selected.device = devices[0] || "";
    if (!selected.device) {
      measurements = [];
      renderMeasurements();
      clearChart(t("noDevices"));
      return;
    }
    loadMeasurements();
  }

  function loadMeasurements() {
    measurements = takeStrings(
      module.list_measurements(selected.table, selected.device)
    );
    checked = measurements.length ? [measurements[0]] : [];
    renderMeasurements();
    readRange();
  }

  function renderMeasurements() {
    measuresEl.innerHTML = "";
    if (!measurements.length) {
      var idle = document.createElement("span");
      idle.className = "empty-hint";
      idle.textContent = t("noMeasures");
      measuresEl.appendChild(idle);
      return;
    }
    measurements.forEach(function (name) {
      var label = document.createElement("label");
      var box = document.createElement("input");
      box.type = "checkbox";
      box.value = name;
      box.checked = checked.indexOf(name) !== -1;
      box.addEventListener("change", function () {
        checked = Array.prototype.slice
          .call(measuresEl.querySelectorAll("input:checked"))
          .map(function (input) {
            return input.value;
          });
        readSeries();
      });
      label.appendChild(box);
      label.appendChild(document.createTextNode(name));
      measuresEl.appendChild(label);
    });
  }

  function readRange() {
    var range = module.time_range(selected.table, selected.device);
    fullRange = { start: range.start, end: range.end };
    startEl.value = fullRange.start;
    endEl.value = fullRange.end;
    startEl.disabled = false;
    endEl.disabled = false;
    resetEl.disabled = false;
    unit = detectUnit(Number(fullRange.end));
    unitEl.textContent = t("unitLabel", { unit: unit });
    readSeries();
  }

  function parseRange() {
    var start = startEl.value.trim();
    var end = endEl.value.trim();
    // Some devices carry no statistics; read them with the widest range
    // instead of refusing the query.
    if (start === "" && end === "") {
      return { start: MIN_TIME, end: MAX_TIME };
    }
    if (!/^-?\d+$/.test(start) || !/^-?\d+$/.test(end)) {
      showError(t("errInteger"));
      return null;
    }
    if (BigInt(start) > BigInt(end)) {
      showError(t("errOrder"));
      return null;
    }
    return { start: start, end: end };
  }

  function readSeries() {
    if (!module || !selected.device) {
      return;
    }
    if (!checked.length) {
      clearChart(t("emptyNoSelection"));
      return;
    }
    var range = parseRange();
    if (!range) {
      return;
    }
    showError("");

    var vector = new module.VectorString();
    checked.forEach(function (name) {
      vector.push_back(name);
    });
    var data = module.read_series(
      selected.table,
      selected.device,
      vector,
      range.start,
      range.end
    );
    vector.delete();
    var times = copyDoubles(data.times);
    var values = copyDoubles(data.values);
    var nulls = copyBytes(data.nulls);
    var names = copyStrings(data.names);
    var columnCount = data.column_count;
    var timeBase = data.time_base;
    data.times.delete();
    data.values.delete();
    data.nulls.delete();
    data.names.delete();

    var error = module.last_error();
    if (error) {
      showError(error);
    }
    if (!times.length) {
      clearChart(t("emptyNoData"));
      return;
    }

    var columns = [];
    var drawn = 0;
    for (var c = 0; c < columnCount; c++) {
      var column = {
        name: names[c],
        color: COLORS[c % COLORS.length],
        values: values.subarray(c * times.length, (c + 1) * times.length),
        nulls: nulls.subarray(c * times.length, (c + 1) * times.length),
      };
      for (var i = 0; i < column.nulls.length; i++) {
        if (!column.nulls[i] && isFinite(column.values[i])) {
          drawn++;
        }
      }
      columns.push(column);
    }
    if (drawn === 0) {
      clearChart(t("emptyAllNull"));
      return;
    }
    lastData = { times: times, columns: columns, timeBase: timeBase };
    drawChart();
  }

  function clearChart(message) {
    lastData = null;
    metaEl.textContent = "";
    legendEl.innerHTML = "";
    emptyEl.textContent = message || t("emptyDefault");
    emptyEl.classList.remove("hidden");
    drawChart();
  }

  // --- drawing -------------------------------------------------------------

  function css(name) {
    return getComputedStyle(document.documentElement)
      .getPropertyValue(name)
      .trim();
  }

  function niceStep(span, target) {
    var raw = span / Math.max(1, target);
    var power = Math.pow(10, Math.floor(Math.log10(raw)));
    var fraction = raw / power;
    var step = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
    return step * power;
  }

  function formatValue(v) {
    var abs = Math.abs(v);
    if (abs >= 1e6 || (abs > 0 && abs < 1e-3)) {
      return v.toExponential(1);
    }
    return String(Math.round(v * 1000) / 1000);
  }

  function addOffset(base, offset) {
    try {
      return (BigInt(base) + BigInt(Math.round(offset))).toString();
    } catch (error) {
      return String(Number(base) + offset);
    }
  }

  function formatTick(offset, step, min) {
    return formatTime(addOffset(lastData ? lastData.timeBase : "0", offset));
  }

  function drawChart() {
    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    canvas.width = Math.max(1, Math.round(rect.width * dpr));
    canvas.height = Math.max(1, Math.round(rect.height * dpr));

    var ctx = canvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, rect.width, rect.height);

    var data = lastData;
    if (!data) {
      return;
    }

    var padL = 64;
    var padR = 16;
    var padT = 16;
    var padB = 40;
    var width = rect.width - padL - padR;
    var height = rect.height - padT - padB;
    if (width <= 0 || height <= 0) {
      return;
    }

    var xMin = Infinity;
    var xMax = -Infinity;
    var yMin = Infinity;
    var yMax = -Infinity;
    data.times.forEach && data.times.forEach(function (v) {
      if (v < xMin) xMin = v;
      if (v > xMax) xMax = v;
    });
    for (var c = 0; c < data.columns.length; c++) {
      var col = data.columns[c];
      for (var i = 0; i < col.values.length; i++) {
        if (col.nulls[i]) {
          continue;
        }
        var v = col.values[i];
        if (v < yMin) yMin = v;
        if (v > yMax) yMax = v;
      }
    }
    if (!isFinite(xMin) || !isFinite(yMin)) {
      return;
    }
    if (xMax === xMin) {
      xMin -= 1;
      xMax += 1;
    }
    if (yMax === yMin) {
      var pad = Math.abs(yMax) > 1 ? Math.abs(yMax) * 0.1 : 1;
      yMin -= pad;
      yMax += pad;
    }
    var yPad = (yMax - yMin) * 0.06;
    yMin -= yPad;
    yMax += yPad;

    var sx = function (t) {
      return padL + ((t - xMin) / (xMax - xMin)) * width;
    };
    var sy = function (v) {
      return padT + height - ((v - yMin) / (yMax - yMin)) * height;
    };

    var gridColor = css("--grid");
    var mutedColor = css("--muted");
    ctx.font = "11px " + css("--font");
    ctx.lineWidth = 1;

    var yStep = niceStep(yMax - yMin, 5);
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    for (var y = Math.ceil(yMin / yStep) * yStep; y <= yMax; y += yStep) {
      var py = sy(y);
      ctx.strokeStyle = gridColor;
      ctx.beginPath();
      ctx.moveTo(padL, py);
      ctx.lineTo(padL + width, py);
      ctx.stroke();
      ctx.fillStyle = mutedColor;
      ctx.fillText(formatValue(y), padL - 8, py);
    }

    var xStep = niceStep(xMax - xMin, 6);
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    for (var x = Math.ceil(xMin / xStep) * xStep; x <= xMax; x += xStep) {
      var px = sx(x);
      ctx.strokeStyle = gridColor;
      ctx.beginPath();
      ctx.moveTo(px, padT);
      ctx.lineTo(px, padT + height);
      ctx.stroke();
      ctx.fillStyle = mutedColor;
      ctx.fillText(formatTick(x, xStep, xMin), px, padT + height + 10);
    }

    ctx.lineWidth = 1.6;
    ctx.lineJoin = "round";
    data.columns.forEach(function (col) {
      ctx.strokeStyle = col.color;
      ctx.beginPath();
      var pen = false;
      for (var k = 0; k < data.times.length; k++) {
        if (col.nulls[k] || !isFinite(col.values[k])) {
          pen = false;
          continue;
        }
        var cx = sx(data.times[k]);
        var cy = sy(col.values[k]);
        if (pen) {
          ctx.lineTo(cx, cy);
        } else {
          ctx.moveTo(cx, cy);
          pen = true;
        }
      }
      ctx.stroke();
    });

    // A series with one or two points draws no visible line.
    if (data.times.length <= 2) {
      data.columns.forEach(function (col) {
        ctx.fillStyle = col.color;
        for (var k = 0; k < data.times.length; k++) {
          if (col.nulls[k] || !isFinite(col.values[k])) continue;
          ctx.beginPath();
          ctx.arc(sx(data.times[k]), sy(col.values[k]), 3, 0, Math.PI * 2);
          ctx.fill();
        }
      });
    }

    // Tooltip-free but readable: paint dots when a series is very short.
    var legend = "";
    data.columns.forEach(function (col) {
      legend +=
        '<span><i style="background:' + col.color + '"></i>' +
        col.name + "</span>";
    });
    legendEl.innerHTML = legend;
    emptyEl.classList.add("hidden");

    metaEl.textContent = t("meta", {
      points: data.times.length,
      series: data.columns.length,
      from: formatTime(addOffset(data.timeBase, data.times[0])),
      to: formatTime(addOffset(data.timeBase, data.times[data.times.length - 1])),
    });
  }

  // --- file loading --------------------------------------------------------

  function setDropLabel(label) {
    var strong = dropEl.querySelector("strong");
    if (strong) {
      strong.textContent = label;
    }
  }

  function tooLarge(byteLength) {
    if (byteLength <= MAX_BYTES) {
      return false;
    }
    showErrorHtml(t("errTooLarge", { size: formatSize(byteLength) }));
    return true;
  }

  function formatSize(byteLength) {
    if (byteLength >= 1024 * 1024) {
      return (byteLength / (1024 * 1024)).toFixed(1) + " MB";
    }
    return Math.round(byteLength / 1024) + " KB";
  }

  function loadBytes(bytes, label) {
    if (tooLarge(bytes.length)) {
      return;
    }
    if (!module.open_file(bytes)) {
      selectorCard.classList.add("hidden");
      clearChart(t("emptyDefault"));
      showError(
        t("errOpen", { reason: translateError(module.last_error()) || "" })
      );
      return;
    }
    if (label) {
      setDropLabel(label);
    }
    loadTables();
  }

  function loadFromUrl(url) {
    fetch(url)
      .then(function (response) {
        if (!response.ok) {
          throw new Error(response.status + " " + response.statusText);
        }
        return response.arrayBuffer();
      })
      .then(function (buffer) {
        loadBytes(new Uint8Array(buffer), url.split("/").pop());
      })
      .catch(function (error) {
        showError(t("errFetch", { url: url, reason: error.message }));
      });
  }

  function handleFile(file) {
    if (!file) {
      return;
    }
    showError("");
    if (tooLarge(file.size)) {
      return;
    }
    var reader = new FileReader();
    reader.onload = function () {
      loadBytes(new Uint8Array(reader.result), file.name);
    };
    reader.onerror = function () {
      showError(t("errRead"));
    };
    reader.readAsArrayBuffer(file);
  }

  // --- wiring --------------------------------------------------------------

  dropEl.addEventListener("click", function () {
    fileEl.click();
  });
  fileEl.addEventListener("change", function () {
    handleFile(fileEl.files[0]);
  });
  ["dragenter", "dragover"].forEach(function (name) {
    dropEl.addEventListener(name, function (event) {
      event.preventDefault();
      dropEl.classList.add("hover");
    });
  });
  ["dragleave", "drop"].forEach(function (name) {
    dropEl.addEventListener(name, function (event) {
      event.preventDefault();
      dropEl.classList.remove("hover");
    });
  });
  dropEl.addEventListener("drop", function (event) {
    handleFile(event.dataTransfer.files[0]);
  });

  if (sampleEl) {
    sampleEl.addEventListener("click", function () {
      loadFromUrl("sample.tsfile");
    });
  }

  tableEl.addEventListener("change", function () {
    selected.table = tableEl.value;
    loadDevices();
  });

  deviceEl.addEventListener("change", function () {
    selected.device = deviceEl.value;
    loadMeasurements();
  });

  [startEl, endEl].forEach(function (input) {
    input.addEventListener("change", readSeries);
  });

  resetEl.addEventListener("click", function () {
    startEl.value = fullRange.start;
    endEl.value = fullRange.end;
    readSeries();
  });

  window.addEventListener("resize", drawChart);

  createTsFileModule().then(function (mod) {
    module = mod;
    if (sampleEl) {
      sampleEl.disabled = false;
    }
    var sample = new URLSearchParams(window.location.search).get("file");
    if (sample) {
      loadFromUrl(sample);
    }
  });

  // Embedded in a docs page, follow the site's own light/dark toggle instead
  // of the operating system preference.
  function parentDocument() {
    try {
      if (window.parent && window.parent !== window && window.parent.document) {
        return window.parent.document;
      }
    } catch (error) {
      // Cross-origin parent: fall back to the media query.
    }
    return null;
  }

  function applyParentTheme() {
    var parentDoc = parentDocument();
    if (!parentDoc) {
      return;
    }
    var parentRoot = parentDoc.documentElement;
    var dark = parentRoot.getAttribute("data-theme") === "dark" ||
      parentRoot.classList.contains("dark");
    document.documentElement.setAttribute("data-theme", dark ? "dark" : "light");
  }

  function fitFrame() {
    if (!window.frameElement) {
      return;
    }
    var height = document.documentElement.scrollHeight;
    if (height > 0) {
      window.frameElement.style.height = height + "px";
    }
  }

  var parentDoc = parentDocument();
  if (parentDoc) {
    applyParentTheme();
    new MutationObserver(function () {
      applyParentTheme();
      drawChart();
    }).observe(parentDoc.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme", "class"],
    });
  }

  if (window.frameElement) {
    window.addEventListener("load", fitFrame);
    if (window.ResizeObserver) {
      new ResizeObserver(fitFrame).observe(document.body);
    }
  }
})();
