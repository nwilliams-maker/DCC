/* DCC Deploy Watcher v9 — loaded as a static file via <script src=>.
 * Bypasses Streamlit's f-string + iframe srcdoc pipeline that was mangling v6/v7/v8.
 * Reads INSTANCE_ID from the DOM (data-id on #dcc-instance-id) instead of Python
 * substitution, so this file is fully static and never modified.
 *
 * Wired surfaces:
 *   - 15s INSTANCE_ID poll (deploy detected mid-session)
 *   - WebSocket close hook (server gone) → 30s auto-reload, cancellable via Dismiss
 *   - 4s stuck-skeleton recovery → force reload
 *   - MutationObserver hides Streamlit skeleton placeholders silently
 *   - Stale-tab error handler (Bad message / SessionInfo / fragment id / chunk-load) → 30s auto-reload
 *   - Visibility resume → re-check INSTANCE_ID
 *   - offline event → banner
 *   - localStorage seed for cross-tab consistency
 */
(function() {
  if (window._dccWatcherV9) return;
  window._dccWatcherV9 = true;
  // Dev hint — visible in browser console for debugging shipped builds.
  try {
    var idEl = document.getElementById("dcc-instance-id");
    var bid = idEl && idEl.dataset && idEl.dataset.id;
    console.info("%c[dcc-watcher] v9 loaded · instance=" + bid, "color:#633094;font-weight:bold");
  } catch (_) {}

  var doc = document, win = window;
  var LS_KEY = "dcc_known_instance_id";
  var lastActivity = Date.now();

  function readInstanceId() {
    var el = doc.getElementById("dcc-instance-id");
    return el && el.dataset && el.dataset.id;
  }

  // First-paint INSTANCE_ID may not be in the DOM yet (Streamlit renders async).
  // Capture it lazily — first time we see a non-empty value, that's our baseline.
  var MY_ID = null;
  function ensureBaseline() {
    if (MY_ID) return MY_ID;
    var v = readInstanceId();
    if (v) {
      MY_ID = v;
      try { win.localStorage.setItem(LS_KEY, MY_ID); } catch (_) {}
    }
    return MY_ID;
  }
  ensureBaseline();
  // Poll baseline every 200ms for up to 5s in case the DOM isn't ready yet.
  var baselineAttempts = 0;
  var baselineIv = setInterval(function() {
    baselineAttempts++;
    if (ensureBaseline() || baselineAttempts > 25) clearInterval(baselineIv);
  }, 200);

  // Delegated click handler for header pill anchors with data-action attributes.
  // Email pill anchor in header has data-action="email-settings"; click forwards
  // to the hidden Streamlit button (key="_email_pill_hidden") so dialog opens
  // via a normal Streamlit rerun (no page reload).
  doc.addEventListener("click", function(e) {
    var a = e.target && e.target.closest && e.target.closest('[data-action]');
    if (!a) return;
    var action = a.getAttribute("data-action");
    if (action === "email-settings") {
      e.preventDefault();
      // Find the hidden Streamlit button and click it.
      var btn = doc.querySelector('div.st-key-_email_pill_hidden button');
      if (btn) {
        btn.click();
      }
    }
  }, true);

  ["click", "keydown", "mousemove", "scroll", "input", "touchstart"].forEach(function(ev) {
    doc.addEventListener(ev, function() { lastActivity = Date.now(); }, { capture: true, passive: true });
  });

  function showBanner(msg) {
    var bn = doc.getElementById("dcc-update-banner");
    if (!bn) return;
    if (bn.parentElement !== doc.body) {
      try { doc.body.appendChild(bn); } catch (_) {}
    }
    if (msg) {
      var sp = bn.querySelector("span");
      if (sp) sp.textContent = msg;
    }
    bn.setAttribute("style",
      "position:fixed !important;top:0 !important;left:0 !important;right:0 !important;" +
      "background:#fef3c7 !important;border-bottom:2px solid #f59e0b !important;" +
      "color:#78350f !important;padding:12px 20px !important;" +
      "font-family:system-ui,-apple-system,sans-serif !important;font-size:14px !important;" +
      "font-weight:500 !important;z-index:2147483647 !important;" +
      "display:flex !important;justify-content:space-between !important;align-items:center !important;" +
      "box-shadow:0 2px 8px rgba(0,0,0,0.15) !important;visibility:visible !important;opacity:1 !important;"
    );
  }
  win._dccShowBanner = showBanner;

  function wireButtons() {
    var rb = doc.getElementById("dcc-refresh-btn");
    if (rb && !rb._dccWired) {
      rb._dccWired = true;
      rb.addEventListener("click", function() { win.location.reload(); });
    }
    var db = doc.getElementById("dcc-dismiss-btn");
    if (db && !db._dccWired) {
      db._dccWired = true;
      db.addEventListener("click", function() {
        var x = doc.getElementById("dcc-update-banner");
        if (x) x.style.setProperty("display", "none", "important");
        if (win._dccPendingReload) {
          try { clearTimeout(win._dccPendingReload); } catch (_) {}
          win._dccPendingReload = null;
        }
      });
    }
  }
  wireButtons();
  setInterval(wireButtons, 1000);

  // Skeleton-kill CSS — hides Streamlit's reconnect placeholder bars.
  if (!doc.getElementById("dcc-kill-skeleton-css")) {
    var st = doc.createElement("style");
    st.id = "dcc-kill-skeleton-css";
    st.textContent =
      '[data-testid="stSkeleton"], .stSkeleton { display:none !important; visibility:hidden !important; }' +
      '[class*="ReconnectDialog" i] { display:none !important; }' +
      'div[data-testid="stStatusWidget"][data-status="error"] { display:none !important; }';
    doc.head.appendChild(st);
  }

  // 15s INSTANCE_ID poll — catches deploys that happen while tab is open.
  setInterval(function() {
    wireButtons();
    var bn = doc.getElementById("dcc-update-banner");
    if (bn && bn.parentElement !== doc.body) {
      try { doc.body.appendChild(bn); } catch (_) {}
    }
    var cur = readInstanceId();
    var baseline = ensureBaseline();
    if (cur && baseline && cur !== baseline) {
      try { win.localStorage.setItem(LS_KEY, cur); } catch (_) {}
      var idleMs = Date.now() - lastActivity;
      if (idleMs > 600000) { win.location.reload(); }
      else { showBanner(); }
    }
  }, 15000);

  // 4s stuck-skeleton recovery — force reload if Streamlit is in skeleton state with no real content.
  var stuckCheckStart = null;
  setInterval(function() {
    var stApp = doc.querySelector('[data-testid="stApp"]');
    if (!stApp) return;
    var connState = stApp.getAttribute("data-test-connection-state") || "";
    var hasContent = !!doc.querySelector('[data-testid="stMarkdown"], [data-testid="stHorizontalBlock"], [data-testid="stTabs"], [data-testid="stForm"]');
    var skeletonCount = doc.querySelectorAll('[data-testid="stSkeleton"], .stSkeleton').length;
    var isStuck = (connState === "DISCONNECTED") || (!hasContent && skeletonCount > 0);
    if (isStuck) {
      if (stuckCheckStart === null) stuckCheckStart = Date.now();
      if (Date.now() - stuckCheckStart > 4000) { win.location.reload(); }
    } else {
      stuckCheckStart = null;
    }
  }, 2000);

  // WebSocket hook — fires on Streamlit's stream WS close.
  try {
    var OrigWS = win.WebSocket;
    if (OrigWS && !win._dccWSHooked) {
      win._dccWSHooked = true;
      var W = function(url, p) {
        var ws = (p !== undefined) ? new OrigWS(url, p) : new OrigWS(url);
        try {
          if (typeof url === "string" && url.indexOf("/_stcore/stream") >= 0) {
            var openedOnce = false;
            ws.addEventListener("open", function() { openedOnce = true; });
            ws.addEventListener("close", function(ev) {
              if (openedOnce && ev && ev.code !== 1000 && ev.code !== 1001) {
                showBanner("📦 App was updated — auto-refreshing in 30s. Click Reload now to skip, Dismiss to stay.");
                if (win._dccPendingReload) {
                  try { clearTimeout(win._dccPendingReload); } catch (_) {}
                }
                win._dccPendingReload = setTimeout(function() { win.location.reload(); }, 30000);
              }
            });
          }
        } catch (_) {}
        return ws;
      };
      W.prototype = OrigWS.prototype;
      W.CONNECTING = OrigWS.CONNECTING;
      W.OPEN = OrigWS.OPEN;
      W.CLOSING = OrigWS.CLOSING;
      W.CLOSED = OrigWS.CLOSED;
      win.WebSocket = W;
    }
  } catch (_) {}


  // Streamlit server-side exception DOM watcher — catches the red error box that
  // appears when the runtime hits "Could not find fragment with id ..." after a
  // deploy. The watcher's window.error listener doesn't catch this because
  // Streamlit serializes the error via the WebSocket and renders it as DOM,
  // not as a JS exception. So we listen for the DOM rendering directly.
  function isFragmentErr(text) {
    return /Could not find fragment with id|Could not find fragment id|Bad message format|Bad .setIn. index|SessionInfo|RuntimeError.*fragment/i.test(text);
  }
  function checkExceptionBox(node) {
    if (!node || node.nodeType !== 1) return false;
    var t = node.getAttribute && node.getAttribute("data-testid");
    var text = "";
    if (t === "stException" || t === "stExceptionMessage" || t === "stAlert") {
      text = node.innerText || "";
    } else if (node.querySelector && node.querySelector('[data-testid="stException"], [data-testid="stExceptionMessage"]')) {
      var inner = node.querySelector('[data-testid="stException"], [data-testid="stExceptionMessage"]');
      text = inner.innerText || "";
    }
    if (text && isFragmentErr(text)) {
      try { node.style.setProperty("display", "none", "important"); } catch (_) {}
      showBanner("📦 Reconnecting — auto-refreshing in 30s. Click Reload now to skip, Dismiss to stay.");
      if (win._dccPendingReload) {
        try { clearTimeout(win._dccPendingReload); } catch (_) {}
      }
      win._dccPendingReload = setTimeout(function() { win.location.reload(); }, 30000);
      return true;
    }
    return false;
  }
  try {
    var exMo = new MutationObserver(function(muts) {
      for (var i = 0; i < muts.length; i++) {
        var added = muts[i].addedNodes || [];
        for (var j = 0; j < added.length; j++) {
          if (checkExceptionBox(added[j])) return;
        }
      }
    });
    exMo.observe(doc.body, { childList: true, subtree: true });
    // Also scan once at script start in case the error is already rendered.
    var existing = doc.querySelectorAll('[data-testid="stException"], [data-testid="stExceptionMessage"], [data-testid="stAlert"]');
    for (var i = 0; i < existing.length; i++) checkExceptionBox(existing[i]);
  } catch (_) {}

  // MutationObserver — silently hides any skeleton elements that appear after first paint.
  try {
    var mo = new MutationObserver(function(muts) {
      for (var i = 0; i < muts.length; i++) {
        var added = muts[i].addedNodes || [];
        for (var j = 0; j < added.length; j++) {
          var n = added[j];
          if (n && n.nodeType === 1) {
            if (n.getAttribute && n.getAttribute("data-testid") === "stSkeleton") {
              try { n.style.setProperty("display", "none", "important"); } catch (_) {}
            }
            if (n.querySelectorAll) {
              var inner = n.querySelectorAll('[data-testid="stSkeleton"], .stSkeleton');
              for (var k = 0; k < inner.length; k++) {
                try { inner[k].style.setProperty("display", "none", "important"); } catch (_) {}
              }
            }
          }
        }
      }
    });
    mo.observe(doc.body, { childList: true, subtree: true });
  } catch (_) {}

  // Stale-tab Streamlit errors — Bad message / SessionInfo / fragment id / chunk load failures.
  function isStreamlitErr(m) {
    return /Bad message format|Bad .setIn. index|Could not find fragment id|Could not find fragment with id|SessionInfo|ChunkLoadError|Loading chunk \d+ failed|Loading CSS chunk|NetworkError|Failed to fetch/i.test(m);
  }
  win.addEventListener("error", function(e) {
    var m = String((e && e.message) || (e && e.error && e.error.message) || "");
    if (isStreamlitErr(m)) {
      try { e.preventDefault(); } catch (_) {}
      showBanner("📦 Reconnecting — auto-refreshing in 30s. Click Reload now to skip, Dismiss to stay.");
      if (win._dccPendingReload) {
        try { clearTimeout(win._dccPendingReload); } catch (_) {}
      }
      win._dccPendingReload = setTimeout(function() { win.location.reload(); }, 30000);
    }
  }, true);
  win.addEventListener("unhandledrejection", function(e) {
    var m = "";
    try { m = String((e.reason && (e.reason.message || e.reason)) || ""); } catch (_) {}
    if (isStreamlitErr(m)) {
      try { e.preventDefault(); } catch (_) {}
      showBanner("📦 Reconnecting — auto-refreshing in 30s. Click Reload now to skip, Dismiss to stay.");
      if (win._dccPendingReload) {
        try { clearTimeout(win._dccPendingReload); } catch (_) {}
      }
      win._dccPendingReload = setTimeout(function() { win.location.reload(); }, 30000);
    }
  }, true);

  // Visibility resume — re-check INSTANCE_ID when tab refocuses.
  doc.addEventListener("visibilitychange", function() {
    if (doc.visibilityState === "visible") {
      var cur = readInstanceId();
      var baseline = ensureBaseline();
      if (cur && baseline && cur !== baseline) {
        try { win.localStorage.setItem(LS_KEY, cur); } catch (_) {}
        showBanner();
      }
    }
  });

  // offline → banner
  win.addEventListener("offline", function() {
    showBanner("📡 Connection lost — click Reload now once back online.");
  });
})();
