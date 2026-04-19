import { useCallback, useEffect, useMemo, useState } from "react";
import "./App.css";

async function api(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = { raw: text };
  }
  if (!res.ok) {
    const msg = data?.error || res.statusText || "Request failed";
    throw new Error(msg);
  }
  return data;
}

const FINDING_TYPES = [
  "ALL",
  "STATISTICAL_OUTLIER",
  "DUPLICATE_TICKET",
  "SEQUENCE_GAP",
  "MISSING_OR_LOW_ACTIVITY",
  "MANIFEST_INCONSISTENCY",
];

const TYPE_LABELS = {
  ALL: "All types",
  STATISTICAL_OUTLIER: "Outliers",
  DUPLICATE_TICKET: "Duplicates",
  SEQUENCE_GAP: "Time gaps",
  MISSING_OR_LOW_ACTIVITY: "Low activity",
  MANIFEST_INCONSISTENCY: "Manifest",
};

const TABLE_LIMIT = 80;

const METRIC_CONFIG = [
  { key: "rows", label: "Tickets", accent: "metric-accent-rows" },
  { key: "sites", label: "Sites", accent: "metric-accent-sites" },
  { key: "outlier_rows", label: "Outliers", accent: "metric-accent-risk" },
  { key: "duplicate_rows", label: "Duplicates", accent: "metric-accent-alert" },
  { key: "gap_rows", label: "Gaps", accent: "metric-accent-default" },
  { key: "missing_day_rows", label: "Low activity", accent: "metric-accent-risk" },
  { key: "manifest_rows", label: "Manifest", accent: "metric-accent-alert" },
  { key: "finding_count", label: "Findings", accent: "metric-accent-total" },
];

function formatType(t) {
  if (!t) return "";
  return t.replace(/_/g, " ");
}

function findingMatchesQuery(f, q) {
  if (!q.trim()) return true;
  const s = q.toLowerCase();
  const blob = [
    f.type,
    f.severity,
    f.site_id,
    f.ticket_id,
    ...(f.reasons || []),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
  return blob.includes(s);
}

export default function App() {
  const [healthOk, setHealthOk] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);
  const [dataset, setDataset] = useState(null);
  const [analysis, setAnalysis] = useState(null);
  const [typeFilter, setTypeFilter] = useState("ALL");
  const [findingQuery, setFindingQuery] = useState("");
  const [matchForm, setMatchForm] = useState({
    ticket_id: "LIVE-CHECK-1",
    site_id: "SITE-01",
    timestamp: new Date().toISOString().slice(0, 19) + "Z",
    net_volume_bbl: "118.5",
    manifest_ref: "MNF-SITE-01-2025-01-15",
  });
  const [matchResult, setMatchResult] = useState(null);

  const ping = useCallback(async () => {
    try {
      await api("/health");
      setHealthOk(true);
    } catch {
      setHealthOk(false);
    }
  }, []);

  useEffect(() => {
    ping();
    const id = setInterval(ping, 15000);
    return () => clearInterval(id);
  }, [ping]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const s = await api("/api/v1/dataset/status");
        if (!cancelled) setDataset(s);
      } catch {
        if (!cancelled) setDataset(null);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const run = async (fn) => {
    setError(null);
    setBusy(true);
    try {
      await fn();
    } catch (e) {
      setError(e.message || String(e));
    } finally {
      setBusy(false);
    }
  };

  const loadSample = () =>
    run(async () => {
      await api("/api/v1/dataset/load-sample", { method: "POST" });
      setAnalysis(null);
      setMatchResult(null);
      await refreshStatus();
    });

  const refreshStatus = async () => {
    const s = await api("/api/v1/dataset/status");
    setDataset(s);
  };

  const runAnalysis = () =>
    run(async () => {
      const j = await api("/api/v1/analysis/run", { method: "POST" });
      setAnalysis(j);
    });

  const submitMatch = () =>
    run(async () => {
      const body = {
        ...matchForm,
        net_volume_bbl: parseFloat(matchForm.net_volume_bbl),
      };
      const j = await api("/api/v1/tickets/match", {
        method: "POST",
        body: JSON.stringify(body),
      });
      setMatchResult(j);
    });

  const hasData = (dataset?.rows ?? 0) > 0;

  const filteredFindings = useMemo(() => {
    const list = analysis?.findings || [];
    const byType = typeFilter === "ALL" ? list : list.filter((f) => f.type === typeFilter);
    return byType.filter((f) => findingMatchesQuery(f, findingQuery));
  }, [analysis, typeFilter, findingQuery]);

  const visibleRows = filteredFindings.slice(0, TABLE_LIMIT);

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark" aria-hidden>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
              <path d="M9 12l2 2 4-4" />
            </svg>
          </div>
          <div className="brand-text">
            <p className="brand-kicker">Operations</p>
            <p className="brand-title">Fraud detection</p>
          </div>
        </div>
        <div
          className={`status-pill ${healthOk === true ? "ok" : ""} ${healthOk === false ? "err" : ""}`}
          title="Checks /health every 15 seconds"
        >
          <span className="dot" aria-hidden />
          {healthOk === null && "Checking API…"}
          {healthOk === true && "API live · port 5000"}
          {healthOk === false && "API offline — start Flask"}
        </div>
      </header>

      <div className="shell">
        <section className="hero">
          <h1>Transport ticket control room</h1>
          <p className="hero-desc">
            Load tickets, run detection across twelve collection sites, then filter and search findings. Keep{" "}
            <code className="hero-code">python run.py</code> and <code className="hero-code">npm run dev</code> running
            together.
          </p>
        </section>

        <div className="workflow" aria-label="Suggested workflow">
          <div className={`workflow-step ${hasData ? "done" : ""}`}>
            <span className="step-num">{hasData ? "✓" : "1"}</span>
            <div>
              <strong>Load data</strong>
              <span>Pull the sample CSV into memory so rules can run.</span>
            </div>
          </div>
          <div className={`workflow-step ${analysis ? "done" : ""}`}>
            <span className="step-num">{analysis ? "✓" : "2"}</span>
            <div>
              <strong>Run detection</strong>
              <span>Outliers, duplicates, gaps, coverage, and manifest checks.</span>
            </div>
          </div>
          <div className={`workflow-step ${analysis ? "done" : ""}`}>
            <span className="step-num">3</span>
            <div>
              <strong>Review &amp; match</strong>
              <span>Use filters and search, then spot-check single tickets.</span>
            </div>
          </div>
        </div>

        <div className="actions" aria-busy={busy}>
          <button type="button" className="btn btn-primary" disabled={busy} onClick={loadSample}>
            Load sample dataset
          </button>
          <button type="button" className="btn btn-secondary" disabled={busy} onClick={() => run(refreshStatus)}>
            Refresh status
          </button>
          <button type="button" className="btn btn-secondary" disabled={busy || !hasData} onClick={runAnalysis}>
            Run analysis
          </button>
          {busy && (
            <span className="busy-hint" aria-live="polite">
              <span className="spinner" aria-hidden />
              Working…
            </span>
          )}
        </div>

        {error && (
          <div className="banner banner-error" role="alert">
            {error}
          </div>
        )}

        {dataset != null && (
          <div className="data-strip">
            <div>
              <p style={{ margin: 0, fontSize: "0.75rem", fontWeight: 600, textTransform: "uppercase", letterSpacing: "0.08em", color: "var(--text-muted)" }}>
                Active dataset
              </p>
              <strong>{dataset.rows?.toLocaleString() ?? "—"}</strong>
              <span style={{ color: "var(--text-secondary)", marginLeft: "0.35rem" }}>rows</span>
              {dataset.source && <p className="hint">{dataset.source}</p>}
            </div>
            {!hasData && (
              <p style={{ margin: 0, fontSize: "0.875rem", color: "var(--text-muted)", maxWidth: "18rem" }}>
                No rows loaded yet. Use <strong style={{ color: "var(--text-secondary)" }}>Load sample dataset</strong> to begin.
              </p>
            )}
          </div>
        )}

        {analysis?.summary && (
          <section className="metrics" aria-label="Analysis summary">
            {METRIC_CONFIG.map(({ key, label, accent }) => (
              <div key={key} className={`metric ${accent}`}>
                <p className="label">{label}</p>
                <p className="value">{analysis.summary[key]?.toLocaleString?.() ?? analysis.summary[key]}</p>
              </div>
            ))}
          </section>
        )}

        <section className="section" aria-label="Findings">
          <div className="section-head">
            <h2>Findings</h2>
            {!analysis && <p className="section-sub">Run analysis after loading data to populate this table.</p>}
            {analysis && (
              <p className="section-sub">
                {analysis.summary?.finding_count?.toLocaleString()} total · filter and search below
              </p>
            )}
          </div>

          {!analysis && (
            <div className="empty-panel">
              <strong>No analysis yet</strong>
              Load the sample dataset, then click &quot;Run analysis&quot; to see outliers, duplicates, and reconciliation flags.
            </div>
          )}

          {analysis && (
            <>
              <div className="findings-toolbar">
                <div className="search-field">
                  <label htmlFor="finding-search">Search findings</label>
                  <input
                    id="finding-search"
                    type="search"
                    placeholder="Site, ticket, type, reason…"
                    value={findingQuery}
                    onChange={(e) => setFindingQuery(e.target.value)}
                    autoComplete="off"
                  />
                </div>
                <div className="chip-scroll" role="tablist" aria-label="Finding type">
                  {FINDING_TYPES.map((t) => (
                    <button
                      key={t}
                      type="button"
                      role="tab"
                      aria-selected={typeFilter === t}
                      className={`chip ${typeFilter === t ? "chip-on" : ""}`}
                      onClick={() => setTypeFilter(t)}
                    >
                      {TYPE_LABELS[t] || t}
                    </button>
                  ))}
                </div>
              </div>
              <p className="table-meta">
                Showing <strong style={{ color: "var(--text)" }}>{visibleRows.length}</strong> of{" "}
                {filteredFindings.length} after filters · hard cap {TABLE_LIMIT} rows for performance
              </p>
              <div className="table-shell">
                <table>
                  <thead>
                    <tr>
                      <th>Type</th>
                      <th>Severity</th>
                      <th>Site</th>
                      <th>Ticket</th>
                      <th>Reasons</th>
                    </tr>
                  </thead>
                  <tbody>
                    {visibleRows.map((f, i) => (
                      <tr key={`${f.id}-${f.type}-${i}`}>
                        <td>
                          <span className="type-badge" title={f.type}>
                            {formatType(f.type)}
                          </span>
                        </td>
                        <td>
                          <span className={`sev-badge sev-${f.severity || "MEDIUM"}`}>{f.severity || "—"}</span>
                        </td>
                        <td className="cell-mono">{f.site_id ?? "—"}</td>
                        <td className="cell-mono">{f.ticket_id ?? "—"}</td>
                        <td className="cell-mono" style={{ maxWidth: "280px", wordBreak: "break-word" }}>
                          {(f.reasons || []).join(" · ") || "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          )}
        </section>

        <section className="section" aria-label="Ticket match">
          <div className="section-head">
            <h2>Live ticket match</h2>
            <p className="section-sub">One-off check against the loaded dataset · duplicate ID, inter-arrival gap, manifest drift</p>
          </div>
          <div className="match-grid">
            {["ticket_id", "site_id", "timestamp", "net_volume_bbl", "manifest_ref"].map((key) => (
              <div key={key} className="field">
                <label htmlFor={`match-${key}`}>{key.replace(/_/g, " ")}</label>
                <input
                  id={`match-${key}`}
                  value={matchForm[key]}
                  onChange={(e) => setMatchForm((m) => ({ ...m, [key]: e.target.value }))}
                  autoComplete="off"
                />
              </div>
            ))}
          </div>
          <button type="button" className="btn btn-primary" disabled={busy || !hasData} onClick={submitMatch}>
            Run match
          </button>
          {!hasData && (
            <p className="table-meta" style={{ marginTop: "0.75rem" }}>
              Load data first to evaluate a ticket against history.
            </p>
          )}
          {matchResult && (
            <div className={`match-outcome ${matchResult.status === "CLEAR" ? "clear" : "review"}`}>
              <div className="match-outcome-top">
                <span className="match-status-label">Result</span>
                <span className="match-status">{matchResult.status}</span>
              </div>
              {matchResult.reasons?.length > 0 ? (
                <ul className="match-reasons">
                  {matchResult.reasons.map((r) => (
                    <li key={r}>{r.replace(/_/g, " ")}</li>
                  ))}
                </ul>
              ) : (
                <p className="match-reasons" style={{ listStyle: "none", padding: 0, margin: 0 }}>
                  No flags for this ticket with current rules.
                </p>
              )}
              <pre className="match-json">{JSON.stringify(matchResult.details, null, 2)}</pre>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
