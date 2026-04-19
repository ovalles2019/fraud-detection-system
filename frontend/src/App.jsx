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

const TABLE_LIMIT = 80;

export default function App() {
  const [healthOk, setHealthOk] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);
  const [dataset, setDataset] = useState(null);
  const [analysis, setAnalysis] = useState(null);
  const [typeFilter, setTypeFilter] = useState("ALL");
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

  const filteredFindings = useMemo(() => {
    const list = analysis?.findings || [];
    if (typeFilter === "ALL") return list;
    return list.filter((f) => f.type === typeFilter);
  }, [analysis, typeFilter]);

  const visibleRows = filteredFindings.slice(0, TABLE_LIMIT);

  return (
    <div className="shell">
      <header className="top">
        <div>
          <p className="eyebrow">Transport documentation</p>
          <h1>Fraud detection dashboard</h1>
          <p className="sub">
            Load sample tickets, run statistical and reconciliation checks, and triage findings. Start the
            Flask API on port 5000, then run the Vite dev server — requests proxy automatically.
          </p>
        </div>
        <div
          className={`status-pill ${healthOk === true ? "ok" : ""} ${healthOk === false ? "err" : ""}`}
          title="GET /health every 15s"
        >
          <span className="dot" />
          {healthOk === null && "Checking API…"}
          {healthOk === true && "API connected"}
          {healthOk === false && "API unreachable (is Flask on :5000?)"}
        </div>
      </header>

      <div className="toolbar">
        <button type="button" className="btn btn-primary" disabled={busy} onClick={loadSample}>
          Load sample dataset
        </button>
        <button type="button" className="btn btn-ghost" disabled={busy} onClick={() => run(refreshStatus)}>
          Refresh status
        </button>
        <button type="button" className="btn btn-ghost" disabled={busy} onClick={runAnalysis}>
          Run analysis
        </button>
      </div>

      {error && (
        <div className="banner banner-error" role="alert">
          {error}
        </div>
      )}

      {dataset && (
        <p className="meta-line">
          Dataset: <strong>{dataset.rows?.toLocaleString() ?? "—"}</strong> rows
          {dataset.source ? ` · ${dataset.source}` : ""}
        </p>
      )}

      {analysis?.summary && (
        <section className="grid" aria-label="Summary metrics">
          {[
            ["Rows", analysis.summary.rows],
            ["Sites", analysis.summary.sites],
            ["Outliers", analysis.summary.outlier_rows],
            ["Duplicates", analysis.summary.duplicate_rows],
            ["Gaps", analysis.summary.gap_rows],
            ["Low activity", analysis.summary.missing_day_rows],
            ["Manifest", analysis.summary.manifest_rows],
            ["Findings", analysis.summary.finding_count],
          ].map(([label, val]) => (
            <div key={label} className="card">
              <p className="label">{label}</p>
              <p className="value">{val?.toLocaleString?.() ?? val}</p>
            </div>
          ))}
        </section>
      )}

      {analysis && (
        <section className="panel" aria-label="Findings">
          <h2>Findings</h2>
          <div className="filters">
            {FINDING_TYPES.map((t) => (
              <button
                key={t}
                type="button"
                className={`chip ${typeFilter === t ? "on" : ""}`}
                onClick={() => setTypeFilter(t)}
              >
                {t === "ALL" ? "All types" : t.replace(/_/g, " ").toLowerCase()}
              </button>
            ))}
          </div>
          <p className="meta-line" style={{ marginBottom: "0.75rem" }}>
            Showing {visibleRows.length} of {filteredFindings.length} (filter) · cap {TABLE_LIMIT} rows
          </p>
          <div className="table-wrap">
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
                {visibleRows.map((f) => (
                  <tr key={`${f.id}-${f.type}`}>
                    <td>{f.type?.replace(/_/g, " ")}</td>
                    <td className={`sev-${f.severity || "MEDIUM"}`}>{f.severity}</td>
                    <td>{f.site_id ?? "—"}</td>
                    <td>{f.ticket_id ?? "—"}</td>
                    <td>{(f.reasons || []).join(", ")}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      <section className="panel" aria-label="Ticket match">
        <h2>Ticket match (live check)</h2>
        <p className="meta-line" style={{ marginBottom: "0.85rem" }}>
          POST /api/v1/tickets/match — duplicate ID, long gap since last site ticket, or manifest volume drift.
        </p>
        <div className="match-grid">
          {["ticket_id", "site_id", "timestamp", "net_volume_bbl", "manifest_ref"].map((key) => (
            <div key={key} className="field">
              <label htmlFor={key}>{key}</label>
              <input
                id={key}
                value={matchForm[key]}
                onChange={(e) => setMatchForm((m) => ({ ...m, [key]: e.target.value }))}
              />
            </div>
          ))}
        </div>
        <button type="button" className="btn btn-primary" disabled={busy} onClick={submitMatch}>
          Run match
        </button>
        {matchResult && (
          <pre className="match-result">
            {JSON.stringify(matchResult, null, 2)}
          </pre>
        )}
      </section>
    </div>
  );
}
