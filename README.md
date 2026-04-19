# Transport fraud detection (demo)

A small portfolio system that models **transport-style ticket** checks across **12 collection sites**: statistical outliers, duplicate tickets, long gaps between tickets at a site, thin daily activity versus a rolling baseline, and manifest volume inconsistency. It exposes a **Flask REST API** and a **React (Vite)** dashboard for loading data, running analysis, and triaging findings.

**Live-style workflow:** load a dataset → run the pipeline → review findings and optional per-ticket match checks.

## Tech stack

- **Backend:** Python 3, Flask, Pandas, NumPy  
- **Frontend:** React 19, Vite 6  
- **Data:** CSV sample generator (`scripts/generate_sample_data.py`)

## Repository

```bash
git clone git@github.com:ovalles2019/fraud-detection-system.git
cd fraud-detection-system
```

HTTPS: `https://github.com/ovalles2019/fraud-detection-system.git`

## Quick start

### 1. API (port 5000)

```bash
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python scripts/generate_sample_data.py   # creates data/sample_tickets.csv
python run.py
```

Health check: [http://127.0.0.1:5000/health](http://127.0.0.1:5000/health)

### 2. Dashboard (port 5173)

In a second terminal:

```bash
cd frontend
npm install
npm run dev
```

Open [http://127.0.0.1:5173](http://127.0.0.1:5173). The Vite dev server **proxies** `/health` and `/api` to the Flask app, so keep both processes running.

### 3. Typical demo flow

1. In the UI: **Load sample dataset** → **Run analysis**.  
2. Or with `curl`: `POST /api/v1/dataset/load-sample` then `POST /api/v1/analysis/run`.

## API overview

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness |
| `GET` | `/api/v1/meta/sites` | Collection sites in the loaded data |
| `GET` | `/api/v1/dataset/status` | Row count, columns, source |
| `POST` | `/api/v1/dataset/load-sample` | Load `data/sample_tickets.csv` |
| `POST` | `/api/v1/dataset/upload` | CSV file (`file`) or JSON `{ "records": [...] }` |
| `POST` | `/api/v1/analysis/run` | Run full pipeline; returns `summary`, `findings`, `samples` |
| `POST` | `/api/v1/tickets/match` | JSON body: `ticket_id`, `site_id`, `timestamp`, `net_volume_bbl`, `manifest_ref` |
| `POST` | `/api/v1/tickets/ingest` | Append a ticket to the in-memory store and dataset |
| `GET` | `/api/v1/investigations` | Findings wrapped as investigation stubs |
| `POST` | `/api/v1/investigations/<id>/transition` | Stub workflow state update |
| `POST` | `/api/v1/ledger/reset` | Clear ticket ledger |

Expected CSV columns for analysis: `ticket_id`, `site_id`, `timestamp`, `net_volume_bbl`, `ticket_amount_usd`, `manifest_ref`.

## Project layout

```
app/           Flask app factory, routes, in-memory dataset store
core/          Pandas/NumPy analytics (outliers, tickets, reconciliation)
data/          sample_tickets.csv (generated; safe to regenerate)
frontend/      Vite + React dashboard
scripts/       generate_sample_data.py
run.py         Dev entrypoint for the API
```

## Production notes

The API uses **in-memory** storage for the active dataset and a small ticket ledger; this is intentional for a demo. For a real deployment you would add a database, authentication, and a production WSGI server (e.g. Gunicorn) plus hosting for the built frontend (`npm run build` → `frontend/dist`).

## License

No license file is included by default; add one (e.g. MIT) if you want to clarify reuse terms.
