# MatchPulse

MatchPulse is a personal job-tracking dashboard that monitors selected career boards, scores new roles against an active PDF resume through LiteLLM, and can send a daily email digest for high-affinity matches.

## Project structure

- `frontend/` — React 19 + Vinext dashboard with four product views
- `backend/` — FastAPI, SQLModel, SQLite, and PDF ingestion
- `agent/` — Playwright scraper, LiteLLM matcher, and SMTP notifier
- `data/` — local SQLite database created on first backend start

## Local setup

The frontend requires Node.js 22.13 or newer. The backend requires Python 3.10 or newer.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
playwright install chromium
cp .env.example .env
```

Add only the provider and email credentials you intend to use to `.env`. The application starts without credentials; AI matching and email delivery will remain disabled until configured.

Start the backend:

```bash
uvicorn backend.app.main:app --reload --port 8000
```

In a second terminal, start the frontend:

```bash
cd frontend
npm install
npm run dev
```

Open the local URL printed by the frontend. Seed companies and sample jobs are added only when the database is empty.

## Run the background agent

```bash
python -m agent.run_agent
```

Only companies with **Agent monitored** enabled are crawled. Jobs scoring at least `8.0` are eligible for the email digest.

## Configuration

- `NEXT_PUBLIC_API_BASE_URL` changes the frontend API origin when it is not `http://localhost:8000/api`.
- `AI_MODEL` selects any supported LiteLLM model from the dashboard list.
- `GEMINI_API_KEY`, `OPENAI_API_KEY`, or `ANTHROPIC_API_KEY` provides the selected model credential.
- SMTP values in `.env.example` enable the optional digest.

Never commit `.env`, uploaded resumes, or `data/tracker.db`.
