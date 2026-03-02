# Sales Data App

Streamlit control panel for a lead pipeline: ingest, clean, enrich, deduplicate against Attio, manage campaign lists, create JustCall campaigns, and transcribe call recordings to Attio.

## Local setup

1. **Clone and enter the repo**
   ```bash
   git clone <your-repo-url>
   cd sales-data-app-deploy
   ```

2. **Create a virtual environment and install dependencies**
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Configure environment**
   - Copy `.env.example` to `.env`
   - Fill in API keys (Attio, JustCall, OpenRouter, Mistral as needed) and optional `APP_PASSWORD`

4. **Run the app**
   ```bash
   streamlit run app.py
   ```

5. **Optional: transcribe calls from the CLI**
   ```bash
   python transcribe_calls.py --help
   ```

## Deploy to Railway

1. **Push this folder to a new GitHub repo**  
   Use the contents of `sales-data-app-deploy` as the root of the repo (so `app.py`, `Dockerfile`, `lib/`, etc. are at the repo root).

2. **Create a Railway project**
   - [railway.app](https://railway.app) → **New** → **Deploy from GitHub repo**
   - Select the repository. Railway will use the **Dockerfile** to build.

3. **Add a persistent volume (for SQLite)**
   - Service → **Volumes** → **Add Volume**
   - Mount path: `/data`  
   The app expects `DB_PATH=/data/leads.db` so the database persists across deploys.

4. **Set environment variables**  
   In the service **Variables** tab, add (from `.env.example`):

   | Variable | Required | Notes |
   |----------|----------|--------|
   | `DB_PATH` | Optional | Default `/data/leads.db` if using the volume above. |
   | `ATTIO_API_KEY` | For dedup/sync | From Attio. |
   | `JUSTCALL_API_KEY` | For campaigns | From JustCall. |
   | `JUSTCALL_API_SECRET` | For campaigns | From JustCall. |
   | `OPENROUTER_API_KEY` | For enrichment | Clean & Enrich tab (LLM). |
   | `MISTRAL_API_KEY` | For transcripts | Transcripts tab. |
   | `APP_PASSWORD` | Optional | Password-protect the app. |
   | `JINA_API_KEY` | Optional | Web-search fallback in enrichment. |

   Railway sets `PORT` automatically; no need to add it.

5. **Deploy**  
   Push to GitHub or click **Deploy** in Railway. After the build, open the generated domain. You should see the Streamlit app (or the password screen if `APP_PASSWORD` is set).

### If the repo root is a subfolder

If this app lives in a subfolder (e.g. `sales-data-app-deploy/` inside a monorepo), set Railway **Root Directory** to that folder so the Dockerfile and `app.py` are found.

### Troubleshooting

- **Database empty after redeploy** — Ensure a volume is mounted at `/data` and `DB_PATH` is `/data/leads.db`.
- **502 or slow start** — Streamlit can take a moment to start; check service logs.
- **Build timeout on `crawl4ai-setup`** — You can comment out `RUN crawl4ai-setup` in the Dockerfile; the app runs but URL enrichment in Clean & Enrich may be limited until crawl4ai is configured.

## Project layout

- `app.py` — Streamlit entry point
- `lib/` — DB, config, Attio/JustCall clients, enrichment, transcript processing
- `tabs/` — Ingest, Clean & Enrich, Campaign Lists, Campaigns, Transcripts, Settings
- `scraper/` — Directory scraper and website enricher (used by the app)
- `transcribe_calls.py` — CLI for transcribing JustCall recordings and syncing to Attio
