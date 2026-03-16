# Ask My Amazon Account

A local, on-demand interface for querying an Amazon Seller account using natural language via Cursor.

**How it works:** Ask a question in natural language → Cursor writes and runs a Python script → data is pulled fresh from the SP-API → raw response is saved locally → Cursor answers from the downloaded file.

No database. No scheduled syncing. Every Cursor thread pulls fresh data.

---

## Prerequisites

### 1. SP-API Credentials (one-time setup)

Before anything works, you need SP-API developer credentials from Amazon:

1. Log into Seller Central → **Apps & Services → Develop Apps**
2. Register as a developer (a private/self-authorized app is sufficient)
3. Create an app — note your **Client ID** and **Client Secret**
4. Complete the OAuth flow to get your **Refresh Token**
5. Note your **Marketplace ID** (US = `ATVPDKIKX0DER`)

> This review process can take a few days. It is the only meaningful upfront friction.

### 2. Python 3.9+

---

## Setup

```bash
# 1. Clone / open this folder in Cursor

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # macOS/Linux
# .venv\Scripts\activate         # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Add your credentials
cp .env.example .env
# Open .env and fill in all four values

# 5. Validate the setup
python test_auth.py
```

---

## Credentials (`.env`)

```
SP_API_CLIENT_ID=your_client_id
SP_API_CLIENT_SECRET=your_client_secret
SP_API_REFRESH_TOKEN=your_refresh_token
SP_API_MARKETPLACE_ID=ATVPDKIKX0DER
```

`.env` is gitignored — never commit it.

---

## Usage

Open a new Cursor chat and ask a question in natural language:

> "How many orders did I get last week?"
> "What were my top 5 selling SKUs in January?"
> "Show me my current FBA inventory levels."
> "What fees did I pay last month?"

Cursor will:
1. Determine the right endpoint or report type
2. Write a Python script
3. Run it in the terminal
4. Save the raw response to `/data/`
5. Answer your question from the downloaded data

---

## File Structure

```
/project-root
  .env                  # Your credentials (gitignored)
  .env.example          # Credential template
  auth.py               # Shared auth helper — imported by all scripts
  requirements.txt      # Dependencies
  test_auth.py          # Setup validation script
  /data
    /orders             # Orders API responses
    /inventory          # Inventory API responses
    /reports            # Downloaded report files
  /.cursor/rules/       # Rules that guide Cursor's script-writing behavior
```

---

## Two Types of API Calls

**Direct calls** (fast, synchronous) — orders, inventory, listings, catalog data.

**Report-based calls** (async: request → poll → download) — sales summaries, fee reports, settlement data. These take 30 seconds to a few minutes. Cursor handles the poll loop automatically.

---

## Notes

- Raw data files accumulate in `/data/` as a passive archive. Individual files are gitignored but the folder structure is tracked.
- Each new Cursor thread pulls fresh data — there is no caching or staleness logic.
- `auth.py` is the single source of credentials. Never rewrite or bypass it.
