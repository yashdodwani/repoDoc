# 🩺 RepoDoctor — Autonomous Bug Fixing Agent

> **"Give me a repo → I find, reproduce, fix, and PR bugs automatically — with explainable reasoning."**

Powered by **OpenClaw** (autonomous agent loop) + Claude Sonnet + GitHub API + Telegram.

---

## 🏗️ Architecture

```
Telegram Bot ──/analyze──▶ FastAPI Backend ──▶ OpenClaw Agent Loop
                                                      │
                          ┌───────────────────────────┤
                          │                           │
                    🔍 Observe                  🧠 Decide
                    Clone repo                 Run tests
                    Map files                  Lint check
                          │                   Logic scan
                          │                           │
                    ✅ Ship ◀──────────────── 🛠️ Act + Verify
                    GitHub PR                  Fix → Test
                                              Retry ≤ 2×
```

## 🚀 Quick Start

### 1. Clone & configure

```bash
git clone https://github.com/yourorg/repodoctor
cd repodoctor
cp .env.example .env
# Fill in:
#   ANTHROPIC_API_KEY=sk-ant-...
#   TELEGRAM_BOT_TOKEN=...  (from @BotFather)
```

### 2. Run with Docker

```bash
docker-compose up --build
```

- **Dashboard**: http://localhost:3000
- **API**: http://localhost:8000/docs

### 3. Use the Telegram bot

```
/analyze https://github.com/user/repo
/analyze https://github.com/user/repo ghp_yourtoken123
```

### 4. Use the REST API directly

```bash
curl -X POST http://localhost:8000/analyze \
  -H "Content-Type: application/json" \
  -d '{"repo_url": "https://github.com/pallets/flask"}'
```

Stream live agent steps:
```bash
curl http://localhost:8000/stream/<job_id>
```

---

## 🧠 OpenClaw Agent Loop

The agent follows a strict **observe → decide → act → verify → repeat** loop:

| Phase | Action | Tools Used |
|-------|--------|-----------|
| **Observe** | Clone repo, build file map | `git clone`, AST scan |
| **Decide** | Run tests, lint, Claude logic scan | `pytest`, `flake8`, Anthropic API |
| **Act** | Generate minimal diff fix | Claude Sonnet (surgical prompt) |
| **Verify** | Apply patch → run tests | `pytest`, `py_compile` |
| **Retry** | Revert + retry (max 2×) | Loop |
| **Ship** | Create GitHub PR | GitHub REST API |

---

## 🐛 Bug Categories Detected

| Category | Detection Method |
|----------|----------------|
| ✅ Failing Tests | `pytest --tb=short` + stack trace parsing |
| ✅ Lint Errors | `flake8 --select=E711,E712,E9,F8` |
| ✅ Logical Bugs | Claude API: null checks, off-by-one, wrong conditions |

---

## 📡 API Reference

### `POST /analyze`
```json
{
  "repo_url": "https://github.com/user/repo",
  "telegram_chat_id": "optional",
  "github_token": "optional ghp_..."
}
```
Returns `{ "job_id": "job_20250425_143022" }`

### `GET /job/{job_id}`
Returns full job state including steps, bugs found, fixes applied, PR URL.

### `GET /stream/{job_id}`
SSE stream of real-time agent steps.

---

## 🗂️ Project Structure

```
repodoctor/
├── backend/
│   ├── main.py          # FastAPI + OpenClaw agent
│   ├── requirements.txt
│   └── Dockerfile
├── bot/
│   ├── bot.py           # Telegram bot
│   └── Dockerfile
├── frontend/
│   └── index.html       # Dashboard UI
├── docker-compose.yml
└── README.md
```
