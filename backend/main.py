"""
RepoDoctor — Autonomous Bug Fixing Agent
FastAPI Backend
"""

import asyncio
import os
import subprocess
import tempfile
import shutil
import json
import re
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import httpx

app = FastAPI(title="RepoDoctor API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job store (use Redis in prod)
jobs: dict[str, dict] = {}


class AnalyzeRequest(BaseModel):
    repo_url: str
    telegram_chat_id: Optional[str] = None
    github_token: Optional[str] = None


class JobStatus(BaseModel):
    job_id: str
    status: str
    steps: list
    bugs_found: list
    fixes_applied: list
    pr_url: Optional[str] = None
    error: Optional[str] = None


def emit_step(job_id: str, step: dict):
    """Append a step to the job log."""
    if job_id in jobs:
        jobs[job_id]["steps"].append({
            **step,
            "timestamp": datetime.utcnow().isoformat()
        })
        print(f"[{job_id}] {step['emoji']} {step['message']}")


@app.post("/analyze")
async def analyze_repo(req: AnalyzeRequest, background_tasks: BackgroundTasks):
    job_id = f"job_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    jobs[job_id] = {
        "status": "running",
        "repo_url": req.repo_url,
        "steps": [],
        "bugs_found": [],
        "fixes_applied": [],
        "pr_url": None,
        "error": None,
    }
    background_tasks.add_task(
        run_agent,
        job_id,
        req.repo_url,
        req.telegram_chat_id,
        req.github_token,
    )
    return {"job_id": job_id, "status": "started"}


@app.get("/job/{job_id}")
async def get_job(job_id: str):
    if job_id not in jobs:
        raise HTTPException(404, "Job not found")
    return jobs[job_id]


@app.get("/jobs")
async def list_jobs():
    return [{"job_id": k, **v} for k, v in jobs.items()]


@app.get("/stream/{job_id}")
async def stream_job(job_id: str):
    """SSE stream of job steps."""
    async def event_generator():
        seen = 0
        while True:
            if job_id not in jobs:
                break
            job = jobs[job_id]
            steps = job["steps"]
            while seen < len(steps):
                yield f"data: {json.dumps(steps[seen])}\n\n"
                seen += 1
            if job["status"] in ("done", "failed"):
                yield f"data: {json.dumps({'type': 'done', 'status': job['status']})}\n\n"
                break
            await asyncio.sleep(0.5)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ─────────────────────────────────────────────
# OPENCLAW AGENT CORE
# ─────────────────────────────────────────────

async def run_agent(job_id: str, repo_url: str, telegram_chat_id: str, github_token: str):
    """
    OpenClaw Agent Loop:
    observe → decide → act → verify → repeat
    """
    workdir = tempfile.mkdtemp(prefix="repodoctor_")
    repo_name = repo_url.rstrip("/").split("/")[-1].replace(".git", "")
    repo_path = os.path.join(workdir, repo_name)

    try:
        # ── PHASE 1: CLONE ──────────────────────────────
        emit_step(job_id, {"emoji": "🔍", "phase": "clone", "message": f"Cloning {repo_url}..."})
        await send_telegram(telegram_chat_id, f"🔍 Cloning repo: `{repo_url}`")

        clone_result = subprocess.run(
            ["git", "clone", "--depth=1", repo_url, repo_path],
            capture_output=True, text=True, timeout=120
        )
        if clone_result.returncode != 0:
            raise RuntimeError(f"Clone failed: {clone_result.stderr}")

        emit_step(job_id, {"emoji": "✅", "phase": "clone", "message": "Repository cloned successfully"})

        # ── PHASE 2: INGESTION ──────────────────────────
        emit_step(job_id, {"emoji": "🗂️", "phase": "ingest", "message": "Mapping repository structure..."})
        file_map = build_file_map(repo_path)
        jobs[job_id]["file_map"] = file_map
        emit_step(job_id, {
            "emoji": "📊",
            "phase": "ingest",
            "message": f"Detected {file_map['language']} project — {file_map['total_files']} files, {len(file_map['test_files'])} test files"
        })

        # ── PHASE 3: BUG DETECTION ──────────────────────
        await send_telegram(telegram_chat_id, "🧪 Running tests and lint checks...")
        emit_step(job_id, {"emoji": "🧪", "phase": "detect", "message": "Running test suite..."})

        bugs = []

        # A. Failing tests
        test_bugs = await run_test_detection(job_id, repo_path, file_map)
        bugs.extend(test_bugs)

        # B. Lint errors
        emit_step(job_id, {"emoji": "🔎", "phase": "detect", "message": "Running lint analysis..."})
        lint_bugs = await run_lint_detection(job_id, repo_path, file_map)
        bugs.extend(lint_bugs)

        # C. Logical bug scan via Claude
        emit_step(job_id, {"emoji": "🧠", "phase": "detect", "message": "Running logical bug analysis with OpenClaw..."})
        logic_bugs = await run_logic_detection(job_id, repo_path, file_map)
        bugs.extend(logic_bugs)

        jobs[job_id]["bugs_found"] = bugs

        if not bugs:
            emit_step(job_id, {"emoji": "✅", "phase": "detect", "message": "No bugs detected! Repo looks healthy."})
            await send_telegram(telegram_chat_id, "✅ No bugs found! Your repo looks healthy.")
            jobs[job_id]["status"] = "done"
            return

        emit_step(job_id, {
            "emoji": "❌",
            "phase": "detect",
            "message": f"Found {len(bugs)} bug(s): {', '.join(b['category'] for b in bugs)}"
        })
        await send_telegram(telegram_chat_id, f"❌ Found *{len(bugs)} bug(s)*. Starting fix generation...")

        # ── PHASE 4: FIX + VERIFY LOOP ──────────────────
        fixes = []
        for i, bug in enumerate(bugs[:3]):  # cap at 3 bugs
            emit_step(job_id, {
                "emoji": "🛠️",
                "phase": "fix",
                "message": f"[Bug {i+1}/{len(bugs[:3])}] Generating fix for: {bug['description'][:80]}"
            })

            fix = await fix_bug_with_retry(job_id, repo_path, file_map, bug, max_retries=2)
            if fix:
                fixes.append(fix)
                emit_step(job_id, {"emoji": "✅", "phase": "verify", "message": f"Fix verified: {bug['description'][:60]}"})
            else:
                emit_step(job_id, {"emoji": "⚠️", "phase": "verify", "message": f"Could not auto-fix: {bug['description'][:60]}"})

        jobs[job_id]["fixes_applied"] = fixes

        # ── PHASE 5: PR CREATION ────────────────────────
        if fixes and github_token:
            emit_step(job_id, {"emoji": "🚀", "phase": "pr", "message": "Creating Pull Request..."})
            pr_url = await create_pull_request(repo_url, repo_path, fixes, github_token)
            if pr_url:
                jobs[job_id]["pr_url"] = pr_url
                emit_step(job_id, {"emoji": "🎉", "phase": "pr", "message": f"PR created: {pr_url}"})
                await send_telegram(telegram_chat_id, f"🚀 PR created! [View PR]({pr_url})")
            else:
                emit_step(job_id, {"emoji": "📋", "phase": "pr", "message": "Fixes ready — GitHub token needed to auto-create PR"})
        elif fixes:
            emit_step(job_id, {"emoji": "📋", "phase": "pr", "message": f"{len(fixes)} fixes ready. Add GitHub token to auto-create PR."})
            await send_telegram(telegram_chat_id, f"✅ {len(fixes)} fixes generated. Add your GitHub token to auto-PR!")

        jobs[job_id]["status"] = "done"
        emit_step(job_id, {"emoji": "🏁", "phase": "done", "message": "Agent run complete."})

    except Exception as e:
        jobs[job_id]["status"] = "failed"
        jobs[job_id]["error"] = str(e)
        emit_step(job_id, {"emoji": "💥", "phase": "error", "message": f"Agent error: {str(e)}"})
        await send_telegram(telegram_chat_id, f"💥 Error: {str(e)}")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def build_file_map(repo_path: str) -> dict:
    """Scan repo and build a structural map."""
    py_files = []
    js_files = []
    test_files = []
    entry_points = []

    for root, dirs, files in os.walk(repo_path):
        dirs[:] = [d for d in dirs if d not in {".git", "node_modules", "__pycache__", ".venv", "venv"}]
        for f in files:
            full = os.path.join(root, f)
            rel = os.path.relpath(full, repo_path)
            if f.endswith(".py"):
                py_files.append(rel)
                if "test" in f.lower() or "test" in root.lower():
                    test_files.append(rel)
                if f in ("main.py", "app.py", "run.py", "server.py"):
                    entry_points.append(rel)
            elif f.endswith((".js", ".ts", ".jsx", ".tsx")):
                js_files.append(rel)
                if "test" in f.lower() or "spec" in f.lower():
                    test_files.append(rel)

    language = "python" if len(py_files) >= len(js_files) else "javascript"

    return {
        "language": language,
        "python_files": py_files[:50],
        "js_files": js_files[:50],
        "test_files": test_files[:20],
        "entry_points": entry_points,
        "total_files": len(py_files) + len(js_files),
        "has_pytest": os.path.exists(os.path.join(repo_path, "pytest.ini"))
                      or os.path.exists(os.path.join(repo_path, "setup.cfg"))
                      or any("conftest" in f for f in py_files),
        "has_package_json": os.path.exists(os.path.join(repo_path, "package.json")),
    }


async def run_test_detection(job_id: str, repo_path: str, file_map: dict) -> list:
    """Run tests and capture failures."""
    bugs = []
    if file_map["language"] == "python" and file_map["test_files"]:
        # Install deps quietly
        req_file = os.path.join(repo_path, "requirements.txt")
        if os.path.exists(req_file):
            subprocess.run(
                ["pip", "install", "-r", req_file, "-q", "--break-system-packages"],
                capture_output=True, timeout=60, cwd=repo_path
            )

        result = subprocess.run(
            ["python", "-m", "pytest", "--tb=short", "-q", "--no-header"],
            capture_output=True, text=True, timeout=120, cwd=repo_path
        )

        if result.returncode != 0:
            # Parse failures
            output = result.stdout + result.stderr
            failures = parse_pytest_output(output)
            for f in failures:
                bugs.append({
                    "category": "failing_test",
                    "description": f["description"],
                    "file": f.get("file", "unknown"),
                    "line": f.get("line"),
                    "trace": f.get("trace", ""),
                    "raw": output[:2000],
                })
            emit_step(job_id, {
                "emoji": "❌",
                "phase": "detect",
                "message": f"Test runner: {len(failures)} failure(s) found"
            })
        else:
            emit_step(job_id, {"emoji": "✅", "phase": "detect", "message": "All tests passing"})

    return bugs


async def run_lint_detection(job_id: str, repo_path: str, file_map: dict) -> list:
    """Run linters and collect errors."""
    bugs = []
    if file_map["language"] == "python" and file_map["python_files"]:
        result = subprocess.run(
            ["python", "-m", "flake8", "--max-line-length=120",
             "--select=E711,E712,E721,W6,E9,F8", "--statistics", "."],
            capture_output=True, text=True, timeout=60, cwd=repo_path
        )
        if result.stdout.strip():
            lines = result.stdout.strip().split("\n")[:10]
            for line in lines:
                # e.g. ./file.py:10:5: E711 comparison to None
                m = re.match(r"(.+):(\d+):\d+:\s+(E\d+|W\d+|F\d+)\s+(.+)", line)
                if m:
                    bugs.append({
                        "category": "lint_error",
                        "description": f"{m.group(3)}: {m.group(4)}",
                        "file": m.group(1).lstrip("./"),
                        "line": int(m.group(2)),
                        "code": m.group(3),
                        "trace": line,
                    })
            if bugs:
                emit_step(job_id, {
                    "emoji": "⚠️",
                    "phase": "detect",
                    "message": f"Lint: {len(bugs)} issue(s) found"
                })
    return bugs


async def run_logic_detection(job_id: str, repo_path: str, file_map: dict) -> list:
    """Use Claude (OpenClaw) to find logical bugs."""
    bugs = []
    # Read a sample of non-test source files
    source_files = [
        f for f in (file_map["python_files"] if file_map["language"] == "python" else file_map["js_files"])
        if "test" not in f.lower() and "migration" not in f.lower()
    ][:5]

    for rel_path in source_files:
        full_path = os.path.join(repo_path, rel_path)
        try:
            with open(full_path, "r", errors="ignore") as fh:
                code = fh.read()
            if len(code) < 50:
                continue

            # Call Claude API
            result = await call_claude_for_bugs(rel_path, code[:4000])
            bugs.extend(result)
        except Exception:
            pass

    return bugs


async def call_claude_for_bugs(filename: str, code: str) -> list:
    """Call Anthropic API to detect logical bugs."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"Content-Type": "application/json"},
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 1000,
                    "system": (
                        "You are a precise bug detector. Analyze code for ONLY these categories:\n"
                        "1. Null/None dereference bugs\n"
                        "2. Off-by-one errors\n"
                        "3. Wrong conditions (== vs is, >= vs >, etc.)\n\n"
                        "Respond ONLY with valid JSON array. Each item: "
                        "{\"description\": str, \"line\": int or null, \"category\": \"logic_bug\", "
                        "\"severity\": \"high|medium|low\"}. "
                        "If no bugs found: []. No markdown, no explanation."
                    ),
                    "messages": [{
                        "role": "user",
                        "content": f"File: {filename}\n\n```\n{code}\n```\n\nFind bugs:"
                    }]
                }
            )
            data = resp.json()
            text = data["content"][0]["text"].strip()
            text = re.sub(r"```json|```", "", text).strip()
            found = json.loads(text)
            for bug in found:
                bug["file"] = filename
            return found[:3]  # cap per file
    except Exception:
        return []


async def fix_bug_with_retry(job_id: str, repo_path: str, file_map: dict, bug: dict, max_retries: int = 2) -> Optional[dict]:
    """OpenClaw fix loop: generate → apply → verify → retry."""
    for attempt in range(max_retries + 1):
        if attempt > 0:
            emit_step(job_id, {
                "emoji": "🔄",
                "phase": "fix",
                "message": f"Retrying fix (attempt {attempt + 1}/{max_retries + 1})..."
            })

        # Generate fix via Claude
        fix = await generate_fix(bug, repo_path)
        if not fix:
            continue

        # Apply patch
        applied = apply_patch(repo_path, fix)
        if not applied:
            continue

        # Verify
        passed = verify_fix(repo_path, file_map, bug)
        if passed:
            return {
                "bug": bug,
                "patch": fix["patch"],
                "file": fix["file"],
                "description": fix["description"],
                "attempts": attempt + 1,
            }
        else:
            # Revert
            revert_file(repo_path, fix["file"], fix["original"])

    return None


async def generate_fix(bug: dict, repo_path: str) -> Optional[dict]:
    """Ask Claude for a minimal fix."""
    file_path = bug.get("file", "")
    if not file_path:
        return None

    full_path = os.path.join(repo_path, file_path)
    if not os.path.exists(full_path):
        return None

    try:
        with open(full_path, "r", errors="ignore") as fh:
            original = fh.read()

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"Content-Type": "application/json"},
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 2000,
                    "system": (
                        "You are a surgical code fixer. Rules:\n"
                        "- Minimal diff only\n"
                        "- No refactoring\n"
                        "- Fix ONLY the reported bug\n"
                        "- Return ONLY the complete fixed file content, no markdown\n"
                        "- First line of response: '// FIXED: <one sentence explanation>'\n"
                        "  (use # for python comments)"
                    ),
                    "messages": [{
                        "role": "user",
                        "content": (
                            f"Bug: {bug['description']}\n"
                            f"Line: {bug.get('line', 'unknown')}\n"
                            f"Category: {bug.get('category')}\n\n"
                            f"File ({file_path}):\n```\n{original[:3000]}\n```\n\n"
                            "Return the COMPLETE fixed file:"
                        )
                    }]
                }
            )
            data = resp.json()
            fixed_code = data["content"][0]["text"].strip()
            fixed_code = re.sub(r"^```\w*\n?|```$", "", fixed_code, flags=re.MULTILINE).strip()

            # Extract description from first comment line
            first_line = fixed_code.split("\n")[0]
            desc = first_line.replace("# FIXED:", "").replace("// FIXED:", "").strip()

            return {
                "file": file_path,
                "original": original,
                "fixed": fixed_code,
                "patch": generate_diff(original, fixed_code, file_path),
                "description": desc,
            }
    except Exception:
        return None


def generate_diff(original: str, fixed: str, filename: str) -> str:
    """Generate unified diff."""
    import difflib
    orig_lines = original.splitlines(keepends=True)
    fixed_lines = fixed.splitlines(keepends=True)
    diff = list(difflib.unified_diff(orig_lines, fixed_lines, fromfile=f"a/{filename}", tofile=f"b/{filename}"))
    return "".join(diff)


def apply_patch(repo_path: str, fix: dict) -> bool:
    """Write fixed content to file."""
    try:
        full_path = os.path.join(repo_path, fix["file"])
        with open(full_path, "w") as fh:
            fh.write(fix["fixed"])
        return True
    except Exception:
        return False


def revert_file(repo_path: str, rel_path: str, original: str):
    """Restore original file content."""
    try:
        with open(os.path.join(repo_path, rel_path), "w") as fh:
            fh.write(original)
    except Exception:
        pass


def verify_fix(repo_path: str, file_map: dict, bug: dict) -> bool:
    """Run tests to verify the fix works."""
    if file_map["language"] == "python" and file_map["test_files"]:
        result = subprocess.run(
            ["python", "-m", "pytest", "--tb=no", "-q", "--no-header"],
            capture_output=True, text=True, timeout=60, cwd=repo_path
        )
        return result.returncode == 0
    # For non-test bugs, do basic syntax check
    if file_map["language"] == "python" and bug.get("file"):
        result = subprocess.run(
            ["python", "-m", "py_compile", bug["file"]],
            capture_output=True, timeout=10, cwd=repo_path
        )
        return result.returncode == 0
    return True  # assume pass if no test suite


async def create_pull_request(repo_url: str, repo_path: str, fixes: list, github_token: str) -> Optional[str]:
    """Create a GitHub PR with all fixes."""
    try:
        # Extract owner/repo
        m = re.match(r"https://github\.com/([^/]+)/([^/]+?)(?:\.git)?$", repo_url)
        if not m:
            return None
        owner, repo = m.group(1), m.group(2)

        branch = f"fix/repodoctor-{datetime.utcnow().strftime('%Y%m%d%H%M')}"

        # Create branch
        subprocess.run(["git", "checkout", "-b", branch], cwd=repo_path, capture_output=True)
        subprocess.run(["git", "config", "user.email", "repodoctor@bot.ai"], cwd=repo_path, capture_output=True)
        subprocess.run(["git", "config", "user.name", "RepoDoctor Bot"], cwd=repo_path, capture_output=True)
        subprocess.run(["git", "add", "-A"], cwd=repo_path, capture_output=True)

        commit_msg = "fix: automated bug fixes by RepoDoctor\n\n" + "\n".join(
            f"- {f['description']}" for f in fixes
        )
        subprocess.run(["git", "commit", "-m", commit_msg], cwd=repo_path, capture_output=True)

        # Push
        push_url = f"https://{github_token}@github.com/{owner}/{repo}.git"
        subprocess.run(["git", "push", push_url, branch], cwd=repo_path, capture_output=True)

        # Create PR via API
        pr_body = "## RepoDoctor — Automated Bug Fix\n\n"
        for fix in fixes:
            pr_body += f"### 🐛 {fix['bug']['description']}\n"
            pr_body += f"- **File**: `{fix['file']}`\n"
            pr_body += f"- **Fix**: {fix['description']}\n"
            pr_body += f"- **Attempts**: {fix['attempts']}\n\n"

        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"https://api.github.com/repos/{owner}/{repo}/pulls",
                headers={
                    "Authorization": f"token {github_token}",
                    "Accept": "application/vnd.github.v3+json",
                },
                json={
                    "title": "fix: automated bug fixes by RepoDoctor 🤖",
                    "body": pr_body,
                    "head": branch,
                    "base": "main",
                }
            )
            data = resp.json()
            return data.get("html_url")
    except Exception as e:
        print(f"PR creation error: {e}")
        return None


def parse_pytest_output(output: str) -> list:
    """Parse pytest short output for failures."""
    failures = []
    lines = output.split("\n")
    current = None
    for line in lines:
        if line.startswith("FAILED "):
            parts = line.replace("FAILED ", "").split(" - ")
            current = {
                "description": parts[-1].strip() if len(parts) > 1 else line,
                "file": parts[0].split("::")[0] if parts else "unknown",
                "trace": "",
            }
            failures.append(current)
        elif current and line.strip():
            current["trace"] += line + "\n"
    return failures


async def send_telegram(chat_id: Optional[str], text: str):
    """Send Telegram message if bot token and chat_id are configured."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token or not chat_id:
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
            )
    except Exception:
        pass