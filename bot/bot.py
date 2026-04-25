"""
RepoDoctor — Telegram Bot
Listens for /analyze commands and relays to FastAPI backend.
"""

import os
import asyncio
import httpx
from telegram import Update, BotCommand
from telegram.ext import (
    Application, CommandHandler, ContextTypes, MessageHandler, filters
)

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TOKEN_HERE")


WELCOME = """
🩺 *RepoDoctor* — Autonomous Bug Fixing Agent

I clone your GitHub repo, find bugs, fix them, and open a PR — automatically.

*Commands:*
/analyze `<github-url>` — Start analysis
/status `<job-id>` — Check job status
/help — Show this message

*Example:*
`/analyze https://github.com/user/my-project`

Powered by OpenClaw 🦾
"""


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(WELCOME, parse_mode="Markdown")


async def help_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(WELCOME, parse_mode="Markdown")


async def analyze(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if not args or not args[0].startswith("https://github.com"):
        await update.message.reply_text(
            "❌ Usage: `/analyze https://github.com/user/repo`",
            parse_mode="Markdown"
        )
        return

    repo_url = args[0]
    chat_id = str(update.effective_chat.id)
    github_token = ctx.args[1] if len(ctx.args) > 1 else None

    await update.message.reply_text(
        f"🚀 Starting RepoDoctor on:\n`{repo_url}`\n\nYou'll get live updates here!",
        parse_mode="Markdown"
    )

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{BACKEND_URL}/analyze",
                json={
                    "repo_url": repo_url,
                    "telegram_chat_id": chat_id,
                    "github_token": github_token,
                }
            )
            data = resp.json()
            job_id = data["job_id"]

        await update.message.reply_text(
            f"🔖 Job ID: `{job_id}`\n\nUse /status `{job_id}` to check progress anytime.",
            parse_mode="Markdown"
        )
    except Exception as e:
        await update.message.reply_text(f"💥 Failed to start job: {e}")


async def status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: /status <job-id>")
        return

    job_id = ctx.args[0]
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(f"{BACKEND_URL}/job/{job_id}")
            job = resp.json()

        lines = [f"📋 *Job {job_id}*", f"Status: `{job['status']}`"]
        if job.get("bugs_found"):
            lines.append(f"🐛 Bugs found: {len(job['bugs_found'])}")
        if job.get("fixes_applied"):
            lines.append(f"✅ Fixes applied: {len(job['fixes_applied'])}")
        if job.get("pr_url"):
            lines.append(f"🚀 PR: {job['pr_url']}")
        if job.get("error"):
            lines.append(f"💥 Error: {job['error']}")

        recent_steps = job.get("steps", [])[-3:]
        if recent_steps:
            lines.append("\n*Recent steps:*")
            for s in recent_steps:
                lines.append(f"{s['emoji']} {s['message']}")

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Error fetching job: {e}")


async def unknown(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Unknown command. Use /help for options.")


def main():
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("analyze", analyze))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(MessageHandler(filters.COMMAND, unknown))

    print("🤖 RepoDoctor Bot started...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()