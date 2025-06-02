#!/usr/bin/env python3
# bot.py

import os
import json
import logging
import tempfile
import datetime
import asyncio
from collections import defaultdict

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from telegram.error import TelegramError

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

# 1. BOT TOKEN (set as environment variable)
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE" or not BOT_TOKEN:
    raise RuntimeError("Error: BOT_TOKEN not set. Exiting.")

# 2. GROUP CHAT ID (must be a negative number for supergroups)
GROUP_CHAT_ID = -1002637490216

# 3. TIMEOUT FOR WORKER ASSIGNMENTS (minutes)
ASSIGNMENT_TIMEOUT_MINUTES = 30

# 4. LOG FILE DIRECTORY (local directory where you store batch log files)
#    This folder should contain subfolders named YYYY-MM-DD, each with batch_N.txt files.
LOGS_ROOT = "./logs"

# 5. VALID DATE BUTTONS (number of recent dates to show automatically)
RECENT_DATES_TO_SHOW = 4

# ─────────────────────────────────────────────────────────────────────────────
# SET UP LOGGING (for debugging)
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL STATE
# ─────────────────────────────────────────────────────────────────────────────

# Tracks which worker (user_id) has which log currently assigned:
#    assignments[user_id] = (date_str, batch_number, line_index, timestamp_assigned)
assignments: dict[int, tuple[str, int, int, datetime.datetime]] = {}

# For each batch (by date), how many lines each worker processed:
#    processed_counts[(date_str, batch_number)][user_id] = count_of_lines_processed
processed_counts: dict[tuple[str, int], defaultdict[int, int]] = defaultdict(lambda: defaultdict(int))

# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def _get_recent_dates() -> list[str]:
    """
    Return up to RECENT_DATES_TO_SHOW most recent YYYY-MM-DD folders in LOGS_ROOT.
    """
    dates = []
    if not os.path.isdir(LOGS_ROOT):
        return []
    for name in os.listdir(LOGS_ROOT):
        full = os.path.join(LOGS_ROOT, name)
        if os.path.isdir(full):
            try:
                datetime.datetime.strptime(name, "%Y-%m-%d")
                dates.append(name)
            except ValueError:
                continue
    dates.sort(reverse=True)
    return dates[:RECENT_DATES_TO_SHOW]


def _parse_date_input(text: str) -> str | None:
    """
    Try to parse user-entered date into YYYY-MM-DD.
    Acceptable: YYYY-MM-DD, MM-DD, MM/DD, DD.MM, 'Month D', 'Mon D'.
    """
    text = text.strip()
    today = datetime.datetime.utcnow().date()

    # 1) YYYY-MM-DD
    try:
        d = datetime.datetime.strptime(text, "%Y-%m-%d").date()
        return d.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # 2) MM-DD or MM/DD (assume current year)
    for fmt in ("%m-%d", "%m/%d"):
        try:
            d = datetime.datetime.strptime(text, fmt).date().replace(year=today.year)
            return d.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # 3) DD.MM (assume current year)
    try:
        d = datetime.datetime.strptime(text, "%d.%m").date().replace(year=today.year)
        return d.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # 4) Full month name or short month name + day, e.g. "June 2" or "Jun 2"
    for fmt in ("%B %d", "%b %d"):
        try:
            d = datetime.datetime.strptime(text, fmt).date().replace(year=today.year)
            return d.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return None


def _list_batches(date_str: str) -> list[int]:
    """
    Given YYYY-MM-DD, return sorted list of batch numbers (existing batch_N.txt).
    """
    folder = os.path.join(LOGS_ROOT, date_str)
    if not os.path.isdir(folder):
        return []
    batches = []
    for fname in os.listdir(folder):
        if fname.startswith("batch_") and fname.endswith(".txt"):
            try:
                num = int(fname.split("_")[1].split(".")[0])
                batches.append(num)
            except (IndexError, ValueError):
                continue
    batches.sort()
    return batches


def _load_batch_file(date_str: str, batch_num: int) -> list[str]:
    """
    Return list of lines (no newline) from logs/YYYY-MM-DD/batch_N.txt.
    """
    path = os.path.join(LOGS_ROOT, date_str, f"batch_{batch_num}.txt")
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return [line.rstrip("\n") for line in f]


def _write_checked_file(date_str: str, batch_num: int, checked_lines: list[str]) -> str:
    """
    Dump `checked_lines` into a temporary .txt file. Return that file path.
    """
    tmp = tempfile.NamedTemporaryFile(
        mode="w+", encoding="utf-8", delete=False, suffix=f"_batch_{batch_num}_checked.txt"
    )
    for line in checked_lines:
        tmp.write(line + "\n")
    tmp.flush()
    path = tmp.name
    tmp.close()
    return path


def _cleanup_timed_out_assignments(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Called every minute by job_queue. Remove any worker assignments older than
    ASSIGNMENT_TIMEOUT_MINUTES so they can request a new log after timeout.
    """
    now = datetime.datetime.utcnow()
    to_remove = []
    for user_id, (date_str, batch_num, idx, ts_assigned) in assignments.items():
        elapsed_minutes = (now - ts_assigned).total_seconds() / 60.0
        if elapsed_minutes >= ASSIGNMENT_TIMEOUT_MINUTES:
            to_remove.append(user_id)

    for user_id in to_remove:
        del assignments[user_id]
        logger.info(f"🕒 Assignment for user {user_id} timed out and was removed.")


# ─────────────────────────────────────────────────────────────────────────────
# COMMAND HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /start: Assign the next unprocessed log to worker or remind them if already assigned.
    """
    user_id = update.effective_user.id

    # If worker already has an assignment, ask them to finish it:
    if user_id in assignments:
        await update.message.reply_text(
            "⚠️ You already have a log assigned. Please reply with your balance before requesting another."
        )
        return

    # Otherwise, find next available log (today first)
    today = datetime.datetime.utcnow().date().strftime("%Y-%m-%d")
    batches = _list_batches(today)
    if not batches:
        await update.message.reply_text("✅ No batches available today. Please try again later.")
        return

    for bnum in batches:
        lines = _load_batch_file(today, bnum)
        total = len(lines)
        done = sum(processed_counts[(today, bnum)].values())
        if done < total:
            idx = done  # zero-based index = next unprocessed line
            raw_line = lines[idx]
            # Record assignment
            assignments[user_id] = (today, bnum, idx, datetime.datetime.utcnow())
            # Send to worker
            text = (
                f"🆕 Batch {bnum} — Log ID: {idx + 1}\n\n"
                f"{raw_line}\n\n"
                f"Please reply with the balance (e.g. 12.50 or $12)."
            )
            await update.message.reply_text(text)
            return

    await update.message.reply_text("✅ All logs for today have been assigned. Please check back later.")


async def balance_reply_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    When a worker replies with a balance, record it, forward to group, then send next log.
    """
    user_id = update.effective_user.id
    if user_id not in assignments:
        return  # no assignment, ignore

    balance_text = update.message.text.strip()
    date_str, bnum, idx, ts_assigned = assignments[user_id]

    # Update worker's processed count
    processed_counts[(date_str, bnum)][user_id] += 1

    # Build forwarded line (append " | balance = $XX")
    lines = _load_batch_file(date_str, bnum)
    raw_line = lines[idx]
    if balance_text.startswith("$"):
        bal = balance_text
    else:
        bal = f"${balance_text}"
    forwarded = f"{raw_line} | balance = {bal}"
    try:
        await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=forwarded)
    except TelegramError as e:
        logger.error(f"Failed to forward to group: {e}")

    # Remove this assignment
    del assignments[user_id]

    # Send next log if any remain:
    batches = _list_batches(date_str)
    for next_bnum in batches:
        next_lines = _load_batch_file(date_str, next_bnum)
        total_next = len(next_lines)
        done_next = sum(processed_counts[(date_str, next_bnum)].values())
        if done_next < total_next:
            next_idx = done_next
            raw_next = next_lines[next_idx]
            assignments[user_id] = (date_str, next_bnum, next_idx, datetime.datetime.utcnow())
            text = (
                "✅ Balance recorded! Sending your next log...\n\n"
                f"🆕 Batch {next_bnum} — Log ID: {next_idx + 1}\n\n"
                f"{raw_next}\n\n"
                "Please reply with the balance (e.g. 12.50 or $12)."
            )
            await update.message.reply_text(text)
            return

    # If no more logs:
    await update.message.reply_text("✅ All logs processed for today. Thank you!")


async def queue_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /queue: Show today’s queue status (batches and progress).
    """
    today = datetime.datetime.utcnow().date().strftime("%Y-%m-%d")
    batches = _list_batches(today)
    if not batches:
        await update.message.reply_text("📋 Queue Status:\n• No batches found for today.")
        return

    lines = ["📋 Queue Status:"]
    for bnum in batches:
        all_lines = _load_batch_file(today, bnum)
        total = len(all_lines)
        done = sum(processed_counts[(today, bnum)].values())
        pct = (done / total * 100) if total else 0
        status = "✓ Finished" if done >= total else "⏳ In Progress"
        lines.append(f"• Batch {bnum} {today}: {done}/{total} ({pct:.0f}%) — {status}")

    lines.append("\n(Use /download to fetch a finished batch file.)")
    await update.message.reply_text("\n".join(lines))


async def download_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /download: Show up to RECENT_DATES_TO_SHOW recent dates as buttons, plus instructions.
    """
    recent_dates = _get_recent_dates()
    keyboard = []
    for d in recent_dates:
        btn_label = datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%m-%d")
        keyboard.append([InlineKeyboardButton(btn_label, callback_data=f"DLDATE|{d}")])

    prompt = "Select a date (or type another date in MM/DD, DD.MM, or \"Month D\" format):"
    await update.message.reply_text(
        prompt,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def download_date_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    When user taps a date button: show all batch-buttons for that date.
    """
    query = update.callback_query
    await query.answer()
    data = query.data  # "DLDATE|YYYY-MM-DD"
    _, date_str = data.split("|", 1)

    batches = _list_batches(date_str)
    if not batches:
        await query.edit_message_text(f"No batches found for {date_str}.")
        return

    keyboard = []
    row = []
    for bnum in batches:
        row.append(InlineKeyboardButton(f"Batch {bnum}", callback_data=f"DLBATCH|{date_str}|{bnum}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    await query.edit_message_text(
        text=f"Select a batch to download from {date_str}:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def download_batch_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    When user taps a batch button: send a summary text + a single .txt file containing all checked lines.
    """
    query = update.callback_query
    await query.answer()
    data = query.data  # "DLBATCH|YYYY-MM-DD|N"
    _, date_str, batch_str = data.split("|", 2)
    bnum = int(batch_str)

    # Load original lines
    all_lines = _load_batch_file(date_str, bnum)
    total = len(all_lines)
    counts = processed_counts.get((date_str, bnum), {})
    done = sum(counts.values())

    # Build summary text
    if done == 0:
        summary = f"📊 0/{total} done.\nNo workers processed lines yet."
    else:
        parts = [f"📊 {done}/{total} done."]
        for uid, cnt in counts.items():
            try:
                user_obj = await context.bot.get_chat(uid)
                name = user_obj.username or user_obj.full_name or str(uid)
            except TelegramError:
                name = str(uid)
            parts.append(f"{name}: {cnt} replies")
        summary = "\n".join(parts)

    # Attempt to find a "checked" file on disk
    checked_folder = os.path.join(LOGS_ROOT, date_str, "checked")
    combined_path = os.path.join(checked_folder, f"batch_{bnum}_checked.txt")
    checked_lines = []
    if os.path.isfile(combined_path):
        with open(combined_path, "r", encoding="utf-8") as rf:
            checked_lines = [ln.rstrip("\n") for ln in rf]
    else:
        # Fallback: prepend the first `done` lines and append UNKNOWN balance
        for idx in range(done):
            raw_line = all_lines[idx]
            checked_lines.append(f"{raw_line} | balance = UNKNOWN")

    # Write our temp .txt
    temp_path = _write_checked_file(date_str, bnum, checked_lines)

    # Edit summary
    try:
        await query.edit_message_text(summary)
    except TelegramError:
        pass

    # Send the .txt as a document
    try:
        with open(temp_path, "rb") as f:
            await context.bot.send_document(
                chat_id=query.message.chat_id,
                document=InputFile(f, filename=f"batch_{bnum}_checked.txt"),
            )
    except TelegramError as e:
        logger.error(f"Failed to send batch file: {e}")

    # Cleanup temporary file
    await asyncio.sleep(1)
    try:
        os.remove(temp_path)
    except OSError:
        pass


async def manual_date_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    If user types a date (instead of tapping a button), parse it and show batch buttons.
    """
    text = update.message.text.strip()
    parsed = _parse_date_input(text)
    if not parsed:
        await update.message.reply_text(
            "❌ Invalid date format. Please send in MM/DD, DD.MM, Month D, or YYYY-MM-DD format."
        )
        return

    date_str = parsed
    batches = _list_batches(date_str)
    if not batches:
        await update.message.reply_text(f"No batches found for {date_str}.")
        return

    keyboard = []
    row = []
    for bnum in batches:
        row.append(InlineKeyboardButton(f"Batch {bnum}", callback_data=f"DLBATCH|{date_str}|{bnum}"))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    await update.message.reply_text(
        text=f"Select a batch to download from {date_str}:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def add_user_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin-only command: /add_user <telegram_id> <worker|admin>
    Adds or updates a user’s role in data/users.json.
    """
    if not update.effective_user:
        return

    # Load existing users from data/users.json (or start fresh if missing/corrupt)
    users_path = os.path.join(" data", "users.json")
    try:
        with open(users_path, "r", encoding="utf-8") as f:
            users = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        users = {}

    # Only an existing admin can add another user
    requester_id = str(update.effective_user.id)
    if requester_id not in users or users[requester_id] != "admin":
        await update.message.reply_text("❌ You must be an admin to use /add_user.")
        return

    # Validate the arguments (we expect exactly two: <telegram_id> <worker|admin>)
    if len(context.args) != 2:
        await update.message.reply_text("Usage: /add_user <telegram_id> <worker|admin>")
        return

    new_user_id = context.args[0]
    new_role = context.args[1].lower()
    if new_role not in ("worker", "admin"):
        await update.message.reply_text("Role must be either 'worker' or 'admin'.")
        return

    # Update the users dict and write it back to data/users.json
    users[new_user_id] = new_role
    os.makedirs(os.path.dirname(users_path), exist_ok=True)
    with open(users_path, "w", encoding="utf-8") as f:
        json.dump(users, f, indent=2)

    await update.message.reply_text(f"✅ Added user {new_user_id} as {new_role}.")


def main() -> None:
    """Run the bot."""
    app = Application.builder().token(BOT_TOKEN).build()

    # Register command handlers
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("queue", queue_handler))
    app.add_handler(CommandHandler("download", download_handler))
    app.add_handler(CommandHandler("add_user", add_user_handler))

    # CallbackQuery handlers for download flow
    app.add_handler(CallbackQueryHandler(download_date_callback, pattern=r"^DLDATE\|"))
    app.add_handler(CallbackQueryHandler(download_batch_callback, pattern=r"^DLBATCH\|"))

    # Manual date text handler
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND, manual_date_input_handler
        )
    )

    # Balance replies from workers
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND, balance_reply_handler
        )
    )

    # Schedule cleanup job every 60 seconds
    app.job_queue.run_repeating(
        _cleanup_timed_out_assignments,
        interval=60,
        first=60,
    )

    logger.info("Bot is starting...")
    app.run_polling()


if __name__ == "__main__":
    main()
