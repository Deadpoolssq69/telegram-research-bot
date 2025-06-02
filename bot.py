#!/usr/bin/env python3
# bot.py

import os
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
#    Replace with your actual group chat ID here:
GROUP_CHAT_ID = -1002637490216

# 3. TIMEOUT FOR WORKER ASSIGNMENTS (minutes)
ASSIGNMENT_TIMEOUT_MINUTES = 30

# 4. LOG FILE DIRECTORY (local directory where you store batch log files)
#    This folder should contain files like:
#      logs/YYYY-MM-DD/batch_1.txt, batch_2.txt, etc.
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
#    assignments[user_id] = (batch_date_str, batch_number, log_line_index, timestamp_assigned)
assignments: dict[int, tuple[str, int, int, datetime.datetime]] = {}

# For each batch (by date), list of workers who processed each log:
#    processed_counts[(date_str, batch_number)][user_id] = count_of_lines_processed
processed_counts: dict[tuple[str, int], defaultdict[int, int]] = defaultdict(lambda: defaultdict(int))

# Tracks when each batch was last updated (for debugging/cleanup):
#    last_active[(date_str, batch_number)] = datetime.datetime
last_active: dict[tuple[str, int], datetime.datetime] = {}

# ─────────────────────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def _get_recent_dates() -> list[str]:
    """
    Return RECENT_DATES_TO_SHOW most recent dates (YYYY-MM-DD) from LOGS_ROOT that exist.
    """
    dates = []
    if not os.path.isdir(LOGS_ROOT):
        return []
    for name in os.listdir(LOGS_ROOT):
        full = os.path.join(LOGS_ROOT, name)
        if os.path.isdir(full):
            try:
                # ensure format is YYYY-MM-DD
                datetime.datetime.strptime(name, "%Y-%m-%d")
                dates.append(name)
            except ValueError:
                continue
    dates.sort(reverse=True)
    return dates[:RECENT_DATES_TO_SHOW]


def _parse_date_input(text: str) -> str | None:
    """
    Try to parse user-entered date (in various formats) into YYYY-MM-DD string.
    Acceptable formats: MM-DD, MM/DD, DD.MM, "Month D", "YYYY-MM-DD".
    """
    text = text.strip()
    now = datetime.datetime.utcnow().date()

    # 1. YYYY-MM-DD
    try:
        dt = datetime.datetime.strptime(text, "%Y-%m-%d").date()
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # 2. MM-DD or MM/DD (assume current year)
    for fmt in ("%m-%d", "%m/%d"):
        try:
            dt = datetime.datetime.strptime(text, fmt).date().replace(year=now.year)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # 3. DD.MM (assume current year)
    try:
        dt = datetime.datetime.strptime(text, "%d.%m").date().replace(year=now.year)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass

    # 4. "Month D" (e.g., "June 2" or "Jun 2")
    try:
        dt = datetime.datetime.strptime(text, "%B %d").date().replace(year=now.year)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    try:
        dt = datetime.datetime.strptime(text, "%b %d").date().replace(year=now.year)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass

    return None


def _list_batches(date_str: str) -> list[int]:
    """
    Given a date (YYYY-MM-DD), return a sorted list of batch numbers available.
    E.g., if LOGS_ROOT/YYYY-MM-DD/batch_1.txt exists, etc.
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
    Read all lines from the given batch file into a list of stripped strings.
    """
    path = os.path.join(LOGS_ROOT, date_str, f"batch_{batch_num}.txt")
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        lines = [line.rstrip("\n") for line in f]
    return lines


def _write_checked_file(date_str: str, batch_num: int, checked_lines: list[str]) -> str:
    """
    Create a temporary .txt file containing the checked lines (with balances appended).
    Return the file path.
    """
    tf = tempfile.NamedTemporaryFile(mode="w+", encoding="utf-8", delete=False, suffix=f"_batch_{batch_num}_checked.txt")
    for line in checked_lines:
        tf.write(line + "\n")
    tf.flush()
    path = tf.name
    tf.close()
    return path


def _cleanup_timed_out_assignments():
    """
    Periodic job that runs every minute to:
      1) Find any worker assignments older than ASSIGNMENT_TIMEOUT_MINUTES and unassign them.
      2) Remove any conversation context (we rely on assignments dict; Telegram messages expire automatically).
    """
    now = datetime.datetime.utcnow()
    to_remove = []
    for user_id, (date_str, batch_num, line_idx, ts_assigned) in assignments.items():
        elapsed = (now - ts_assigned).total_seconds() / 60.0
        if elapsed >= ASSIGNMENT_TIMEOUT_MINUTES:
            to_remove.append(user_id)

    for user_id in to_remove:
        try:
            del assignments[user_id]
            logger.info(f"⏱️ Assignment for user {user_id} expired and removed.")
        except KeyError:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# COMMAND HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /start command for workers. If they have no assignment, send first log.
    If they do, remind them to finish it.
    """
    user_id = update.effective_user.id
    if user_id in assignments:
        await update.message.reply_text(
            "⚠️ You already have a log assigned. Please reply with your balance before requesting another."
        )
        return

    # Assign next available log (simplest: look at today's earliest unchecked).
    today = datetime.datetime.utcnow().date().strftime("%Y-%m-%d")
    batches = _list_batches(today)
    if not batches:
        await update.message.reply_text("✅ No batches available today. Please check back later.")
        return

    # Find next log that is not yet processed by any worker:
    for batch_num in batches:
        lines = _load_batch_file(today, batch_num)
        total_logs = len(lines)

        # Determine how many lines processed so far:
        processed_count = sum(processed_counts[(today, batch_num)].values())
        if processed_count < total_logs:
            # Assign next unprocessed line:
            assigned_line_index = processed_count  # zero-based index
            raw_line = lines[assigned_line_index]

            # Record assignment
            assignments[user_id] = (today, batch_num, assigned_line_index, datetime.datetime.utcnow())

            # Send log to worker
            text = (
                f"🆕 Batch {batch_num} — Log ID: {assigned_line_index + 1}\n\n"
                f"{raw_line}\n\n"
                f"Please reply with the balance (e.g. 12.50 or $12)."
            )
            await update.message.reply_text(text)
            return

    await update.message.reply_text("✅ All logs for today have been assigned. Check again later.")


async def balance_reply_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    When a worker replies with a balance for their assigned log.
    Record it, increment counters, send next log or notify if done.
    """
    user_id = update.effective_user.id
    if user_id not in assignments:
        # No assignment: ignore or ask them to /start
        return

    balance_text = update.message.text.strip()
    # Record worker's count
    date_str, batch_num, line_idx, ts_assigned = assignments[user_id]

    processed_counts[(date_str, batch_num)][user_id] += 1

    # Format and forward to group chat:
    lines = _load_batch_file(date_str, batch_num)
    raw_line = lines[line_idx]
    # Append " | balance = $XX.XX"
    if balance_text.startswith("$"):
        bal = balance_text
    else:
        bal = f"${balance_text}"
    out_line = f"{raw_line} | balance = {bal}"
    try:
        await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=out_line)
    except TelegramError as e:
        logger.error(f"Failed to forward to group: {e}")

    # Clean up this assignment:
    del assignments[user_id]

    # Now send next log if any remain:
    batches = _list_batches(date_str)
    for bnum in batches:
        lines_all = _load_batch_file(date_str, bnum)
        total_logs = len(lines_all)
        processed_count = sum(processed_counts[(date_str, bnum)].values())
        if processed_count < total_logs:
            # Next available log in this batch
            next_idx = processed_count
            raw_next = lines_all[next_idx]
            assignments[user_id] = (date_str, bnum, next_idx, datetime.datetime.utcnow())
            text = (
                f"✅ Balance recorded! Sending your next log...\n\n"
                f"🆕 Batch {bnum} — Log ID: {next_idx + 1}\n\n"
                f"{raw_next}\n\n"
                f"Please reply with the balance (e.g. 12.50 or $12)."
            )
            await update.message.reply_text(text)
            return

    # If we reach here, no more logs today
    await update.message.reply_text("✅ All logs processed for today. Thank you!")


async def queue_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /queue command: Show queue status for today (batches and their progress).
    """
    today = datetime.datetime.utcnow().date().strftime("%Y-%m-%d")
    batches = _list_batches(today)
    if not batches:
        await update.message.reply_text("📋 Queue Status:\n• No batches found for today.")
        return

    message = ["📋 Queue Status:"]
    for bnum in batches:
        lines = _load_batch_file(today, bnum)
        total = len(lines)
        done = sum(processed_counts[(today, bnum)].values())
        pct = (done / total * 100) if total else 0
        status = "✓ Finished" if done >= total else "⏳ In Progress"
        ts = last_active.get((today, bnum))
        date_str = ts.strftime("%H:%M %p") if ts else ""
        message.append(f"• Batch {bnum} {today}: {done}/{total} ({pct:.0f}%) — {status} {date_str}")

    message.append("\n(Use /download to fetch a finished batch file.)")
    await update.message.reply_text("\n".join(message))


async def download_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /download command: Step 1: show recent dates and prompt for date input.
    """
    recent_dates = _get_recent_dates()
    keyboard = []
    for d in recent_dates:
        # Convert YYYY-MM-DD to MM-DD for button label
        btn_label = datetime.datetime.strptime(d, "%Y-%m-%d").strftime("%m-%d")
        keyboard.append([InlineKeyboardButton(btn_label, callback_data=f"DLDATE|{d}")])

    prompt = (
        "Select a date (or type another date in MM/DD, DD.MM, or \"Month D\" format):"
    )
    await update.message.reply_text(
        prompt,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

# ─────────────────────────────────────────────────────────────────────────────
# CALLBACK QUERY HANDLER FOR DOWNLOAD BUTTONS
# ─────────────────────────────────────────────────────────────────────────────

async def download_date_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Handler when user selects a date button (callback_data "DLDATE|YYYY-MM-DD").
    Show batch buttons for that date.
    """
    query = update.callback_query
    await query.answer()
    data = query.data  # "DLDATE|YYYY-MM-DD"
    _, date_str = data.split("|", 1)

    # List batches
    batches = _list_batches(date_str)
    if not batches:
        await query.edit_message_text(f"No batches found for {date_str}.")
        return

    # Create batch buttons
    keyboard = []
    row = []
    for idx, bnum in enumerate(batches, start=1):
        row.append(InlineKeyboardButton(f"Batch {bnum}", callback_data=f"DLBATCH|{date_str}|{bnum}"))
        # Two buttons per row
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
    Handler when user selects a batch button (callback_data "DLBATCH|YYYY-MM-DD|N").
    Generate summary and .txt file, then send both.
    """
    query = update.callback_query
    await query.answer()
    data = query.data  # "DLBATCH|YYYY-MM-DD|N"
    _, date_str, batch_str = data.split("|", 2)
    batch_num = int(batch_str)

    # Load all lines and gather worker stats
    lines = _load_batch_file(date_str, batch_num)
    total = len(lines)
    counts = processed_counts.get((date_str, batch_num), {})
    done = sum(counts.values())

    # Build summary text
    if done == 0:
        summary = "📊 0/{:d} done.\nNo workers processed lines yet.".format(total)
    else:
        lines_stats = [f"📊 {done}/{total} done."]
        for uid, cnt in counts.items():
            try:
                user = await context.bot.get_chat(uid)
                name = user.username or user.full_name or str(uid)
            except TelegramError:
                name = str(uid)
            lines_stats.append(f"{name}: {cnt} replies")
        summary = "\n".join(lines_stats)

    # Now build the checked file lines
    checked_lines = []
    # We assume that the first 'done' lines are exactly those processed, in order.
    for idx in range(done):
        raw_line = lines[idx]
        # We need to know the exact balance for each line; 
        # but in our scheme, the raw_line already had " | balance = $X.XX" appended
        # and forwarded to group at processing time. So we re-load from group history?
        # For simplicity, we re-read from the admin log folder:
        #   logs/YYYY-MM-DD/batch_N_checked.txt if that exists
        checked_folder = os.path.join(LOGS_ROOT, date_str, "checked")
        os.makedirs(checked_folder, exist_ok=True)
        combined_path = os.path.join(checked_folder, f"batch_{batch_num}_checked.txt")
        if os.path.isfile(combined_path):
            # Append all lines from combined_path once, then break
            with open(combined_path, "r", encoding="utf-8") as f:
                checked_lines = [ln.rstrip("\n") for ln in f]
            break

    # If combined_path didn't exist, we fallback: show raw lines without balances
    if not checked_lines:
        for idx in range(done):
            raw_line = lines[idx]
            checked_lines.append(f"{raw_line} | balance = UNKNOWN")

    # Write temporary .txt file
    checked_path = _write_checked_file(date_str, batch_num, checked_lines)

    # Send summary text + file
    try:
        await query.edit_message_text(summary)
    except TelegramError:
        pass

    try:
        with open(checked_path, "rb") as f:
            await context.bot.send_document(
                chat_id=query.message.chat_id,
                document=InputFile(f, filename=f"batch_{batch_num}_checked.txt"),
                caption=None,
            )
    except TelegramError as e:
        logger.error(f"Failed to send batch file: {e}")

    # Cleanup temporary file after a short delay
    await asyncio.sleep(1)
    try:
        os.remove(checked_path)
    except OSError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# MESSAGE HANDLER: MANUAL DATE INPUT (for /download)
# ─────────────────────────────────────────────────────────────────────────────

async def manual_date_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    After /download, if user types a date instead of pressing a button, parse it.
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
    for idx, bnum in enumerate(batches, start=1):
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


# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND TASK SETUP
# ─────────────────────────────────────────────────────────────────────────────

async def cleanup_job(app: Application) -> None:
    """
    Periodic background task to call _cleanup_timed_out_assignments every minute.
    """
    while True:
        try:
            _cleanup_timed_out_assignments()
        except Exception as e:
            logger.error(f"Error in cleanup_job: {e}")
        await asyncio.sleep(60)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN FUNCTION
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    """Run the bot."""
    app = Application.builder().token(BOT_TOKEN).build()

    # Command handlers
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CommandHandler("queue", queue_handler))
    app.add_handler(CommandHandler("download", download_handler))

    # CallbackQuery handlers for /download flow
    app.add_handler(CallbackQueryHandler(download_date_callback, pattern=r"^DLDATE\|"))
    app.add_handler(CallbackQueryHandler(download_batch_callback, pattern=r"^DLBATCH\|"))

    # Manual date text handler (only when /download is active)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, manual_date_input_handler))

    # Balance replies from workers
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, balance_reply_handler))

    # Start background cleanup job
    app.post_init(lambda _: asyncio.create_task(cleanup_job(app)))

    logger.info("Bot is starting...")
    app.run_polling()  # or .run_webhook() if you prefer webhooks


if __name__ == "__main__":
    main()
