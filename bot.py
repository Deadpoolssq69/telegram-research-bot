# bot.py

import os
import re
import logging
import tempfile
import psycopg2
import zipfile
from datetime import datetime, timedelta
import pytz
import asyncio

from telegram import (
    Update,
    InputFile,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ------------------------
# 1) BASIC LOGGING
# ------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ------------------------
# 2) READ ENVIRONMENT VARIABLES
# ------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    logger.error("Error: BOT_TOKEN not set. Exiting.")
    exit(1)

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    logger.error("Error: DATABASE_URL not set. Exiting.")
    exit(1)

# Group chat ID where logs are posted
GROUP_CHAT_ID = -4828152242
# Username to ping if balance > 100
HIGH_BALANCE_PING = "@manipulation"

# Timezone (Asia/Kolkata)
TIMEZONE = "Asia/Kolkata"
tz = pytz.timezone(TIMEZONE)

# ------------------------
# 3) REGEXES FOR CLEANING & BALANCE
# ------------------------
PHONE_RE   = re.compile(r"Phone Number:\s*([0-9\-\s]+)")
CARD_RE    = re.compile(r"Loyalty Card:\s*(\d{16})")
DIGITS_RE  = re.compile(r"[^\d]")  # strips non-digits
BALANCE_RE = re.compile(r"[-+]?\d*\.?\d+")  # match integer or decimal

# ------------------------
# 4) IN-MEMORY TRACKERS
# ------------------------
pending_assignment = {}      # worker_id → (log_id, message_id_to_delete, batch_id)
download_date_prompt = {}    # admin_id → True  (waiting for text‐date)
download_batch_prompt = {}   # admin_id → [list_of_batch_ids]

# ------------------------
# 5) DATABASE HELPERS
# ------------------------

def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    # users table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            role VARCHAR(10) NOT NULL CHECK(role IN ('worker','admin')),
            name TEXT,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    # batches table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            total_lines INTEGER NOT NULL,
            finished_at TIMESTAMP WITH TIME ZONE
        );
    """)

    # unchecked_logs table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS unchecked_logs (
            id SERIAL PRIMARY KEY,
            batch_id INTEGER REFERENCES batches(id),
            log_text TEXT NOT NULL,
            assigned_to BIGINT REFERENCES users(telegram_id),
            assigned_at TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    # checked_logs table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS checked_logs (
            id SERIAL PRIMARY KEY,
            batch_id INTEGER NOT NULL,
            orig_log_id INTEGER REFERENCES unchecked_logs(id) ON DELETE CASCADE,
            log_text TEXT NOT NULL,
            worker_id BIGINT REFERENCES users(telegram_id),
            worker_username TEXT,
            result_text TEXT NOT NULL,
            checked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    # pending_deletions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_deletions (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            message_id INTEGER NOT NULL,
            delete_at TIMESTAMP WITH TIME ZONE NOT NULL
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_pending_deletions_delete_at
        ON pending_deletions(delete_at);
    """)

    cur.close()
    conn.close()

# ------------------------
# 6) CLEANING FUNCTION
# ------------------------

def clean_lines(raw_lines):
    """
    Takes raw lines → returns (good_cleaned_lines, bad_lines_with_notes).
    """
    good = []
    bad = []
    for ln in raw_lines:
        m_phone = PHONE_RE.search(ln)
        m_card  = CARD_RE.search(ln)
        if not (m_phone and m_card):
            bad.append(ln + "  <-- missing phone or card")
            continue
        digits = DIGITS_RE.sub("", m_phone.group(1))
        if len(digits) != 10:
            bad.append(ln + "  <-- phone not 10 digits")
            continue
        cleaned = PHONE_RE.sub(f"Phone Number: {digits}", ln)
        good.append(cleaned)
    return good, bad

# ------------------------
# 7) AUTH HELPERS
# ------------------------

def is_user_authorized(telegram_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT role FROM users WHERE telegram_id = %s;", (telegram_id,))
    res = cur.fetchone()
    cur.close()
    conn.close()
    if res:
        return True, res[0]
    return False, None

# ------------------------
# 8) BATCH HELPERS
# ------------------------

def create_new_batch(total_lines):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("INSERT INTO batches(total_lines) VALUES (%s) RETURNING id;", (total_lines,))
    batch_id = cur.fetchone()[0]
    cur.close()
    conn.close()
    return batch_id

def mark_batch_finished_if_complete(batch_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM unchecked_logs WHERE batch_id = %s;", (batch_id,))
    remaining = cur.fetchone()[0]
    if remaining == 0:
        cur.execute("""
            UPDATE batches
            SET finished_at = NOW() AT TIME ZONE %s
            WHERE id = %s AND finished_at IS NULL;
        """, (TIMEZONE, batch_id))
    conn.commit()
    cur.close()
    conn.close()

# ------------------------
# 9) MESSAGE DELETION HELPERS
# ------------------------

def schedule_message_deletion(chat_id, message_id, delete_at):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO pending_deletions(chat_id, message_id, delete_at)
        VALUES (%s, %s, %s);
    """, (chat_id, message_id, delete_at))
    conn.commit()
    cur.close()
    conn.close()

async def process_pending_deletions(context: ContextTypes.DEFAULT_TYPE):
    """
    Every minute: delete any Telegram messages whose delete_at ≤ now.
    """
    now = datetime.now(tz)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, chat_id, message_id
        FROM pending_deletions
        WHERE delete_at <= %s;
    """, (now,))
    rows = cur.fetchall()
    for row in rows:
        pd_id, chat_id, message_id = row
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception as e:
            logger.warning(f"Failed to delete message {message_id} in chat {chat_id}: {e}")
        cur.execute("DELETE FROM pending_deletions WHERE id = %s;", (pd_id,))
    conn.commit()
    cur.close()
    conn.close()

# ------------------------
# 10) COMMAND HANDLERS
# ------------------------

async def start_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /start or /next → assign one available log to a worker.
    """
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "worker":
        await update.message.reply_text("❌ You are not authorized to get a log. Ask an admin to add you.")
        return

    if user_id in pending_assignment:
        await update.message.reply_text(
            "⚠️ You already have a log assigned. Please reply with your result before requesting another."
        )
        return

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"""
        WITH candidate AS (
            SELECT id, log_text, batch_id
            FROM unchecked_logs
            WHERE
              (assigned_to IS NULL
               OR assigned_at < (NOW() AT TIME ZONE '{TIMEZONE}') - INTERVAL '30 minutes')
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE unchecked_logs
        SET assigned_to = %s,
            assigned_at = NOW() AT TIME ZONE %s
        FROM candidate
        WHERE unchecked_logs.id = candidate.id
        RETURNING unchecked_logs.id, unchecked_logs.log_text, unchecked_logs.batch_id;
    """, (user_id, TIMEZONE))

    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        await update.message.reply_text("🗒️ No logs available right now. Try again later.")
        return

    log_id, log_text, batch_id = row
    sent_msg = await update.message.reply_text(
        f"🆕 *Batch {batch_id}* — Log ID: `{log_id}`\n\n"
        f"{log_text}\n\n"
        "Please reply with the balance (e.g. `12.50` or `$12`).",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("Next log ⏭️")]],
            one_time_keyboard=False,
            resize_keyboard=True
        )
    )

    # Schedule deletion of this “assignment” message in 4 hours
    delete_at = datetime.now(tz) + timedelta(hours=4)
    schedule_message_deletion(update.effective_chat.id, sent_msg.message_id, delete_at)

    pending_assignment[user_id] = (log_id, sent_msg.message_id, batch_id)

async def handle_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle a worker’s plain‐text reply (their “balance”).
    Moves the log to checked_logs, posts to the group with balance, pings if >100.
    """
    user_id = update.effective_user.id
    text = update.message.text.strip()
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "worker":
        return

    if user_id not in pending_assignment:
        return

    log_id, msg_to_delete, batch_id = pending_assignment[user_id]

    # Fetch original log_text
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT log_text FROM unchecked_logs WHERE id = %s;", (log_id,))
    res = cur.fetchone()
    if not res:
        # Already processed or expired
        del pending_assignment[user_id]
        await update.message.reply_text(
            "⚠️ That log was already processed or expired. Try /next again."
        )
        cur.close()
        conn.close()
        return

    orig_text = res[0]

    # Attempt to parse a numeric balance from the worker’s reply
    m_balance = BALANCE_RE.search(text)
    balance_amount = None
    if m_balance:
        try:
            balance_amount = float(m_balance.group())
        except:
            balance_amount = None

    # Insert into checked_logs (including worker_username)
    user_obj = await context.bot.get_chat(user_id)
    worker_username = user_obj.username or str(user_id)

    cur.execute("""
        INSERT INTO checked_logs(batch_id, orig_log_id, log_text, worker_id, worker_username, result_text, checked_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW() AT TIME ZONE %s);
    """, (batch_id, log_id, orig_text, user_id, worker_username, text, TIMEZONE))

    # Delete from unchecked_logs
    cur.execute("DELETE FROM unchecked_logs WHERE id = %s;", (log_id,))
    conn.commit()

    # Mark batch finished if needed
    mark_batch_finished_if_complete(batch_id)

    cur.close()
    conn.close()

    del pending_assignment[user_id]
    await update.message.reply_text("✅ Got it! Your result has been saved. Thank you.")

    # Now, post to the group chat with balance
    if balance_amount is not None:
        balance_text = f"{orig_text} | balance = ${balance_amount:.2f}"
        await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=balance_text)

        if balance_amount > 100:
            celebratory = (
                f"{HIGH_BALANCE_PING} 🎉 Whoa! This log has a HIGH balance of ${balance_amount:.2f}! 🎉"
            )
            await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=celebratory)

async def add_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /add_user <telegram_id_or_@username> <worker|admin>
    Only admins may run.
    """
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to add users.")
        return

    args = context.args
    if len(args) != 2:
        await update.message.reply_text("Usage: /add_user <telegram_id_or_@username> <worker|admin>")
        return

    target, new_role = args
    new_role = new_role.lower()
    if new_role not in ("worker", "admin"):
        await update.message.reply_text("Role must be either 'worker' or 'admin'.")
        return

    # Resolve username if needed
    if target.startswith("@"):
        try:
            user_obj = await context.bot.get_chat(target)
            target_id = user_obj.id
        except Exception as e:
            await update.message.reply_text(f"❌ Could not find user {target}. {e}")
            return
    else:
        try:
            target_id = int(target)
        except ValueError:
            await update.message.reply_text("Invalid telegram_id or username.")
            return

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO users(telegram_id, role)
            VALUES (%s, %s)
            ON CONFLICT (telegram_id) DO UPDATE SET role = EXCLUDED.role;
        """, (target_id, new_role))
        conn.commit()
        await update.message.reply_text(f"✅ Added/Updated user `{target_id}` as `{new_role}`.", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Database error: {e}")
    finally:
        cur.close()
        conn.close()

async def list_workers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /list_workers
    Only admins. Lists all users with roles and creation time.
    """
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to list users.")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT telegram_id, role, created_at FROM users ORDER BY created_at;")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        await update.message.reply_text("No users found.")
        return

    msg = "*Registered Users:*\n"
    for tg_id, r, created in rows:
        local_ts = created.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
        msg += f"• `{tg_id}`: {r} (added {local_ts})\n"

    await update.message.reply_text(msg, parse_mode="Markdown")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /stats
    Only admins. Shows remaining logs, checked today/week/month, per-user totals.
    """
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to view stats.")
        return

    conn = get_db_connection()
    cur = conn.cursor()

    # Remaining logs
    cur.execute(f"""
        SELECT COUNT(*) FROM unchecked_logs
        WHERE assigned_to IS NULL
          OR assigned_at < (NOW() AT TIME ZONE '{TIMEZONE}') - INTERVAL '30 minutes';
    """)
    remaining = cur.fetchone()[0]

    # Checked Today
    cur.execute(f"""
        SELECT COUNT(*) FROM checked_logs
        WHERE checked_at >= date_trunc('day', NOW() AT TIME ZONE '{TIMEZONE}')
          AND checked_at < date_trunc('day', NOW() AT TIME ZONE '{TIMEZONE}') + INTERVAL '1 day';
    """)
    checked_today = cur.fetchone()[0]

    # Checked This Week
    cur.execute(f"""
        SELECT COUNT(*) FROM checked_logs
        WHERE checked_at >= date_trunc('week', NOW() AT TIME ZONE '{TIMEZONE}')
          AND checked_at < date_trunc('week', NOW() AT TIME ZONE '{TIMEZONE}') + INTERVAL '1 week';
    """)
    checked_week = cur.fetchone()[0]

    # Checked This Month
    cur.execute(f"""
        SELECT COUNT(*) FROM checked_logs
        WHERE checked_at >= date_trunc('month', NOW() AT TIME ZONE '{TIMEZONE}')
          AND checked_at < date_trunc('month', NOW() AT TIME ZONE '{TIMEZONE}') + INTERVAL '1 month';
    """)
    checked_month = cur.fetchone()[0]

    # Per-user totals (all time)
    cur.execute("""
        SELECT worker_username, COUNT(*) AS total
        FROM checked_logs
        GROUP BY worker_username
        ORDER BY total DESC;
    """)
    per_worker = cur.fetchall()

    cur.close()
    conn.close()

    msg = "*📊 Overall Metrics:*\n"
    msg += f"• Remaining logs: `{remaining}`\n"
    msg += f"• Checked Today: `{checked_today}`\n"
    msg += f"• Checked This Week: `{checked_week}`\n"
    msg += f"• Checked This Month: `{checked_month}`\n\n"
    msg += "*👥 Per-Worker Totals (All Time):*\n"
    if per_worker:
        for wk, total in per_worker:
            msg += f"• `{wk}`: `{total}`\n"
    else:
        msg += "No completed logs yet.\n"

    await update.message.reply_text(msg, parse_mode="Markdown")

async def batch_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /batch_stats <batch_id>
    Only admins. Shows detailed stats for that batch.
    """
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to view batch stats.")
        return

    args = context.args
    if len(args) != 1:
        await update.message.reply_text("Usage: /batch_stats <batch_id>")
        return

    try:
        batch_id = int(args[0])
    except ValueError:
        await update.message.reply_text("Invalid batch_id. It must be a number.")
        return

    conn = get_db_connection()
    cur = conn.cursor()

    # Fetch batch info
    cur.execute("""
        SELECT total_lines, created_at, finished_at
        FROM batches
        WHERE id = %s;
    """, (batch_id,))
    row = cur.fetchone()
    if not row:
        await update.message.reply_text(f"Batch `{batch_id}` not found.", parse_mode="Markdown")
        cur.close()
        conn.close()
        return

    total_lines, created_at, finished_at = row
    created_local = created_at.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
    finished_local = (
        finished_at.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
        if finished_at else "In Progress"
    )

    # Count remaining logs
    cur.execute("SELECT COUNT(*) FROM unchecked_logs WHERE batch_id = %s;", (batch_id,))
    remaining = cur.fetchone()[0]
    processed = total_lines - remaining

    # Per-worker for this batch
    cur.execute("""
        SELECT worker_username, COUNT(*) AS cnt
        FROM checked_logs
        WHERE batch_id = %s
        GROUP BY worker_username
        ORDER BY cnt DESC;
    """, (batch_id,))
    per_user = cur.fetchall()

    cur.close()
    conn.close()

    msg = f"*Batch `{batch_id}` Stats:*\n"
    msg += f"• Total lines: `{total_lines}`\n"
    msg += f"• Processed: `{processed}`\n"
    msg += f"• Remaining: `{remaining}`\n"
    msg += f"• Started at: {created_local}\n"
    msg += f"• Finished at: {finished_local}\n\n"
    msg += "*👥 Per-Worker in this batch:*\n"
    if per_user:
        for wk, cnt in per_user:
            msg += f"• `{wk}`: `{cnt}`\n"
    else:
        msg += "No one has processed any lines yet.\n"

    await update.message.reply_text(msg, parse_mode="Markdown")

async def queue_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /queue
    Only admins. Shows all batches with (processed/total, percent, status).
    """
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to view queue.")
        return

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT id, total_lines, created_at, finished_at FROM batches ORDER BY created_at;")
    batches = cur.fetchall()

    if not batches:
        await update.message.reply_text("No batches found.")
        cur.close()
        conn.close()
        return

    msg = "*📋 Queue Status:*\n"
    for bid, total, created_at, finished_at in batches:
        created_local = created_at.astimezone(tz).strftime("%Y-%m-%d")
        cur.execute("SELECT COUNT(*) FROM checked_logs WHERE batch_id = %s;", (bid,))
        proc = cur.fetchone()[0]
        perc = (proc / total) * 100 if total else 0
        status = (
            f"✓ Finished (on {finished_at.astimezone(tz).strftime('%Y-%m-%d')})"
            if finished_at else "⏳ In Progress"
        )
        msg += f"• Batch `{bid}` [{created_local}]: {proc}/{total} ({perc:.1f}%) — {status}\n"

    cur.close()
    conn.close()
    await update.message.reply_text(msg, parse_mode="Markdown")

# ------------------------
# 11) DOWNLOAD MENU HELPERS
# ------------------------

def format_date_label(dt_obj):
    """
    Given a datetime, return “MM-DD” (e.g. “06-02”).
    """
    return dt_obj.strftime("%m-%d")

async def download_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /download
    First step: show dates or prompt for a date if >10.
    """
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to download batches.")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    # Fetch distinct dates (YYYY-MM-DD) from batches
    cur.execute(f"""
        SELECT DISTINCT DATE(created_at AT TIME ZONE %s)
        FROM batches
        ORDER BY 1 DESC;
    """, (TIMEZONE,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        await update.message.reply_text("No batches have been created yet.")
        return

    dates = [r[0] for r in rows]  # list of date objects
    if len(dates) <= 10:
        # Show as buttons (label = “MM-DD”, data = “date|YYYY-MM-DD”)
        buttons = []
        for dt in dates:
            lbl = format_date_label(dt)
            data = f"date|{dt.strftime('%Y-%m-%d')}"
            buttons.append(InlineKeyboardButton(lbl, callback_data=data))
        # Arrange 2 columns per row
        keyboard = []
        for i in range(0, len(buttons), 2):
            keyboard.append(buttons[i:i+2])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "Select a date:",
            reply_markup=reply_markup
        )
    else:
        # Too many dates → ask admin to type it manually
        await update.message.reply_text(
            "There are too many dates to show. Please type the date you want (month and day only),\n"
            "for example: `6/2`, `02.06`, or `June 2`. I will ignore the year."
        )
        download_date_prompt[user_id] = True

async def handle_date_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler for when an admin types a date manually (after /download if too many dates).
    """
    user_id = update.effective_user.id
    if user_id not in download_date_prompt:
        return  # Not waiting for date

    text = update.message.text.strip()
    # Try to parse month/day from the text
    try:
        # Accept formats: “MM/DD”, “DD.MM”, “MonthName D”
        # Attempt MM/DD first
        if "/" in text:
            parts = text.split("/")
            month = int(parts[0])
            day = int(parts[1])
        elif "." in text:
            parts = text.split(".")
            month = int(parts[1]) if len(parts) == 3 else int(parts[1])
            day = int(parts[0])
        else:
            # Try “July 2” or “Jun 2” or “6 2”
            parts = text.replace(",", "").split()
            month_str = parts[0]
            if month_str.isdigit():
                month = int(month_str)
            else:
                dt = datetime.strptime(month_str, "%B")
                month = dt.month
            day = int(parts[1])
        # We ignore year
        # Build search string “YYYY-MM-DD” for any year
        # Query for batches where month and day match
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, created_at FROM batches
            WHERE EXTRACT(MONTH FROM created_at AT TIME ZONE %s) = %s
              AND EXTRACT(DAY FROM created_at AT TIME ZONE %s) = %s
            ORDER BY created_at DESC;
        """, (TIMEZONE, month, TIMEZONE, day))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        if not rows:
            await update.message.reply_text(f"No batches found on date `{text}`.")
            download_date_prompt.pop(user_id, None)
            return
        batch_ids = [r[0] for r in rows]
        download_date_prompt.pop(user_id, None)
        # Now show the menu of batch IDs
        await send_batch_menu(update, context, batch_ids)
    except Exception as e:
        await update.message.reply_text(
            "Could not parse that date. Please use formats like `6/2`, `02.06`, or `June 2`."
        )
        return

async def send_batch_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, batch_ids):
    """
    Given a list of batch_ids, show up to 4 buttons plus “View More” if needed.
    """
    user_id = update.effective_user.id
    # Store all batch_ids in prompt state
    download_batch_prompt[user_id] = batch_ids

    # Show first 4
    to_show = batch_ids[:4]
    buttons = [InlineKeyboardButton(f"Batch {bid}", callback_data=f"batch|{bid}") for bid in to_show]
    keyboard = []
    for i in range(0, len(buttons), 2):
        keyboard.append(buttons[i:i+2])

    if len(batch_ids) > 4:
        keyboard.append([InlineKeyboardButton("View More…", callback_data="view_more")])

    await update.message.reply_text(
        "Select a batch:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_download_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles all callback_query data for /download flow:
      - “date|YYYY-MM-DD”
      - “batch|<id>”
      - “view_more”
    """
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    if data.startswith("date|"):
        # Admin tapped a date button
        chosen_date = data.split("|", 1)[1]  # “YYYY-MM-DD”
        # Fetch batch IDs for that exact date
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id FROM batches
            WHERE DATE(created_at AT TIME ZONE %s) = %s::date
            ORDER BY created_at DESC;
        """, (TIMEZONE, chosen_date))
        rows = cur.fetchall()
        cur.close()
        conn.close()

        batch_ids = [r[0] for r in rows]
        if not batch_ids:
            await query.message.reply_text(f"No batches found on date `{chosen_date}`.")
            return
        # Show batch menu
        await send_batch_menu(query, context, batch_ids)

    elif data == "view_more":
        # Show next page of batch IDs
        all_batches = download_batch_prompt.get(user_id, [])
        if not all_batches:
            await query.message.reply_text("No batches pending.")
            return
        # Find which were shown already by looking at last text’s buttons
        # Easiest: remove first four, then show next four
        remaining = all_batches[4:]
        if not remaining:
            await query.message.reply_text("No more batches.")
            return
        to_show = remaining[:4]
        buttons = [InlineKeyboardButton(f"Batch {bid}", callback_data=f"batch|{bid}") for bid in to_show]
        keyboard = []
        for i in range(0, len(buttons), 2):
            keyboard.append(buttons[i:i+2])
        if len(remaining) > 4:
            keyboard.append([InlineKeyboardButton("View More…", callback_data="view_more")])

        # Update the prompt so the next “view_more” shows further pages:
        download_batch_prompt[user_id] = remaining

        await query.message.reply_text(
            "Select a batch:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("batch|"):
        # Admin tapped a specific batch ID
        batch_id = int(data.split("|", 1)[1])
        # Clear any prompt state
        download_batch_prompt.pop(user_id, None)
        # Now send the checked‐logs .txt for batch_id
        await send_checked_logs_file(query, context, batch_id)

async def send_checked_logs_file(trigger, context, batch_id):
    """
    Sends a plain .txt file containing only the checked logs for that batch.
    Each line: <log_text> | Result: <result_text> | By: <worker_username> | At: <timestamp>
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT log_text, result_text, worker_username, checked_at
        FROM checked_logs
        WHERE batch_id = %s
        ORDER BY checked_at;
    """, (batch_id,))
    entries = cur.fetchall()
    cur.close()
    conn.close()

    tmp_dir = tempfile.mkdtemp()
    filename = f"batch_{batch_id}_checked.txt"
    filepath = os.path.join(tmp_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        if entries:
            for log_text, result_text, worker_username, checked_at in entries:
                ts_local = checked_at.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
                line = (
                    f"{log_text} | Result: {result_text} | "
                    f"By: {worker_username} | At: {ts_local}\n"
                )
                f.write(line)
        else:
            f.write("** No checked logs yet for this batch. **\n")

    # If triggered by callback_query, use trigger.message.reply_document(...)
    if isinstance(trigger, Update) and trigger.message:
        # Called from a “/download” workflow
        await trigger.message.reply_document(
            document=InputFile(filepath),
            filename=filename
        )
    else:
        # Likely a CallbackQuery
        await trigger.message.reply_document(
            document=InputFile(filepath),
            filename=filename
        )

# ------------------------
# 12) FALLBACK HANDLER FOR TEXT (dates or other commands)
# ------------------------

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Catch-all for plain‐text messages. Used when admin is in “date prompt” state.
    """
    user_id = update.effective_user.id
    if user_id in download_date_prompt:
        await handle_date_text(update, context)

# ------------------------
# 13) /cmdlist Handler
# ------------------------

async def cmdlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /cmdlist
    Only admins. Shows all admin commands and usage.
    """
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to view the command list.")
        return

    msg = (
        "*Admin Command List:*\n\n"
        "• `/add_user <id|@username> <worker|admin>`\n"
        "    – Add or update a user’s role. Example: `/add_user @alice worker`\n\n"
        "• `/list_workers`\n"
        "    – Show all registered users with their roles.\n\n"
        "• `/stats`\n"
        "    – Overall summary: remaining logs, checked today/week/month, per-user totals.\n\n"
        "• `/batch_stats <batch_id>`\n"
        "    – Detailed stats for one batch: total lines, processed vs remaining, who did how many.\n\n"
        "• `/queue`\n"
        "    – Real-time progress for all batches: processed/total (percent%), in progress or finished.\n\n"
        "• `/download`\n"
        "    – Start an interactive download: first pick a date, then a batch. Only checked‐logs are sent.\n\n"
        "• `/cmdlist`\n"
        "    – Show this list of admin commands.\n\n"
        "Notes:\n"
        "- To upload a new batch, send a `.txt` to the bot.\n"
        "- Invalid lines are returned immediately in `invalid_lines.txt`.\n"
        "- Workers use `/next` to get logs and reply with their balance.\n"
        "- Logs with balance > $100 will ping @manipulation in the group.\n"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

# ------------------------
# 14) /handle_document for New Batch Upload
# ------------------------

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Admin uploads a `.txt` → clean, create batch, insert good lines, return invalid lines if any.
    """
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to upload files.")
        return

    doc = update.message.document
    if not doc or not doc.file_name.lower().endswith(".txt"):
        await update.message.reply_text("Please upload a `.txt` file.")
        return

    # Download to temp
    file = await context.bot.get_file(doc.file_id)
    tmp_dir = tempfile.mkdtemp()
    local_path = os.path.join(tmp_dir, doc.file_name)
    await file.download_to_drive(custom_path=local_path)

    # Read and clean lines
    with open(local_path, "r", encoding="utf-8") as f:
        raw_lines = [line.strip() for line in f if line.strip()]
    good, bad = clean_lines(raw_lines)

    # Create new batch
    batch_id = create_new_batch(len(raw_lines))

    # Insert good lines into unchecked_logs
    conn = get_db_connection()
    cur = conn.cursor()
    for ln in good:
        cur.execute("""
            INSERT INTO unchecked_logs(batch_id, log_text)
            VALUES (%s, %s);
        """, (batch_id, ln))
    conn.commit()
    cur.close()
    conn.close()

    # Notify admin of results
    msg = (
        f"✅ Batch `{batch_id}` created with `{len(good)}` valid lines.\n"
        f"⚠️ `{len(bad)}` invalid lines."
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

    if bad:
        invalid_path = os.path.join(tmp_dir, "invalid_lines.txt")
        with open(invalid_path, "w", encoding="utf-8") as f_bad:
            for ln in bad:
                f_bad.write(ln + "\n")
        await update.message.reply_text(
            "Bad lines detected. See `invalid_lines.txt` for details."
        )
        await update.message.reply_document(
            document=InputFile(invalid_path),
            filename="invalid_lines.txt"
        )

# ------------------------
# 15) MAIN: BUILD APP, REGISTER HANDLERS, REGISTER COMMANDS
# ------------------------

def main():
    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # Worker commands
    app.add_handler(CommandHandler(["start", "next"], start_next))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_reply))

    # Admin commands
    app.add_handler(CommandHandler("add_user", add_user))
    app.add_handler(CommandHandler("list_workers", list_workers))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("batch_stats", batch_stats))
    app.add_handler(CommandHandler("queue", queue_status))
    app.add_handler(CommandHandler("download", download_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(handle_download_callback))

    # File upload for new batch
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # Schedule pending deletions
    app.job_queue.run_repeating(process_pending_deletions, interval=60, first=10)

    # Register “/” commands so Telegram shows a menu
    async def set_commands(application):
        await application.bot.set_my_commands([
            BotCommand("start",        "Get the next log (workers only)"),
            BotCommand("add_user",     "Add or update a user (admin only)"),
            BotCommand("list_workers", "List all registered users (admin only)"),
            BotCommand("stats",        "Show overall metrics (admin only)"),
            BotCommand("batch_stats",  "Show stats for a specific batch (admin only)"),
            BotCommand("queue",        "Show real-time progress for all batches (admin only)"),
            BotCommand("download",     "Download checked logs by date & batch (admin only)"),
            BotCommand("cmdlist",      "Show a list of all admin commands (admin only)")
        ])
    app.post_init = set_commands

    app.run_polling()

if __name__ == "__main__":
    main()
