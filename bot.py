# bot.py

import os
import re
import logging
import tempfile
import psycopg2
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
# 1) BASIC LOGGING & DEBUGGING
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

# ------------------------
# 2.1) CONSTANTS
# ------------------------
# Group chat ID where logs are posted (verify the bot is an admin in that group)
GROUP_CHAT_ID = -1002637490216

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
download_date_prompt = {}    # admin_id → True
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
# 10) CLEANUP TIMED‐OUT ASSIGNMENTS (30 min)
# ------------------------
async def cleanup_timed_out_assignments(context: ContextTypes.DEFAULT_TYPE):
    """
    Every minute: any unchecked_log assigned >30 minutes ago → unassign it
    and clear pending_assignment so the worker can request a new log.
    """
    threshold = datetime.now(tz) - timedelta(minutes=30)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT id, assigned_to
        FROM unchecked_logs
        WHERE assigned_to IS NOT NULL
          AND assigned_at < (NOW() AT TIME ZONE '{TIMEZONE}') - INTERVAL '30 minutes';
    """)
    rows = cur.fetchall()
    for log_id, worker_id in rows:
        # Unassign in DB
        cur.execute("""
            UPDATE unchecked_logs
            SET assigned_to = NULL, assigned_at = NULL
            WHERE id = %s;
        """, (log_id,))
        # Clear in-memory assignment
        if worker_id in pending_assignment:
            pending_assignment.pop(worker_id, None)
            # Optionally, notify the worker that their assignment expired
            try:
                await context.bot.send_message(
                    chat_id=worker_id,
                    text="⚠️ Your previous log assignment timed out (30 min). You can request a new log with /start."
                )
            except Exception as e:
                logger.warning(f"Could not notify worker {worker_id} about timeout: {e}")
    conn.commit()
    cur.close()
    conn.close()

# ------------------------
# 11) WORKER & ADMIN COMMAND HANDLERS
# ------------------------
async def start_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /start or /next → assign one available log to a worker.
    """
    user_id = update.effective_user.id
    logger.info(f"===== start_next called for user={user_id}")
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
        "Please reply with the **balance** (e.g. `12.50` or `$12`).",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("Next log ⏭️")]],
            one_time_keyboard=False,
            resize_keyboard=True
        )
    )

    # Schedule deletion of this “assignment” message in 30 minutes
    delete_at = datetime.now(tz) + timedelta(minutes=30)
    schedule_message_deletion(update.effective_chat.id, sent_msg.message_id, delete_at)

    pending_assignment[user_id] = (log_id, sent_msg.message_id, batch_id)

async def handle_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    When a worker sends any plain-text that looks like a numeric balance,
    we:
      1. Check if they have a pending assignment. If so, pull that log.
      2. Save it into checked_logs.
      3. Post "<orig_log> | balance = $X.XX" in the group.
         If > 100, also ping HIGH_BALANCE_PING.
      4. Immediately give them the next log (call start_next).
    """
    user_id = update.effective_user.id
    text = update.message.text.strip()
    logger.info(f"===== handle_reply called: user={user_id}, text='{text}'")

    try:
        authorized, role = is_user_authorized(user_id)
        if not authorized or role != "worker":
            return

        if user_id not in pending_assignment:
            return

        m_balance = BALANCE_RE.search(text)
        if not m_balance:
            await update.message.reply_text(
                "❌ Please send a valid balance (just a number or decimal, e.g. `12` or `12.50` or `$12`)."
            )
            return

        try:
            balance_amount = float(m_balance.group())
        except:
            await update.message.reply_text("❌ Could not parse that number. Please send e.g. `12.50` or `$12`.")
            return

        log_id, msg_to_delete, batch_id = pending_assignment[user_id]

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT log_text FROM unchecked_logs WHERE id = %s;", (log_id,))
        res = cur.fetchone()
        if not res:
            # Already processed or missing
            pending_assignment.pop(user_id, None)
            await update.message.reply_text("⚠️ That log is no longer available. Trying next…")
            cur.close()
            conn.close()
            await start_next(update, context)
            return

        orig_text = res[0]

        # Get the worker's username
        user_obj = await context.bot.get_chat(user_id)
        worker_username = user_obj.username or str(user_id)

        # Insert into checked_logs
        cur.execute("""
            INSERT INTO checked_logs(
                batch_id, orig_log_id, log_text,
                worker_id, worker_username, result_text, checked_at
            ) VALUES (%s,%s,%s,%s,%s,%s,NOW() AT TIME ZONE %s);
        """, (
            batch_id, log_id, orig_text,
            user_id, worker_username, text, TIMEZONE
        ))

        # Delete from unchecked_logs
        cur.execute("DELETE FROM unchecked_logs WHERE id = %s;", (log_id,))
        conn.commit()
        mark_batch_finished_if_complete(batch_id)
        cur.close()
        conn.close()

        # Clear in-memory assignment
        pending_assignment.pop(user_id, None)

        # Acknowledge to worker
        await update.message.reply_text("✅ Balance recorded! Sending your next log…")

        # Post to group
        try:
            balance_text = f"{orig_text} | balance = ${balance_amount:.2f}"
            await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=balance_text)
            if balance_amount > 100:
                celebratory = (
                    f"{HIGH_BALANCE_PING} 🎉 Whoa! This log had a HIGH balance of "
                    f"${balance_amount:.2f}! 🎉"
                )
                await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=celebratory)
        except Exception as e:
            logger.error(f"Failed to post to group: {e}")

        # Give next log
        await start_next(update, context)

    except Exception as e:
        logger.exception(f"Exception inside handle_reply: {e}")

# ------------------------
# 12) ADMIN COMMAND HANDLERS
# ------------------------
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

# ------------------------
# 13) QUEUE STATUS (NO DOWNLOAD BUTTON)
# ------------------------
async def queue_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /queue
    Only admins. Lists all batches with progress, no download button.
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
# 14) DOWNLOAD MENU HELPERS
# ------------------------
def format_date_label(dt_obj):
    """
    Given a date, return “MM-DD” (e.g. “06-02”).
    """
    return dt_obj.strftime("%m-%d")

async def download_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /download
    Only admins. Show up to 4 most recent dates as buttons.
    If > 4 dates, show last 4; also prompt admin to type a date manually.
    """
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to download batches.")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    # Fetch distinct creation dates (date part only), newest first
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

    # Show only the 4 most recent
    to_show = dates[:4]
    buttons = []
    for dt in to_show:
        lbl = format_date_label(dt)
        data = f"date|{dt.strftime('%Y-%m-%d')}"
        buttons.append(InlineKeyboardButton(lbl, callback_data=data))

    # Arrange buttons in 2 columns
    keyboard = []
    for i in range(0, len(buttons), 2):
        keyboard.append(buttons[i:i+2])

    await update.message.reply_text(
        "Select a date (or type another date in MM/DD, DD.MM, or “Month D” format):",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    download_date_prompt[user_id] = True

async def handle_date_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler for when an admin types a date manually (after /download).
    """
    user_id = update.effective_user.id
    if user_id not in download_date_prompt:
        return  # Not waiting for date

    text = update.message.text.strip()
    try:
        # Parse month/day from text
        if "/" in text:
            parts = text.split("/")
            month = int(parts[0])
            day = int(parts[1])
        elif "." in text:
            parts = text.split(".")
            month = int(parts[1])
            day = int(parts[0])
        else:
            # “June 2” or “Jun 2” or “6 2”
            parts = text.replace(",", "").split()
            month_str = parts[0]
            if month_str.isdigit():
                month = int(month_str)
            else:
                dt = datetime.strptime(month_str, "%B")
                month = dt.month
            day = int(parts[1])

        # Find batches matching that month/day (any year)
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id FROM batches
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
        await send_batch_menu(update, context, batch_ids)

    except Exception:
        await update.message.reply_text(
            "Could not parse that date. Please use formats like `6/2`, `02.06`, or `June 2`."
        )

async def send_batch_menu(trigger, context: ContextTypes.DEFAULT_TYPE, batch_ids):
    """
    Given a list of batch_ids, show up to 4 buttons for "Download Batch ⟨id⟩".
    """
    # Store the full list so “View More” could be implemented if needed.
    user_id = trigger.from_user.id if hasattr(trigger, "from_user") else trigger.effective_user.id
    download_batch_prompt[user_id] = batch_ids

    # Show up to 4
    to_show = batch_ids[:4]
    buttons = [InlineKeyboardButton(f"Batch {bid}", callback_data=f"batch|{bid}") for bid in to_show]
    keyboard = []
    for i in range(0, len(buttons), 2):
        keyboard.append(buttons[i:i+2])

    await trigger.message.reply_text(
        "Select a batch to download:",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def handle_download_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles callback_query data for /download flow:
      - “date|YYYY-MM-DD”
      - “batch|<id>”
    """
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    if data.startswith("date|"):
        # Admin tapped a date button
        chosen_date = data.split("|", 1)[1]  # “YYYY-MM-DD”
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
        await send_batch_menu(query, context, batch_ids)

    elif data.startswith("batch|"):
        # Admin tapped a specific batch ID
        batch_id = int(data.split("|", 1)[1])
        download_batch_prompt.pop(user_id, None)
        await send_checked_logs_file(query, context, batch_id)

async def send_checked_logs_file(trigger, context: ContextTypes.DEFAULT_TYPE, batch_id):
    """
    Sends:
      1) A summary message: "📊 X/Y done." plus per-worker counts
      2) A .txt file containing all the checked logs for that batch, in the format:
         <log_text> | balance = $X.XX
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # Total lines in batch
    cur.execute("SELECT total_lines FROM batches WHERE id = %s;", (batch_id,))
    total_line_row = cur.fetchone()
    total_lines = total_line_row[0] if total_line_row else 0

    # Count checked logs
    cur.execute("SELECT COUNT(*) FROM checked_logs WHERE batch_id = %s;", (batch_id,))
    checked_count = cur.fetchone()[0]

    # Per-worker breakdown
    cur.execute("""
        SELECT worker_username, COUNT(*) AS cnt
        FROM checked_logs
        WHERE batch_id = %s
        GROUP BY worker_username
        ORDER BY cnt DESC;
    """, (batch_id,))
    per_user = cur.fetchall()

    # Prepare summary text
    if checked_count == 0:
        summary_msg = f"📊 0/{total_lines} done.\nNo workers processed lines yet."
    else:
        summary_msg = f"📊 {checked_count}/{total_lines} done.\n"
        for wk, cnt in per_user:
            summary_msg += f"{wk}: {cnt} replies\n"

    await trigger.message.reply_text(summary_msg)

    # Now build the .txt file
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
                # Extract balance from result_text
                m_balance = BALANCE_RE.search(result_text)
                bal = m_balance.group() if m_balance else "0"
                f.write(f"{log_text} | balance = ${float(bal):.2f}\n")
        else:
            f.write("** No checked logs yet for this batch. **\n")

    # Send as a .txt attachment
    try:
        await trigger.message.reply_document(
            document=InputFile(filepath, filename=filename),
            filename=filename
        )
    except Exception as e:
        logger.error(f"Failed to send batch file: {e}")

# ------------------------
# 15) FALLBACK HANDLER FOR TEXT (dates)
# ------------------------
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Catch-all for plain‐text messages. Used when admin is in “date prompt” state.
    """
    user_id = update.effective_user.id
    if user_id in download_date_prompt:
        await handle_date_text(update, context)

# ------------------------
# 16) /cmdlist Handler
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
        "• `/queue`\n"
        "    – Lists all batches with progress (no download button).\n\n"
        "• `/download`\n"
        "    – Download checked logs by date → batch (admin only).\n\n"
        "• `/cmdlist`\n"
        "    – Show this list of admin commands.\n\n"
        "Notes:\n"
        "- To upload a new batch, send a `.txt` file in this chat.\n"
        "- Invalid lines returned immediately in `invalid_lines.txt`.\n"
        "- Workers use `/next` or `/start` to get logs and reply with balance.\n"
        "- Logs with balance > $100 will ping @manipulation in the group.\n"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

# ------------------------
# 17) /handle_document for New Batch Upload
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
            document=InputFile(invalid_path, filename="invalid_lines.txt"),
            filename="invalid_lines.txt"
        )

# ------------------------
# 18) MAIN: SET UP BOT, REGISTER HANDLERS, REGISTER COMMANDS
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
    app.add_handler(CommandHandler("queue", queue_status))
    app.add_handler(CallbackQueryHandler(handle_download_callback, pattern="^(date\\||batch\\|)"))
    app.add_handler(CommandHandler("download", download_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CommandHandler("cmdlist", cmdlist))

    # File upload (new batch)
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # Schedule jobs:
    #  1) Pending deletions → every minute
    #  2) Cleanup timed-out assignments → every minute
    app.job_queue.run_repeating(process_pending_deletions, interval=60, first=10)
    app.job_queue.run_repeating(cleanup_timed_out_assignments, interval=60, first=15)

    # Register “/” commands so Telegram shows a menu
    async def set_commands(application):
        await application.bot.set_my_commands([
            BotCommand("start",        "Get the next log (workers only)"),
            BotCommand("add_user",     "Add or update a user (admin only)"),
            BotCommand("list_workers", "List all registered users (admin only)"),
            BotCommand("stats",        "Show overall metrics (admin only)"),
            BotCommand("queue",        "Show all batches (admin only)"),
            BotCommand("download",     "Download checked logs by date → batch (admin only)"),
            BotCommand("cmdlist",      "Show a list of all admin commands (admin only)")
        ])
    app.post_init = set_commands

    app.run_polling()

if __name__ == "__main__":
    main()
