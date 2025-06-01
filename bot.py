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
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
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
# BOT_TOKEN: your Telegram Bot API token (must be set as an environment variable)
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    logger.error("Error: BOT_TOKEN not set. Exiting.")
    exit(1)

# DATABASE_URL: Railway’s PostgreSQL URL (must be set as an environment variable)
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    logger.error("Error: DATABASE_URL not set. Exiting.")
    exit(1)

# Timezone (Asia/Kolkata)
TIMEZONE = "Asia/Kolkata"
tz = pytz.timezone(TIMEZONE)

# ------------------------
# 3) REGEXES FOR CLEANING
# ------------------------
PHONE_RE = re.compile(r"Phone Number:\s*([0-9\-\s]+)")
CARD_RE  = re.compile(r"Loyalty Card:\s*(\d{16})")
DIGITS_RE = re.compile(r"[^\d]")  # strips non-digits

# ------------------------
# 4) IN-MEMORY TRACKERS
# ------------------------
# pending_assignment: 
#   telegram_id → (log_id, message_id_for_deletion, batch_id)
pending_assignment = {}

# download_prompt: 
#   telegram_id → [list_of_matching_batch_ids]
#   Used when /download_batch <date> yields multiple matches
download_prompt = {}

# ------------------------
# 5) DATABASE HELPERS
# ------------------------

def get_db_connection():
    """
    Returns a new psycopg2 connection with autocommit = True.
    """
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn

def init_db():
    """
    Creates all necessary tables if they don’t exist:
      - users
      - batches
      - unchecked_logs
      - checked_logs
      - pending_deletions
    """
    conn = get_db_connection()
    cur = conn.cursor()

    # 1) users table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            role VARCHAR(10) NOT NULL CHECK(role IN ('worker','admin')),
            name TEXT,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    # 2) batches table
    #    Each uploaded file becomes one batch
    cur.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            total_lines INTEGER NOT NULL,
            finished_at TIMESTAMP WITH TIME ZONE
        );
    """)

    # 3) unchecked_logs table
    #    Holds all raw lines from each batch that are not yet processed.
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

    # 4) checked_logs table
    #    Once a worker replies, we move that row here, keeping batch_id.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS checked_logs (
            id SERIAL PRIMARY KEY,
            batch_id INTEGER NOT NULL,
            orig_log_id INTEGER REFERENCES unchecked_logs(id) ON DELETE CASCADE,
            log_text TEXT NOT NULL,
            worker_id BIGINT REFERENCES users(telegram_id),
            result_text TEXT NOT NULL,
            checked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    # 5) pending_deletions table
    #    For scheduling deletion of Telegram messages after 4 hours.
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pending_deletions (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            message_id INTEGER NOT NULL,
            delete_at TIMESTAMP WITH TIME ZONE NOT NULL
        );
    """)

    # Index to speed up finding expired pending_deletions
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
    Takes a list of raw strings (lines). Returns two lists:
      - good: cleaned lines (phone formatted, validated card)
      - bad: lines that failed validation, with a note appended.
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
    """
    Returns (True, role) if telegram_id is in users table.
    Otherwise returns (False, None).
    """
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
    """
    Inserts a new row into batches with total_lines.
    Returns the new batch_id.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO batches(total_lines) VALUES (%s) RETURNING id;",
        (total_lines,)
    )
    batch_id = cur.fetchone()[0]
    cur.close()
    conn.close()
    return batch_id

def mark_batch_finished_if_complete(batch_id):
    """
    If there are no more unchecked_logs for this batch, set finished_at = NOW().
    """
    conn = get_db_connection()
    cur = conn.cursor()
    # Count remaining logs in unchecked_logs for this batch
    cur.execute(
        "SELECT COUNT(*) FROM unchecked_logs WHERE batch_id = %s;",
        (batch_id,)
    )
    remaining = cur.fetchone()[0]
    if remaining == 0:
        # Set finished_at only if it isn’t already set
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
    """
    Add (chat_id, message_id, delete_at) to pending_deletions.
    """
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
    Runs every minute: finds any pending_deletions where delete_at <= now,
    calls bot.delete_message, then removes from table.
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
            # Might have already been deleted or expired
            logger.warning(f"Failed to delete message {message_id} in chat {chat_id}: {e}")
        # Remove from DB
        cur.execute("DELETE FROM pending_deletions WHERE id = %s;", (pd_id,))
    conn.commit()
    cur.close()
    conn.close()

# ------------------------
# 10) COMMAND HANDLERS
# ------------------------

async def start_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler for /start or /next.
    Only for role == 'worker'. Assigns one available log across all batches.
    """
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "worker":
        await update.message.reply_text("❌ You are not authorized to get a log. Ask an admin to add you.")
        return

    # If worker already has a pending assignment, remind them:
    if user_id in pending_assignment:
        await update.message.reply_text(
            "⚠️ You already have a log assigned. Please reply with your result before requesting another."
        )
        return

    # 1) Pick one available log (across all batches) that is unassigned or expired
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f"""
        WITH candidate AS (
            SELECT id, log_text, batch_id
            FROM unchecked_logs
            WHERE
              (
                assigned_to IS NULL
                OR assigned_at < (NOW() AT TIME ZONE '{TIMEZONE}') - INTERVAL '30 minutes'
              )
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE unchecked_logs
        SET
          assigned_to = %s,
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
    # Send the log to the worker, and capture message_id so we can delete later
    sent_msg = await update.message.reply_text(
        f"🆕 *Batch {batch_id}* — Log ID: `{log_id}`\n\n"
        f"{log_text}\n\n"
        "Please reply with your findings within 30 minutes.",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("Next log ⏭️")]],
            one_time_keyboard=False,
            resize_keyboard=True
        )
    )

    # Schedule this message for deletion in 4 hours
    delete_at = datetime.now(tz) + timedelta(hours=4)
    schedule_message_deletion(update.effective_chat.id, sent_msg.message_id, delete_at)

    # Track pending assignment
    pending_assignment[user_id] = (log_id, sent_msg.message_id, batch_id)

async def handle_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles plain-text replies from a worker. If they have a pending assignment, 
    move that row into checked_logs with their reply, then acknowledge.
    Also updates batch finished_at if needed.
    """
    user_id = update.effective_user.id
    text = update.message.text.strip()
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "worker":
        return  # ignore

    if user_id not in pending_assignment:
        return  # no pending assignment, ignore

    log_id, msg_to_delete, batch_id = pending_assignment[user_id]

    # 1) Fetch original log_text
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

    # 2) Insert into checked_logs
    cur.execute("""
        INSERT INTO checked_logs(batch_id, orig_log_id, log_text, worker_id, result_text, checked_at)
        VALUES (%s, %s, %s, %s, %s, NOW() AT TIME ZONE %s);
    """, (batch_id, log_id, orig_text, user_id, text, TIMEZONE))

    # 3) Delete from unchecked_logs
    cur.execute("DELETE FROM unchecked_logs WHERE id = %s;", (log_id,))
    conn.commit()

    # 4) Check if this batch is now complete
    mark_batch_finished_if_complete(batch_id)

    cur.close()
    conn.close()

    # Clear pending assignment (the message will be deleted by the scheduled job)
    del pending_assignment[user_id]

    await update.message.reply_text("✅ Got it! Your result has been saved. Thank you.")

async def add_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /add_user <telegram_id_or_username> <worker|admin>
    Only admins can run this. Adds or updates user role.
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

    # Resolve @username to numeric ID, if needed
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
    Only admins. Lists all users with their roles and creation times.
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
    Only admins. Shows:
      - Remaining logs (all batches combined)
      - Checked Today / This Week / This Month (all batches)
      - Per-user totals (all time)
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
          AND checked_at < date_trunc('month', NOW() ATimeZone '{TIMEZONE}') + INTERVAL '1 month';
    """)
    checked_month = cur.fetchone()[0]

    # Per-user totals (all time)
    cur.execute("""
        SELECT worker_id, COUNT(*) AS total
        FROM checked_logs
        GROUP BY worker_id
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
    Only admins. Shows for that batch:
      - total_lines
      - processed vs remaining
      - started_at, finished_at (if done)
      - how many each user processed
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

    # Count remaining logs in that batch
    cur.execute("SELECT COUNT(*) FROM unchecked_logs WHERE batch_id = %s;", (batch_id,))
    remaining = cur.fetchone()[0]
    processed = total_lines - remaining

    # Per-user count for this batch
    cur.execute("""
        SELECT worker_id, COUNT(*) AS cnt
        FROM checked_logs
        WHERE batch_id = %s
        GROUP BY worker_id
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
    Only admins. Shows a list of all batches, with real-time progress for each:
      (processed / total) and percentage, plus “In Progress” or “Finished (date)”.
    """
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to view queue.")
        return

    conn = get_db_connection()
    cur = conn.cursor()

    # Fetch all batches (id, total_lines, created_at, finished_at)
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
        # Count how many processed for this batch
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

async def download_batch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /download_batch <batch_id_or_DD.MM.YY>
    Only admins. Sends a .txt file with every checked log for that batch:
      <log_text> | Result: <result_text> | By: <worker_id> | At: <timestamp>
    If multiple batches match the date, ask the admin to choose one.
    """
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to download batches.")
        return

    args = context.args
    if user_id in download_prompt and args:
        # If we were already prompting this admin for batch choice but they typed a new argument,
        # clear the old prompt.
        del download_prompt[user_id]

    if user_id in download_prompt:
        # The admin previously was given a list of multiple batch IDs and is now replying with one of them.
        try:
            chosen = int(args[0])
        except (IndexError, ValueError):
            await update.message.reply_text("Please reply with a valid batch number from the list.")
            return

        if chosen not in download_prompt[user_id]:
            await update.message.reply_text(
                f"Batch `{chosen}` was not in the list. Please type one of: {download_prompt[user_id]}"
            )
            return

        # Now fetch and send the chosen batch
        batch_id = chosen
        del download_prompt[user_id]  # clear the prompt state
    else:
        # First step: determine if args[0] is batch_id or date
        if len(args) != 1:
            await update.message.reply_text("Usage: /download_batch <batch_id_or_DD.MM.YY>")
            return

        param = args[0]
        conn = get_db_connection()
        cur = conn.cursor()

        # Try interpreting param as integer batch_id
        batch_id = None
        try:
            bid = int(param)
            # Check if that batch exists
            cur.execute("SELECT 1 FROM batches WHERE id = %s;", (bid,))
            if cur.fetchone():
                batch_id = bid
            else:
                await update.message.reply_text(f"Batch `{bid}` not found. Please try again.")
                cur.close()
                conn.close()
                return
        except ValueError:
            # Not an integer; try parsing as date DD.MM.YY or DD.MM.YYYY
            try:
                parts = param.split(".")
                if len(parts) == 3:
                    day = int(parts[0])
                    month = int(parts[1])
                    year = int(parts[2])
                    if year < 100:
                        year += 2000
                    dt = datetime(year, month, day, tzinfo=tz)
                    date_str = dt.strftime("%Y-%m-%d")
                else:
                    raise ValueError
            except:
                await update.message.reply_text("Invalid format. Use batch ID or date DD.MM.YY / DD.MM.YYYY.")
                cur.close()
                conn.close()
                return

            # Find batches created on that date
            cur.execute("""
                SELECT id FROM batches
                WHERE DATE(created_at AT TIME ZONE %s) = %s::date;
            """, (TIMEZONE, date_str))
            rows = cur.fetchall()
            if not rows:
                await update.message.reply_text(f"No batch found for date `{param}`.", parse_mode="Markdown")
                cur.close()
                conn.close()
                return

            batch_ids = [r[0] for r in rows]
            if len(batch_ids) == 1:
                batch_id = batch_ids[0]
            else:
                # Multiple batches on that date: ask the admin to choose
                await update.message.reply_text(
                    f"On `{param}` we created these batches: {batch_ids}\n"
                    "Please reply with the batch number you wish to download."
                )
                # Store this list so we know what’s valid next time
                download_prompt[user_id] = batch_ids
                cur.close()
                conn.close()
                return

        cur.close()
        conn.close()

    # At this point, we have a valid batch_id to download
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT log_text, result_text, worker_id, checked_at
        FROM checked_logs
        WHERE batch_id = %s
        ORDER BY checked_at;
    """, (batch_id,))
    entries = cur.fetchall()
    if not entries:
        await update.message.reply_text(f"No checked logs found for batch `{batch_id}`.", parse_mode="Markdown")
        cur.close()
        conn.close()
        return

    # Write to a temp .txt file
    tmp_dir = tempfile.mkdtemp()
    filename = f"batch_{batch_id}_checked.txt"
    filepath = os.path.join(tmp_dir, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        for log_text, result_text, worker_id, checked_at in entries:
            ts_local = checked_at.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
            line = (
                f"{log_text} | Result: {result_text} | "
                f"By: {worker_id} | At: {ts_local}\n"
            )
            f.write(line)

    # Send the file
    await update.message.reply_document(
        document=InputFile(filepath),
        filename=filename
    )
    cur.close()
    conn.close()

async def cmdlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /cmdlist
    Only admins. Sends a list of every admin command and what it does.
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
        "• `/download_batch <batch_id|DD.MM.YY>`\n"
        "    – Download a .txt of all checked logs in that batch. If multiple batches share the date, you’ll be prompted to pick one.\n\n"
        "• `/cmdlist`\n"
        "    – Show this list of admin commands.\n\n"
        "Notes:\n"
        "- To upload a new batch, simply send a `.txt` file in this chat.\n"
        "- Invalid lines will be returned in `invalid_lines.txt` immediately.\n"
        "- Workers use `/next` (or tap “Next log ⏭️”) to get logs, then reply with their findings.\n"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    When an admin sends a .txt file, this runs:
      - Download file
      - Read lines
      - Clean lines
      - Create new batch
      - Insert good lines into unchecked_logs with batch_id
      - Write bad lines to invalid_lines.txt and send back
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

    # 1) Download file to temp
    file = await context.bot.get_file(doc.file_id)
    tmp_dir = tempfile.mkdtemp()
    local_path = os.path.join(tmp_dir, doc.file_name)
    await file.download_to_drive(custom_path=local_path)

    # 2) Read lines
    with open(local_path, "r", encoding="utf-8") as f:
        raw_lines = [line.strip() for line in f if line.strip()]

    # 3) Clean lines
    good, bad = clean_lines(raw_lines)

    # 4) Create new batch
    batch_id = create_new_batch(len(raw_lines))

    # 5) Insert good lines into unchecked_logs with this batch_id
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

    # 6) Inform how many good vs bad
    msg = (
        f"✅ Batch `{batch_id}` created with `{len(good)}` valid lines.\n"
        f"⚠️ `{len(bad)}` invalid lines."
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

    # 7) If there are bad lines, write to invalid_lines.txt and send back
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
# 11) MAIN: SET UP BOT, HANDLERS, AND JOBS
# ------------------------

def main():
    # 1) Ensure tables exist
    init_db()

    # 2) Build the Application
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # 3) Register command handlers
    # Worker commands
    app.add_handler(CommandHandler(["start", "next"], start_next))

    # Admin commands
    app.add_handler(CommandHandler("add_user", add_user))
    app.add_handler(CommandHandler("list_workers", list_workers))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("batch_stats", batch_stats))
    app.add_handler(CommandHandler("queue", queue_status))
    app.add_handler(CommandHandler("download_batch", download_batch))
    app.add_handler(CommandHandler("cmdlist", cmdlist))

    # File upload (admin)
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # Worker replies (plain text)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_reply))

    # 4) Schedule job: process_pending_deletions every minute
    app.job_queue.run_repeating(process_pending_deletions, interval=60, first=10)

    # ─── 5) REGISTER “/” COMMANDS FOR TELEGRAM MENU ───────────────────────────────
    async def set_commands(application):
        await application.bot.set_my_commands([
            BotCommand("start",        "Get the next log (workers only)"),
            BotCommand("add_user",     "Add or update a user (admin only)"),
            BotCommand("list_workers", "List all registered users (admin only)"),
            BotCommand("stats",        "Show overall metrics (admin only)"),
            BotCommand("batch_stats",  "Show stats for a specific batch (admin only)"),
            BotCommand("queue",        "Show real-time progress for all batches (admin only)"),
            BotCommand("download_batch", "Download a checked batch as a .txt (admin only)"),
            BotCommand("cmdlist",      "Show a list of all admin commands (admin only)")
        ])

    app.post_init = set_commands

    # 6) Start polling
    app.run_polling()

if __name__ == "__main__":
    main()
