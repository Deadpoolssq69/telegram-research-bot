# bot.py

import os
import re
import logging
import tempfile
import psycopg2
from datetime import datetime, timedelta
import pytz

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

# ------------------------
# 2.1) CONSTANTS
# ------------------------
GROUP_CHAT_ID = -4828152242      # Group where processed logs get posted
HIGH_BALANCE_PING = "@manipulation"
TIMEZONE = "Asia/Kolkata"
tz = pytz.timezone(TIMEZONE)

# ------------------------
# 3) REGEXES
# ------------------------
PHONE_RE   = re.compile(r"Phone Number:\s*([0-9\-\s]+)")
CARD_RE    = re.compile(r"Loyalty Card:\s*(\d{16})")
DIGITS_RE  = re.compile(r"[^\d]")
BALANCE_RE = re.compile(r"[-+]?\d*\.?\d+")

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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            telegram_id BIGINT UNIQUE NOT NULL,
            role VARCHAR(10) NOT NULL CHECK(role IN ('worker','admin')),
            name TEXT,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS batches (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            total_lines INTEGER NOT NULL,
            finished_at TIMESTAMP WITH TIME ZONE
        );
    """)
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
# 9) MESSAGE DELETION & EXPIRATION
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
    now = datetime.now(tz)
    conn = get_db_connection()
    cur = conn.cursor()

    # 1) Delete scheduled Telegram messages
    cur.execute("""
        SELECT id, chat_id, message_id
        FROM pending_deletions
        WHERE delete_at <= %s;
    """, (now,))
    rows = cur.fetchall()
    for pd_id, chat_id, message_id in rows:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except:
            pass
        cur.execute("DELETE FROM pending_deletions WHERE id = %s;", (pd_id,))

    # 2) Expire assignments older than 30 minutes
    cutoff = now - timedelta(minutes=30)
    for worker_id, (log_id, msg_id, batch_id) in list(pending_assignment.items()):
        cur.execute("SELECT assigned_at FROM unchecked_logs WHERE id = %s;", (log_id,))
        fetched = cur.fetchone()
        if not fetched:
            # Already processed or removed
            del pending_assignment[worker_id]
            continue
        assigned_at = fetched[0]
        if assigned_at and assigned_at.replace(tzinfo=tz) <= cutoff:
            # Un-assign in DB
            cur.execute("""
                UPDATE unchecked_logs
                SET assigned_to = NULL, assigned_at = NULL
                WHERE id = %s;
            """, (log_id,))
            conn.commit()
            del pending_assignment[worker_id]
            # Notify worker that assignment expired
            try:
                await context.bot.send_message(
                    chat_id=worker_id,
                    text="⌛ Your previous assignment expired after 30 minutes of inactivity. "
                         "Type /start to get a new log.",
                    reply_markup=ReplyKeyboardMarkup([[]], one_time_keyboard=True)
                )
            except:
                pass

    conn.commit()
    cur.close()
    conn.close()

# ------------------------
# 10) WORKER & ADMIN HANDLERS
# ------------------------
async def start_next(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        "Please reply with the **balance** (e.g. `12.50` or `$12`).",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("Next log ⏭️")]],
            one_time_keyboard=False,
            resize_keyboard=True
        )
    )

    delete_at = datetime.now(tz) + timedelta(hours=4)
    schedule_message_deletion(update.effective_chat.id, sent_msg.message_id, delete_at)
    pending_assignment[user_id] = (log_id, sent_msg.message_id, batch_id)

async def handle_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
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
        del pending_assignment[user_id]
        await update.message.reply_text("⚠️ That log is no longer available. Trying next...")
        cur.close()
        conn.close()
        await start_next(update, context)
        return

    orig_text = res[0]
    user_obj = await context.bot.get_chat(user_id)
    worker_username = user_obj.username or str(user_id)

    cur.execute("""
        INSERT INTO checked_logs(
            batch_id, orig_log_id, log_text,
            worker_id, worker_username, result_text, checked_at
        ) VALUES (%s,%s,%s,%s,%s,%s,NOW() AT TIME ZONE %s);
    """, (
        batch_id, log_id, orig_text,
        user_id, worker_username, text, TIMEZONE
    ))
    cur.execute("DELETE FROM unchecked_logs WHERE id = %s;", (log_id,))
    conn.commit()
    mark_batch_finished_if_complete(batch_id)
    cur.close()
    conn.close()

    del pending_assignment[user_id]
    await update.message.reply_text("✅ Balance recorded! Sending your next log...")

    balance_text = f"{orig_text} | Balance =${balance_amount:.2f}"
    await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=balance_text)

    if balance_amount > 100:
        celebratory = (
            f"{HIGH_BALANCE_PING} 🎉 Whoa! This log had a HIGH balance of "
            f"${balance_amount:.2f}! 🎉"
        )
        await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=celebratory)

    await start_next(update, context)

async def add_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to view stats.")
        return

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(f"""
        SELECT COUNT(*) FROM unchecked_logs
        WHERE assigned_to IS NULL
          OR assigned_at < (NOW() AT TIME ZONE '{TIMEZONE}') - INTERVAL '30 minutes';
    """)
    remaining = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(*) FROM checked_logs
        WHERE checked_at >= date_trunc('day', NOW() AT TIME ZONE '{TIMEZONE}')
          AND checked_at < date_trunc('day', NOW() AT TIME ZONE '{TIMEZONE}') + INTERVAL '1 day';
    """)
    checked_today = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(*) FROM checked_logs
        WHERE checked_at >= date_trunc('week', NOW() AT TIME ZONE '{TIMEZONE}')
          AND checked_at < date_trunc('week', NOW() AT TIME ZONE '{TIMEZONE}') + INTERVAL '1 week';
    """)
    checked_week = cur.fetchone()[0]

    cur.execute(f"""
        SELECT COUNT(*) FROM checked_logs
        WHERE checked_at >= date_trunc('month', NOW() AT TIME ZONE '{TIMEZONE}')
          AND checked_at < date_trunc('month', NOW() AT TIME ZONE '{TIMEZONE}') + INTERVAL '1 month';
    """)
    checked_month = cur.fetchone()[0]

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

async def queue_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

    keyboard = [[InlineKeyboardButton("Download Finished Summaries", callback_data="download_finished")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=reply_markup)

async def handle_queue_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data != "download_finished":
        return

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, total_lines, created_at, finished_at
        FROM batches
        WHERE finished_at IS NOT NULL
        ORDER BY created_at;
    """)
    finished_batches = cur.fetchall()
    if not finished_batches:
        await query.message.reply_text("No finished batches to summarize.")
        cur.close()
        conn.close()
        return

    tmp_dir = tempfile.mkdtemp()
    filename = "finished_batches_summary.txt"
    filepath = os.path.join(tmp_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        for bid, total_lines, created_at, finished_at in finished_batches:
            created_local = created_at.astimezone(tz).strftime("%Y-%m-%d")
            f.write(f"Batch {bid} (Created: {created_local}):\n")
            f.write(f"  • Total lines: {total_lines}\n")
            cur.execute("SELECT COUNT(*) FROM checked_logs WHERE batch_id = %s;", (bid,))
            checked_count = cur.fetchone()[0]
            f.write(f"  • Checked lines: {checked_count}\n")
            cur.execute("""
                SELECT worker_username, COUNT(*) AS cnt
                FROM checked_logs
                WHERE batch_id = %s
                GROUP BY worker_username
                ORDER BY cnt DESC;
            """, (bid,))
            per_user = cur.fetchall()
            if per_user:
                f.write("  • Workers:\n")
                for wk, cnt in per_user:
                    f.write(f"    – {wk}: {cnt}\n")
            else:
                f.write("  • Workers: None\n")
            f.write("\n")

    cur.close()
    conn.close()
    await query.message.reply_document(document=InputFile(filepath), filename=filename)

# ------------------------
# 11) DOWNLOAD MENU HELPERS
# ------------------------
def format_date_label(dt_obj):
    return dt_obj.strftime("%m-%d")

async def download_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to download batches.")
        return

    conn = get_db_connection()
    cur = conn.cursor()
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

    dates = [r[0] for r in rows]
    to_show = dates[:4]
    buttons = []
    for dt in to_show:
        lbl = format_date_label(dt)
        data = f"date|{dt.strftime('%Y-%m-%d')}"
        buttons.append(InlineKeyboardButton(lbl, callback_data=data))

    keyboard = []
    for i in range(0, len(buttons), 2):
        keyboard.append(buttons[i:i+2])

    await update.message.reply_text("Select a date:", reply_markup=InlineKeyboardMarkup(keyboard))

    if len(dates) > 4:
        await update.message.reply_text(
            "There are more dates than shown. To see batches on another date, type the date in one of these formats:\n"
            "  • `MM/DD`  (e.g. `06/02`)\n"
            "  • `DD.MM`  (e.g. `02.06`)\n"
            "  • `MonthName D`  (e.g. `June 2`)\n"
            "I will ignore the year you type."
        )
        download_date_prompt[user_id] = True

async def handle_date_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in download_date_prompt:
        return

    text = update.message.text.strip()
    try:
        if "/" in text:
            parts = text.split("/")
            month = int(parts[0])
            day = int(parts[1])
        elif "." in text:
            parts = text.split(".")
            month = int(parts[1])
            day = int(parts[0])
        else:
            parts = text.replace(",", "").split()
            month_str = parts[0]
            if month_str.isdigit():
                month = int(month_str)
            else:
                dt = datetime.strptime(month_str, "%B")
                month = dt.month
            day = int(parts[1])

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

    except:
        await update.message.reply_text(
            "Could not parse that date. Please use formats like `6/2`, `02.06`, or `June 2`."
        )

async def send_batch_menu(update_obj, context: ContextTypes.DEFAULT_TYPE, batch_ids):
    user_id = update_obj.effective_user.id
    download_batch_prompt[user_id] = batch_ids

    to_show = batch_ids[:4]
    buttons = [InlineKeyboardButton(f"Batch {bid}", callback_data=f"batch|{bid}") for bid in to_show]
    keyboard = []
    for i in range(0, len(buttons), 2):
        keyboard.append(buttons[i:i+2])

    await update_obj.message.reply_text("Select a batch:", reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_download_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    if data.startswith("date|"):
        chosen_date = data.split("|", 1)[1]
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
        batch_id = int(data.split("|", 1)[1])
        await send_checked_logs_file(query, context, batch_id)

async def send_checked_logs_file(trigger, context: ContextTypes.DEFAULT_TYPE, batch_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT total_lines FROM batches WHERE id = %s;", (batch_id,))
    total_lines = cur.fetchone()[0]

    cur.execute("""
        SELECT log_text, result_text, worker_username, checked_at
        FROM checked_logs
        WHERE batch_id = %s
        ORDER BY checked_at;
    """, (batch_id,))
    entries = cur.fetchall()

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

    processed = len(entries)

    # Send summary message first
    summary_msg = f"📊 {processed}/{total_lines} done.\n"
    for wk, cnt in per_user:
        summary_msg += f"{wk}: {cnt} replies\n"
    await trigger.message.reply_text(summary_msg)

    tmp_dir = tempfile.mkdtemp()
    filename = f"batch_{batch_id}_checked.txt"
    filepath = os.path.join(tmp_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(f"Total workers: {len(per_user)}\n")
        for log_text, result_text, worker_username, checked_at in entries:
            # Normalize result_text to ensure it starts with $
            bal_match = BALANCE_RE.search(result_text)
            if bal_match:
                bal = bal_match.group().strip()
            else:
                bal = "0"
            if not bal.startswith("$"):
                bal = f"${bal}"
            f.write(f"{log_text} | Balance ={bal}\n")

    await trigger.message.reply_document(document=InputFile(filepath), filename=filename)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id in download_date_prompt:
        await handle_date_text(update, context)

async def cmdlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
        "    – Show all batches with progress and “Download Finished Summaries”.\n\n"
        "• `/download`\n"
        "    – Download checked logs by date → batch (each as `.txt`).\n\n"
        "• `/cmdlist`\n"
        "    – Show this list of admin commands.\n\n"
        "Notes:\n"
        "- To upload a new batch, send a `.txt` file in this chat.\n"
        "- Invalid lines returned immediately in `invalid_lines.txt`.\n"
        "- Workers use `/next` to get logs and reply with balance.\n"
        "- Logs with balance > $100 will ping @manipulation in the group.\n"
    )
    await update.message.reply_text(msg, parse_mode="Markdown")

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    authorized, role = is_user_authorized(user_id)
    if not authorized or role != "admin":
        await update.message.reply_text("❌ You are not authorized to upload files.")
        return

    doc = update.message.document
    if not doc or not doc.file_name.lower().endswith(".txt"):
        await update.message.reply_text("Please upload a `.txt` file.")
        return

    file = await context.bot.get_file(doc.file_id)
    tmp_dir = tempfile.mkdtemp()
    local_path = os.path.join(tmp_dir, doc.file_name)
    await file.download_to_drive(custom_path=local_path)

    with open(local_path, "r", encoding="utf-8") as f:
        raw_lines = [line.strip() for line in f if line.strip()]
    good, bad = clean_lines(raw_lines)

    batch_id = create_new_batch(len(raw_lines))

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
# 15) MAIN: SET UP BOT
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
    app.add_handler(CallbackQueryHandler(handle_queue_callback, pattern="^download_finished$"))
    app.add_handler(CommandHandler("download", download_handler))
    app.add_handler(CallbackQueryHandler(handle_download_callback, pattern="^(date\\||batch\\|)"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CommandHandler("cmdlist", cmdlist))

    # File upload (new batch)
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))

    # Schedule deletion & expiration
    app.job_queue.run_repeating(process_pending_deletions, interval=60, first=10)

    # Register “/” commands so Telegram shows a menu
    async def set_commands(application):
        await application.bot.set_my_commands([
            BotCommand("start",        "Get the next log (workers only)"),
            BotCommand("add_user",     "Add or update a user (admin only)"),
            BotCommand("list_workers", "List all registered users (admin only)"),
            BotCommand("stats",        "Show overall metrics (admin only)"),
            BotCommand("queue",        "Show all batches + download finished summaries"),
            BotCommand("download",     "Download checked logs by date → batch (admin only)"),
            BotCommand("cmdlist",      "Show a list of all admin commands (admin only)")
        ])
    app.post_init = set_commands

    app.run_polling()

if __name__ == "__main__":
    main()
