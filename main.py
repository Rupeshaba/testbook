import json
import requests
import re
import sqlite3
from flask import Flask, render_template, request, jsonify, Response
import threading
import time
from datetime import datetime, timedelta
from io import StringIO
from flask_socketio import SocketIO

app = Flask(__name__)
app.config['SECRET_KEY'] = 'change-this'
socketio = SocketIO(app, cors_allowed_origins="*")

# ------------------ Globals ------------------
task_progress = {}
runner_lock = threading.Lock()
stop_event = threading.Event()

runner_state = {
    "status": "stopped",         # stopped | running | paused
    "cycle": 0,
    "last_message": "",
    "current": 0,
    "total": 0,
    "started_at": None,
    "finished_at": None
}

DEFAULT_CONFIG = {
    "chunk_size": 500,
    "max_retries": 2,
    "cycle_interval_sec": 300,   # 5 min
    "auto_restart": True,
    "throttle_ms": 100,
    # Telegram
    "telegram_enabled": False,
    "telegram_bot_token": "",
    "telegram_chat_id": ""
}

# ------------------ DB Helpers ------------------
def db_conn():
    return sqlite3.connect('invites.db', check_same_thread=False)

def setup_database():
    conn = db_conn()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS invites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL,
            retry_count INTEGER DEFAULT 0,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    ''')
    for k, v in DEFAULT_CONFIG.items():
        cur.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, json.dumps(v)))
    conn.commit()
    conn.close()

def get_config():
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT key, value FROM settings")
    rows = cur.fetchall()
    cfg = DEFAULT_CONFIG.copy()
    for k, v in rows:
        try:
            cfg[k] = json.loads(v)
        except Exception:
            cfg[k] = v
    conn.close()
    return cfg

def save_config(partial):
    conn = db_conn()
    cur = conn.cursor()
    for k, v in partial.items():
        cur.execute("REPLACE INTO settings (key, value) VALUES (?, ?)", (k, json.dumps(v)))
    conn.commit()
    conn.close()

# ------------------ Telegram ------------------
def telegram_enabled(cfg=None):
    cfg = cfg or get_config()
    return bool(cfg.get("telegram_enabled")) and bool(cfg.get("telegram_bot_token")) and bool(cfg.get("telegram_chat_id"))

def send_telegram_message(text, cfg=None):
    """Fire-and-forget Telegram notify; ignores errors (safe for background)."""
    cfg = cfg or get_config()
    if not telegram_enabled(cfg):
        return
    try:
        url = f"https://api.telegram.org/bot{cfg['telegram_bot_token']}/sendMessage"
        payload = {
            "chat_id": cfg["telegram_chat_id"],
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        # short timeout so it never blocks the loop
        requests.post(url, json=payload, timeout=10)
    except Exception:
        pass  # swallow any network/format issues silently

def notify_cycle_start():
    cfg = get_config()
    if not telegram_enabled(cfg): return
    msg = f"▶️ <b>Cycle #{runner_state['cycle']}</b> started\n" \
          f"⏳ Total planned: <b>{runner_state.get('total',0)}</b>\n" \
          f"🕒 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
    send_telegram_message(msg, cfg)

def notify_cycle_end():
    cfg = get_config()
    if not telegram_enabled(cfg): return
    msg = f"✅ <b>Cycle #{runner_state['cycle']}</b> finished\n" \
          f"📦 Processed: <b>{runner_state.get('current',0)}/{runner_state.get('total',0)}</b>\n" \
          f"🕒 {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
    send_telegram_message(msg, cfg)

def notify_error(err_text):
    cfg = get_config()
    if not telegram_enabled(cfg): return
    send_telegram_message(f"❌ <b>Error</b>: {err_text}", cfg)

# ------------------ Invite Logging ------------------
def log_invite_status(conn, email, status):
    cur = conn.cursor()
    if status in ('failed', 'failed_connection', 'already_registered_failed'):
        cur.execute("SELECT retry_count FROM invites WHERE email = ?", (email,))
        row = cur.fetchone()
        retry_count = (row[0] + 1) if row else 1
        cur.execute('''
            INSERT INTO invites (email, status, retry_count, timestamp)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(email) DO UPDATE SET status = ?, retry_count = ?, timestamp = CURRENT_TIMESTAMP
        ''', (email, status, retry_count, status, retry_count))
    else:
        cur.execute('''
            INSERT INTO invites (email, status, retry_count, timestamp)
            VALUES (?, ?, COALESCE((SELECT retry_count FROM invites WHERE email=?),0), CURRENT_TIMESTAMP)
            ON CONFLICT(email) DO UPDATE SET status = ?, timestamp = CURRENT_TIMESTAMP
        ''', (email, status, email, status))
    conn.commit()

# ------------------ Request Parsing / Emails ------------------
def parse_requests(filename="requests.txt"):
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    blocks = content.split('Request URL')
    if len(blocks) < 3:
        raise ValueError("Could not find the 2nd request in requests.txt")
    second = blocks[2]
    url_match = re.search(r'https://[^\s]+', "Request URL" + second)
    url = url_match.group(0) if url_match else None
    headers = {}
    for line in second.split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            headers[key.strip().lower()] = value.strip()
    payload_section = second.split('Payload :-')
    payload = {}
    if len(payload_section) > 1:
        for line in payload_section[1].strip().split('\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                payload[key.strip()] = value.strip()
    return {"url": url, "headers": headers, "payload_template": payload}

def load_all_emails(filename="emails.json"):
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    out = []
    for item in data:
        if isinstance(item, str):
            out.append({"email": item})
        elif isinstance(item, dict) and 'email' in item:
            out.append({"email": item['email']})
    return out

def get_emails_to_process(conn, all_emails, max_retries):
    cur = conn.cursor()
    cur.execute("SELECT email, status, retry_count FROM invites")
    db_emails = {r[0]: {'status': r[1], 'retry_count': r[2]} for r in cur.fetchall()}

    todo = []
    for item in all_emails:
        email = item.get('email')
        if not email:
            continue
        if email not in db_emails:
            todo.append(item)
        else:
            st = db_emails[email]['status']
            rc = db_emails[email]['retry_count'] or 0
            if st in ('failed', 'failed_connection') and rc < max_retries:
                todo.append(item)
    return todo

def send_invites_chunk(conn, request_info, email_data):
    url = request_info['url']
    headers = request_info['headers']
    emails = [e['email'] for e in email_data if 'email' in e]
    if not emails:
        return

    cleaned_headers = {
        'accept': headers.get('accept'),
        'accept-language': headers.get('accept-language'),
        'content-type': headers.get('content-type', 'application/json'),
        'origin': headers.get('origin'),
        'referer': headers.get('referer'),
        'sec-ch-ua': headers.get('sec-ch-ua'),
        'sec-ch-ua-mobile': headers.get('sec-ch-ua-mobile'),
        'sec-ch-ua-platform': headers.get('sec-ch-ua-platform'),
        'sec-fetch-dest': headers.get('sec-fetch-dest'),
        'sec-fetch-mode': headers.get('sec-fetch-mode'),
        'sec-fetch-site': headers.get('sec-fetch-site'),
        'source': headers.get('source'),
        'user-agent': headers.get('user-agent'),
    }

    try:
        resp = requests.post(url, headers={k:v for k,v in cleaned_headers.items() if v}, json={"emails": emails}, timeout=30)
        status_code = resp.status_code
        ok = (status_code == 200)
        text = resp.text.lower()
        already_phrase = ("already registered" in text) or ("already_registered" in text)
        for email in emails:
            if ok:
                log_invite_status(conn, email, 'Sent')
            else:
                if already_phrase:
                    log_invite_status(conn, email, 'already_registered_failed')
                else:
                    log_invite_status(conn, email, 'failed')
    except requests.exceptions.RequestException as e:
        for email in emails:
            log_invite_status(conn, email, 'failed_connection')

# ------------------ Background Loop ------------------
def background_loop():
    while not stop_event.is_set():
        if runner_state['status'] == 'paused':
            time.sleep(0.3); continue
        if runner_state['status'] != 'running':
            time.sleep(0.5); continue

        cfg = get_config()
        chunk_size = int(cfg['chunk_size'])
        max_retries = int(cfg['max_retries'])
        throttle_ms = int(cfg['throttle_ms'])

        conn = db_conn()
        try:
            request_info = parse_requests("requests.txt")
            all_emails = load_all_emails("emails.json")
            todo = get_emails_to_process(conn, all_emails, max_retries)
            total = len(todo)

            with runner_lock:
                runner_state.update({
                    "cycle": runner_state['cycle'] + 1,
                    "current": 0,
                    "total": total,
                    "started_at": datetime.utcnow().isoformat(),
                    "finished_at": None,
                    "last_message": f"Starting cycle #{runner_state['cycle']}"
                })
            socketio.emit('progress_update', {"type":"cycle_start", **runner_state})
            notify_cycle_start()

            if total == 0:
                with runner_lock:
                    runner_state['last_message'] = "No emails to process (all done or retries exhausted)."
                socketio.emit('progress_update', {"type":"idle", **runner_state})
            else:
                for i in range(0, total, chunk_size):
                    if stop_event.is_set() or runner_state['status'] != 'running':
                        break
                    chunk = todo[i:i+chunk_size]
                    send_invites_chunk(conn, request_info, chunk)
                    with runner_lock:
                        runner_state['current'] = min(total, runner_state['current'] + len(chunk))
                        runner_state['last_message'] = f"Processed {runner_state['current']}/{total}"
                    socketio.emit('progress_update', {"type":"progress", **runner_state})
                    time.sleep(throttle_ms/1000.0)

            with runner_lock:
                runner_state['finished_at'] = datetime.utcnow().isoformat()
                runner_state['last_message'] = "Cycle finished."
            socketio.emit('progress_update', {"type":"cycle_end", **runner_state})
            notify_cycle_end()

        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            with runner_lock:
                runner_state['last_message'] = f"Error in loop: {err}"
            socketio.emit('progress_update', {"type":"error", "message": err, **runner_state})
            notify_error(err)
        finally:
            conn.close()

        cfg = get_config()
        if cfg.get('auto_restart', True) and runner_state['status'] == 'running' and not stop_event.is_set():
            secs = int(cfg.get('cycle_interval_sec', 300))
            for _ in range(secs * 10):
                if stop_event.is_set() or runner_state['status'] != 'running':
                    break
                with runner_lock:
                    runner_state['last_message'] = f"Waiting {secs}s for next cycle…"
                socketio.emit('progress_update', {"type":"waiting", **runner_state})
                time.sleep(0.1)
        else:
            with runner_lock:
                if runner_state['status'] == 'running':
                    runner_state['status'] = 'stopped'
            socketio.emit('progress_update', {"type":"stopped", **runner_state})

threading.Thread(target=background_loop, daemon=True).start()

# ------------------ Legacy endpoints (kept) ------------------
@app.route('/send_invites', methods=['POST'])
def handle_send_invites():
    save_config({"auto_restart": False})
    with runner_lock:
        runner_state['status'] = 'running'
    return jsonify({"task_id": "socketio-live", "message": "Manual run started."})

@app.route('/progress/<task_id>')
def get_progress(task_id):
    info = task_progress.get(task_id, {'total': runner_state['total'], 'current': runner_state['current'],
                                       'status': runner_state['status'], 'message': runner_state['last_message']})
    return jsonify(info)

# ------------------ Control & Settings ------------------
@app.route('/control', methods=['POST'])
def control():
    data = request.get_json(silent=True) or {}
    action = data.get('action')
    if 'auto_restart' in data:
        save_config({"auto_restart": bool(data['auto_restart'])})
    with runner_lock:
        if action == 'start':
            runner_state['status'] = 'running'
        elif action == 'pause':
            runner_state['status'] = 'paused'
        elif action == 'resume':
            runner_state['status'] = 'running'
        elif action == 'stop':
            runner_state['status'] = 'stopped'
        else:
            return jsonify({"ok": False, "error": "Unknown action"}), 400
    socketio.emit('progress_update', {"type":"control", "action": action, **runner_state})
    return jsonify({"ok": True, "state": runner_state})

@app.route('/settings', methods=['POST'])
def update_settings():
    data = request.get_json(silent=True) or {}
    for k in ["chunk_size","max_retries","cycle_interval_sec","throttle_ms"]:
        if k in data:
            try:
                data[k] = int(data[k])
            except:
                return jsonify({"ok": False, "error": f"{k} must be int"}), 400
    if 'telegram_enabled' in data:
        data['telegram_enabled'] = bool(data['telegram_enabled'])
    save_config(data)
    return jsonify({"ok": True, "config": get_config()})

# ------------------ Actions ------------------
@app.route('/resend', methods=['POST'])
def resend_one():
    email = request.args.get('email')
    if not email:
        return jsonify({"ok": False, "error": "email required"}), 400
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("SELECT retry_count FROM invites WHERE email=?", (email,))
    row = cur.fetchone()
    rc = (row[0] if row else 0) + 1
    cur.execute('''
        INSERT INTO invites (email, status, retry_count, timestamp)
        VALUES (?, 'Pending', ?, CURRENT_TIMESTAMP)
        ON CONFLICT(email) DO UPDATE SET status='Pending', retry_count=?, timestamp=CURRENT_TIMESTAMP
    ''', (email, rc, rc))
    conn.commit()
    conn.close()
    socketio.emit('progress_update', {"type":"resend", "email": email})
    return jsonify({"ok": True})

@app.route('/bulk_action', methods=['POST'])
def bulk_action():
    data = request.get_json(silent=True) or {}
    action = data.get('action')
    emails = data.get('emails', [])
    if action not in ('mark_sent','mark_failed','resend') or not emails:
        return jsonify({"ok": False, "error": "invalid payload"}), 400

    conn = db_conn()
    cur = conn.cursor()
    if action == 'mark_sent':
        for e in emails:
            cur.execute('''
                INSERT INTO invites (email, status, timestamp)
                VALUES (?, 'Sent', CURRENT_TIMESTAMP)
                ON CONFLICT(email) DO UPDATE SET status='Sent', timestamp=CURRENT_TIMESTAMP
            ''', (e,))
    elif action == 'mark_failed':
        for e in emails:
            cur.execute("SELECT retry_count FROM invites WHERE email=?", (e,))
            row = cur.fetchone()
            rc = (row[0] if row else 0) + 1
            cur.execute('''
                INSERT INTO invites (email, status, retry_count, timestamp)
                VALUES (?, 'failed', ?, CURRENT_TIMESTAMP)
                ON CONFLICT(email) DO UPDATE SET status='failed', retry_count=?, timestamp=CURRENT_TIMESTAMP
            ''', (e, rc, rc))
    else:
        for e in emails:
            cur.execute("SELECT retry_count FROM invites WHERE email=?", (e,))
            row = cur.fetchone()
            rc = (row[0] if row else 0) + 1
            cur.execute('''
                INSERT INTO invites (email, status, retry_count, timestamp)
                VALUES (?, 'Pending', ?, CURRENT_TIMESTAMP)
                ON CONFLICT(email) DO UPDATE SET status='Pending', retry_count=?, timestamp=CURRENT_TIMESTAMP
            ''', (e, rc, rc))
    conn.commit()
    conn.close()
    socketio.emit('progress_update', {"type":"bulk", "action": action, "count": len(emails)})
    return jsonify({"ok": True, "count": len(emails)})

# ------------------ Export CSV ------------------
@app.route('/export_csv')
def export_csv():
    status_filter = request.args.get('status_filter', 'all')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    conn = db_conn()
    cur = conn.cursor()
    sql = "SELECT email, status, retry_count, timestamp FROM invites WHERE 1=1"
    params = []
    if status_filter != 'all':
        sql += " AND status = ?"; params.append(status_filter)
    if start_date:
        sql += " AND timestamp >= ?"; params.append(start_date + " 00:00:00")
    if end_date:
        sql += " AND timestamp < ?"
        d2 = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        params.append(d2 + " 00:00:00")
    sql += " ORDER BY timestamp DESC"
    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()

    out = StringIO()
    out.write("Email,Status,Retry Count,Timestamp\n")
    for r in rows:
        out.write(f"{r[0]},{r[1]},{r[2]},{r[3]}\n")
    out.seek(0)
    return Response(out.getvalue(),
                    mimetype='text/csv',
                    headers={"Content-Disposition": "attachment;filename=invite_report.csv"})

# ------------------ View ------------------
@app.route('/')
def index():
    conn = db_conn()
    cur = conn.cursor()
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 25, type=int)
    status_filter = request.args.get('status_filter', 'all')
    start_date = request.args.get('start_date', '')
    end_date = request.args.get('end_date', '')

    count_sql = "SELECT status, COUNT(*) FROM invites WHERE 1=1"
    count_params = []
    if start_date:
        count_sql += " AND timestamp >= ?"; count_params.append(start_date + " 00:00:00")
    if end_date:
        count_sql += " AND timestamp < ?"
        d2 = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        count_params.append(d2 + " 00:00:00")
    count_sql += " GROUP BY status"
    cur.execute(count_sql, count_params)
    status_counts = dict(cur.fetchall())

    list_sql = "SELECT email, status, retry_count, timestamp FROM invites WHERE 1=1"
    list_params = []
    if status_filter != 'all':
        list_sql += " AND status = ?"; list_params.append(status_filter)
    if start_date:
        list_sql += " AND timestamp >= ?"; list_params.append(start_date + " 00:00:00")
    if end_date:
        list_sql += " AND timestamp < ?"
        d2 = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        list_params.append(d2 + " 00:00:00")
    list_sql += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
    list_params.extend([per_page, (page-1)*per_page])
    cur.execute(list_sql, list_params)
    all_invites = [{"email": r[0], "status": r[1], "retry_count": r[2], "timestamp": r[3]} for r in cur.fetchall()]

    total_sql = "SELECT COUNT(*) FROM invites WHERE 1=1"
    total_params = []
    if status_filter != 'all':
        total_sql += " AND status = ?"; total_params.append(status_filter)
    if start_date:
        total_sql += " AND timestamp >= ?"; total_params.append(start_date + " 00:00:00")
    if end_date:
        total_sql += " AND timestamp < ?"
        d2 = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        total_params.append(d2 + " 00:00:00")
    cur.execute(total_sql, total_params)
    total_invites = cur.fetchone()[0]
    total_pages = (total_invites + per_page - 1)//per_page

    cfg = get_config()
    try:
        all_emails = load_all_emails("emails.json")
        pending = get_emails_to_process(db_conn(), all_emails, int(cfg['max_retries']))
        queue_count = len(pending)
    except Exception:
        queue_count = 0

    conn.close()
    return render_template(
        'index.html',
        status_counts=status_counts,
        all_invites=all_invites,
        page=page,
        per_page=per_page,
        status_filter=status_filter,
        total_pages=total_pages,
        total_invites=total_invites,
        start_date=start_date,
        end_date=end_date,
        runner_state=runner_state,
        config=cfg,
        queue_count=queue_count
    )

if __name__ == "__main__":
    setup_database()
    socketio.run(app, debug=True)
