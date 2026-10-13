#!/usr/bin/env python3
# ClipHouse — local-network text clipboard with SQLite history.
# Python standard library only; designed for trusted home networks.

import configparser
import ctypes
import json
import os
import re
import socket
import sqlite3
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

APP_NAME = "ClipHouse"
APP_VERSION = 5
APP_DISPLAY_VERSION = "4.2.0"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Optional paths let the same program run in a persistent Home Assistant app container.
CONFIG_PATH = os.path.abspath(os.environ.get("CLIPHOUSE_CONFIG_PATH", os.path.join(BASE_DIR, "config.ini")))
APP_STATE_DIR = os.path.abspath(os.environ.get(
    "CLIPHOUSE_STATE_DIR",
    os.path.join(os.environ.get("LOCALAPPDATA", os.path.expanduser("~")), APP_NAME),
))
NODE_PATH = os.path.join(APP_STATE_DIR, "node-id.txt")
MAX_REQUEST_BYTES = 16 * 1024 * 1024
PAGE_SIZE = 50
SYNC_BATCH_SIZE = 25
POLL_SECONDS = 2.5
PEER_TIMEOUT = 25.0
HOST_NAME = socket.gethostname()
NODE_ID = ""


def _write_default_config():
    cfg = configparser.ConfigParser(interpolation=None)
    cfg["server"] = {"http_port": "8787", "discovery_port": "8788"}
    default_history = r"%LOCALAPPDATA%\ClipHouse" if os.name == "nt" and not os.environ.get("CLIPHOUSE_STATE_DIR") else APP_STATE_DIR
    cfg["storage"] = {"history_dir": default_history}
    cfg["clipboard"] = {"auto_capture": "true", "max_text_chars": "100000"}
    cfg["web"] = {"local_name": "clipboard"}
    with open(CONFIG_PATH, "w", encoding="utf-8-sig", newline="") as f:
        cfg.write(f)


def _expand_path(raw_value, fallback):
    raw_value = (raw_value or "").strip().strip('"')
    if not raw_value:
        return fallback
    expanded = os.path.expandvars(os.path.expanduser(raw_value))

    def replace_percent(match):
        return os.environ.get(match.group(1), match.group(0))

    expanded = re.sub(r"%([A-Za-z_][A-Za-z0-9_]*)%", replace_percent, expanded)
    if re.search(r"%[A-Za-z_][A-Za-z0-9_]*%", expanded):
        print("[ClipHouse] Не удалось раскрыть переменную в history_dir; используется путь по умолчанию.")
        return fallback
    if not os.path.isabs(expanded):
        expanded = os.path.join(BASE_DIR, expanded)
    return os.path.abspath(expanded)


def _read_config():
    if not os.path.isfile(CONFIG_PATH):
        try:
            _write_default_config()
            print("[ClipHouse] Создан файл настроек: %s" % CONFIG_PATH)
        except OSError as exc:
            print("[ClipHouse] Не удалось создать config.ini: %s" % exc)

    cfg = configparser.ConfigParser(interpolation=None)
    try:
        cfg.read(CONFIG_PATH, encoding="utf-8-sig")
    except (OSError, configparser.Error) as exc:
        print("[ClipHouse] Ошибка config.ini, используются настройки по умолчанию: %s" % exc)

    def get_int(section, key, default, low, high):
        try:
            value = cfg.getint(section, key, fallback=default)
        except (ValueError, configparser.Error):
            value = default
        return max(low, min(high, value))

    def get_bool(section, key, default):
        try:
            return cfg.getboolean(section, key, fallback=default)
        except (ValueError, configparser.Error):
            return default

    default_data_dir = APP_STATE_DIR
    raw_history_dir = cfg.get("storage", "history_dir", fallback=default_data_dir)
    history_dir = _expand_path(raw_history_dir, default_data_dir)
    local_name = cfg.get("web", "local_name", fallback="clipboard").strip().strip(".")
    # Only use a hostname-like label, not a URL/path. The router DNS record is configured separately.
    if not local_name or len(local_name) > 253 or not re.fullmatch(r"[A-Za-z0-9.-]+", local_name) or ".." in local_name:
        local_name = "clipboard"
    return {
        "http_port": get_int("server", "http_port", 8787, 1, 65535),
        "discovery_port": get_int("server", "discovery_port", 8788, 1, 65535),
        "history_dir": history_dir,
        "auto_capture": get_bool("clipboard", "auto_capture", True),
        "max_text_chars": get_int("clipboard", "max_text_chars", 100000, 1, 1000000),
        "local_name": local_name,
    }


_SETTINGS = _read_config()
HTTP_PORT = _SETTINGS["http_port"]
DISCOVERY_PORT = _SETTINGS["discovery_port"]
DATA_DIR = _SETTINGS["history_dir"]
DATABASE_PATH = os.path.join(DATA_DIR, "history.sqlite3")
LEGACY_JSON_PATH = os.path.join(DATA_DIR, "history.json")
AUTO_CAPTURE_CLIPBOARD = _SETTINGS["auto_capture"]
MAX_TEXT_CHARS = _SETTINGS["max_text_chars"]
LOCAL_NAME = _SETTINGS["local_name"]

_db = None
_db_lock = threading.RLock()
_clip_lock = threading.Lock()
_peers_lock = threading.RLock()
_peers = {}  # node_id -> {url, name, ip, last_seen}
_sync_error_log = {}  # node_id -> last monotonic timestamp when an error was printed
_suppressed_clipboard = {}  # exact clipboard text -> monotonic expiry
_http_server = None
_FTS_ENABLED = False


def _ensure_state_dir():
    os.makedirs(APP_STATE_DIR, exist_ok=True)


def _load_node_id():
    # Node identity remains stable even if the database is moved to another disk.
    _ensure_state_dir()
    try:
        with open(NODE_PATH, "r", encoding="utf-8") as f:
            value = f.read().strip()
        if value:
            return value
    except OSError:
        pass
    value = str(uuid.uuid4())
    try:
        with open(NODE_PATH, "w", encoding="utf-8") as f:
            f.write(value)
    except OSError as exc:
        print("[ClipHouse] Не удалось сохранить ID компьютера: %s" % exc)
    return value


def _clean_event(raw):
    if not isinstance(raw, dict):
        return None
    text = raw.get("text")
    ident = raw.get("id")
    ts = raw.get("ts")
    if not isinstance(text, str) or not isinstance(ident, str):
        return None
    if not text.strip() or len(text) > MAX_TEXT_CHARS or not ident or len(ident) > 100:
        return None
    try:
        ts = int(ts)
    except (TypeError, ValueError):
        return None
    return {
        "id": ident,
        "ts": ts,
        "text": text,
        "device": str(raw.get("device", "Компьютер"))[:100],
        "node": str(raw.get("node", ""))[:100],
        "source": str(raw.get("source", ""))[:30],
    }


def _event_from_row(row):
    result = {
        "id": row["id"],
        "ts": int(row["ts"]),
        "text": row["text"],
        "device": row["device"],
        "node": row["node"],
        "source": row["source"],
    }
    # History queries include a left-joined favorite state; sync/latest queries may not.
    if "favorite" in row.keys():
        result["favorite"] = bool(row["favorite"])
    return result


def _initialize_database():
    global _db, _FTS_ENABLED, DATA_DIR, DATABASE_PATH, LEGACY_JSON_PATH
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except OSError as exc:
        fallback = os.path.join(APP_STATE_DIR, "History")
        print("[ClipHouse] Не удалось создать папку истории (%s): %s" % (DATA_DIR, exc))
        print("[ClipHouse] Используется запасная папка: %s" % fallback)
        DATA_DIR = fallback
        DATABASE_PATH = os.path.join(DATA_DIR, "history.sqlite3")
        LEGACY_JSON_PATH = os.path.join(DATA_DIR, "history.json")
        os.makedirs(DATA_DIR, exist_ok=True)

    _db = sqlite3.connect(DATABASE_PATH, timeout=30, check_same_thread=False)
    _db.row_factory = sqlite3.Row
    with _db_lock:
        _db.execute("PRAGMA journal_mode=WAL")
        _db.execute("PRAGMA synchronous=NORMAL")
        _db.execute("PRAGMA busy_timeout=30000")
        _db.execute("""CREATE TABLE IF NOT EXISTS entries (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT NOT NULL UNIQUE,
            ts INTEGER NOT NULL,
            text TEXT NOT NULL,
            device TEXT NOT NULL DEFAULT 'Компьютер',
            node TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT ''
        )""")
        _db.execute("CREATE INDEX IF NOT EXISTS idx_entries_time ON entries(ts DESC, seq DESC)")
        _db.execute("""CREATE TABLE IF NOT EXISTS deletions (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT NOT NULL UNIQUE,
            deleted_ts INTEGER NOT NULL,
            device TEXT NOT NULL DEFAULT 'Компьютер',
            node TEXT NOT NULL DEFAULT ''
        )""")
        _db.execute("CREATE INDEX IF NOT EXISTS idx_deletions_time ON deletions(deleted_ts DESC, seq DESC)")
        # Favorite state is kept separately from clipboard text. The event log lets
        # every node forward star/unstar changes to its other peers.
        _db.execute("""CREATE TABLE IF NOT EXISTS favorite_state (
            entry_id TEXT PRIMARY KEY,
            is_favorite INTEGER NOT NULL,
            changed_ts INTEGER NOT NULL,
            device TEXT NOT NULL DEFAULT 'Компьютер',
            node TEXT NOT NULL DEFAULT '',
            event_id TEXT NOT NULL
        )""")
        _db.execute("""CREATE TABLE IF NOT EXISTS favorite_events (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            entry_id TEXT NOT NULL,
            is_favorite INTEGER NOT NULL,
            changed_ts INTEGER NOT NULL,
            device TEXT NOT NULL DEFAULT 'Компьютер',
            node TEXT NOT NULL DEFAULT ''
        )""")
        _db.execute("CREATE INDEX IF NOT EXISTS idx_favorite_events_entry ON favorite_events(entry_id, seq)")
        _db.execute("CREATE INDEX IF NOT EXISTS idx_favorite_state_flag ON favorite_state(is_favorite, changed_ts DESC)")
        _db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        _db.execute("""CREATE TABLE IF NOT EXISTS sync_cursors (
            peer_id TEXT PRIMARY KEY,
            last_seq INTEGER NOT NULL DEFAULT 0,
            last_delete_seq INTEGER NOT NULL DEFAULT 0,
            last_favorite_seq INTEGER NOT NULL DEFAULT 0,
            last_push_seq INTEGER NOT NULL DEFAULT 0,
            last_push_delete_seq INTEGER NOT NULL DEFAULT 0,
            last_push_favorite_seq INTEGER NOT NULL DEFAULT 0
        )""")
        # Upgrade databases from older ClipHouse versions without discarding cursors.
        cursor_columns = {row[1] for row in _db.execute("PRAGMA table_info(sync_cursors)").fetchall()}
        if "last_delete_seq" not in cursor_columns:
            _db.execute("ALTER TABLE sync_cursors ADD COLUMN last_delete_seq INTEGER NOT NULL DEFAULT 0")
        if "last_favorite_seq" not in cursor_columns:
            _db.execute("ALTER TABLE sync_cursors ADD COLUMN last_favorite_seq INTEGER NOT NULL DEFAULT 0")
        if "last_push_seq" not in cursor_columns:
            _db.execute("ALTER TABLE sync_cursors ADD COLUMN last_push_seq INTEGER NOT NULL DEFAULT 0")
        if "last_push_delete_seq" not in cursor_columns:
            _db.execute("ALTER TABLE sync_cursors ADD COLUMN last_push_delete_seq INTEGER NOT NULL DEFAULT 0")
        if "last_push_favorite_seq" not in cursor_columns:
            _db.execute("ALTER TABLE sync_cursors ADD COLUMN last_push_favorite_seq INTEGER NOT NULL DEFAULT 0")
        # Cache the row count so the home page does not scan a giant history table
        # every 2.5 seconds just to display the count.
        has_count = _db.execute("SELECT 1 FROM meta WHERE key='entries_count'").fetchone()
        if not has_count:
            initial_count = int(_db.execute("SELECT COUNT(*) FROM entries").fetchone()[0])
            _db.execute("INSERT INTO meta(key,value) VALUES('entries_count',?)", (str(initial_count),))
        _db.execute("""CREATE TRIGGER IF NOT EXISTS entries_count_ai AFTER INSERT ON entries BEGIN
            UPDATE meta SET value=CAST(value AS INTEGER)+1 WHERE key='entries_count';
        END""")
        _db.execute("""CREATE TRIGGER IF NOT EXISTS entries_count_ad AFTER DELETE ON entries BEGIN
            UPDATE meta SET value=MAX(0,CAST(value AS INTEGER)-1) WHERE key='entries_count';
        END""")
        _db.commit()

        # FTS5 makes word/phrase search fast. If this Python build lacks FTS5,
        # ClipHouse automatically falls back to SQLite LIKE search.
        existing_fts = _db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='entries_fts'").fetchone()
        try:
            _db.execute("CREATE VIRTUAL TABLE IF NOT EXISTS entries_fts USING fts5(text, content='entries', content_rowid='seq')")
            _db.execute("""CREATE TRIGGER IF NOT EXISTS entries_ai AFTER INSERT ON entries BEGIN
                INSERT INTO entries_fts(rowid, text) VALUES (new.seq, new.text);
            END""")
            _db.execute("""CREATE TRIGGER IF NOT EXISTS entries_ad AFTER DELETE ON entries BEGIN
                INSERT INTO entries_fts(entries_fts, rowid, text) VALUES('delete', old.seq, old.text);
            END""")
            if not existing_fts:
                _db.execute("INSERT INTO entries_fts(entries_fts) VALUES('rebuild')")
            _db.commit()
            _FTS_ENABLED = True
        except sqlite3.OperationalError:
            _FTS_ENABLED = False
            print("[ClipHouse] FTS5 недоступен: поиск будет работать в обычном режиме.")
            _db.rollback()

        migrated = _db.execute("SELECT value FROM meta WHERE key='legacy_json_migrated'").fetchone()
        if not migrated:
            # Also check the old default folder. This lets a user choose a new
            # history_dir in config.ini before the very first SQLite launch.
            legacy_candidates = [LEGACY_JSON_PATH, os.path.join(APP_STATE_DIR, "history.json")]
            legacy_path = next((p for p in legacy_candidates if os.path.isfile(p)), None)
            if legacy_path:
                try:
                    with open(legacy_path, "r", encoding="utf-8") as f:
                        old_data = json.load(f)
                    imported = 0
                    if isinstance(old_data, list):
                        with _db:
                            for raw in old_data:
                                event = _clean_event(raw)
                                if not event:
                                    continue
                                cur = _db.execute(
                                    "INSERT OR IGNORE INTO entries(id,ts,text,device,node,source) VALUES(?,?,?,?,?,?)",
                                    (event["id"], event["ts"], event["text"], event["device"], event["node"], event["source"]),
                                )
                                imported += cur.rowcount
                    print("[ClipHouse] Перенесено из %s: %s записей." % (legacy_path, imported))
                    print("[ClipHouse] Исходный JSON сохранён как резервная копия, он не удалён.")
                except (OSError, ValueError, TypeError, sqlite3.Error) as exc:
                    print("[ClipHouse] Не удалось импортировать history.json: %s" % exc)
                    # Do not set the migration marker so it can retry after the issue is fixed.
                    return
            with _db:
                _db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('legacy_json_migrated','1')")


def _get_latest_locked():
    row = _db.execute("SELECT id,ts,text,device,node,source FROM entries ORDER BY ts DESC, seq DESC LIMIT 1").fetchone()
    return _event_from_row(row) if row else None


def _entry_count_locked():
    row = _db.execute("SELECT value FROM meta WHERE key='entries_count'").fetchone()
    if row:
        try:
            return max(0, int(row[0]))
        except (ValueError, TypeError):
            pass
    return int(_db.execute("SELECT COUNT(*) FROM entries").fetchone()[0])


def _insert_event_locked(event):
    # A permanent tombstone wins over an older copy arriving from an offline peer.
    deleted = _db.execute("SELECT 1 FROM deletions WHERE id=?", (event["id"],)).fetchone()
    if deleted:
        return False
    cur = _db.execute(
        "INSERT OR IGNORE INTO entries(id,ts,text,device,node,source) VALUES(?,?,?,?,?,?)",
        (event["id"], event["ts"], event["text"], event["device"], event["node"], event["source"]),
    )
    return cur.rowcount > 0


def _clean_deletion(raw):
    if not isinstance(raw, dict):
        return None
    ident = raw.get("id")
    try:
        deleted_ts = int(raw.get("deleted_ts", raw.get("ts", 0)))
    except (TypeError, ValueError):
        return None
    if not isinstance(ident, str) or not ident or len(ident) > 100 or deleted_ts <= 0:
        return None
    return {
        "id": ident,
        "deleted_ts": deleted_ts,
        "device": str(raw.get("device", "Компьютер"))[:100],
        "node": str(raw.get("node", ""))[:100],
    }


def _merge_deletions(incoming):
    """Apply replicated tombstones. Tombstones are retained so old records cannot reappear."""
    if not isinstance(incoming, list):
        return 0
    changed = 0
    with _db_lock, _db:
        for raw in incoming:
            deletion = _clean_deletion(raw)
            if not deletion:
                continue
            cur = _db.execute(
                "INSERT OR IGNORE INTO deletions(id,deleted_ts,device,node) VALUES(?,?,?,?)",
                (deletion["id"], deletion["deleted_ts"], deletion["device"], deletion["node"]),
            )
            changed += max(0, cur.rowcount)
            # Remove even if the tombstone already existed; this also repairs a stale reinsert.
            _db.execute("DELETE FROM entries WHERE id=?", (deletion["id"],))
            _db.execute("DELETE FROM favorite_state WHERE entry_id=?", (deletion["id"],))
    if changed:
        print("[ClipHouse] Получено удалений от другого узла: %s" % changed)
    return changed


def _delete_events(ids):
    """Delete records locally and create sync tombstones for every supplied ID."""
    if not isinstance(ids, list):
        raise ValueError("Ожидался список ID записей")
    unique_ids = []
    seen = set()
    for ident in ids[:100]:
        if isinstance(ident, str) and 0 < len(ident) <= 100 and ident not in seen:
            unique_ids.append(ident)
            seen.add(ident)
    if not unique_ids:
        raise ValueError("Не выбраны записи для удаления")

    deleted_entries = 0
    new_tombstones = 0
    now_ms = int(time.time() * 1000)
    with _db_lock, _db:
        for ident in unique_ids:
            cur = _db.execute(
                "INSERT OR IGNORE INTO deletions(id,deleted_ts,device,node) VALUES(?,?,?,?)",
                (ident, now_ms, HOST_NAME[:100], NODE_ID),
            )
            new_tombstones += max(0, cur.rowcount)
            cur = _db.execute("DELETE FROM entries WHERE id=?", (ident,))
            deleted_entries += max(0, cur.rowcount)
            _db.execute("DELETE FROM favorite_state WHERE entry_id=?", (ident,))
    return {"deleted": deleted_entries, "tombstones": new_tombstones}


def _set_favorite(entry_id, is_favorite):
    """Set favorite state locally and append a sync event. Returns new state."""
    if not isinstance(entry_id, str) or not entry_id or len(entry_id) > 100:
        raise ValueError("Некорректный ID записи")
    if not isinstance(is_favorite, bool):
        raise ValueError("is_favorite должен быть true или false")
    event_id = str(uuid.uuid4())
    with _db_lock, _db:
        if _db.execute("SELECT 1 FROM entries WHERE id=?", (entry_id,)).fetchone() is None:
            raise ValueError("Запись уже удалена или не найдена")
        if _db.execute("SELECT 1 FROM deletions WHERE id=?", (entry_id,)).fetchone():
            raise ValueError("Запись уже удалена")
        current = _db.execute("SELECT changed_ts FROM favorite_state WHERE entry_id=?", (entry_id,)).fetchone()
        # Ensure an explicit local click wins even if another node's clock is ahead
        # by a small amount; the change timestamp remains monotonic per entry.
        changed_ts = max(int(time.time() * 1000), (int(current[0]) + 1) if current else 0)
        event = {
            "event_id": event_id,
            "entry_id": entry_id,
            "is_favorite": 1 if is_favorite else 0,
            "changed_ts": changed_ts,
            "device": HOST_NAME[:100],
            "node": NODE_ID,
        }
        _db.execute(
            "INSERT INTO favorite_events(event_id,entry_id,is_favorite,changed_ts,device,node) VALUES(?,?,?,?,?,?)",
            (event_id, entry_id, event["is_favorite"], changed_ts, event["device"], event["node"]),
        )
        _db.execute(
            "INSERT INTO favorite_state(entry_id,is_favorite,changed_ts,device,node,event_id) VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(entry_id) DO UPDATE SET is_favorite=excluded.is_favorite,changed_ts=excluded.changed_ts, "
            "device=excluded.device,node=excluded.node,event_id=excluded.event_id",
            (entry_id, event["is_favorite"], changed_ts, event["device"], event["node"], event_id),
        )
    return {"id": entry_id, "favorite": is_favorite, "changed_ts": changed_ts}


def _clean_favorite_event(raw):
    if not isinstance(raw, dict):
        return None
    event_id = raw.get("event_id")
    entry_id = raw.get("entry_id", raw.get("id"))
    try:
        changed_ts = int(raw.get("changed_ts", 0))
        is_favorite = int(raw.get("is_favorite"))
    except (TypeError, ValueError):
        return None
    if not isinstance(event_id, str) or not event_id or len(event_id) > 100:
        return None
    if not isinstance(entry_id, str) or not entry_id or len(entry_id) > 100:
        return None
    if changed_ts <= 0 or is_favorite not in (0, 1):
        return None
    return {
        "event_id": event_id,
        "entry_id": entry_id,
        "is_favorite": is_favorite,
        "changed_ts": changed_ts,
        "device": str(raw.get("device", "Компьютер"))[:100],
        "node": str(raw.get("node", ""))[:100],
    }


def _merge_favorite_events(incoming):
    """Merge and relay star/unstar events with deterministic last-write-wins state."""
    if not isinstance(incoming, list):
        return 0
    changed = 0
    with _db_lock, _db:
        for raw in incoming:
            event = _clean_favorite_event(raw)
            if not event:
                continue
            cur = _db.execute(
                "INSERT OR IGNORE INTO favorite_events(event_id,entry_id,is_favorite,changed_ts,device,node) VALUES(?,?,?,?,?,?)",
                (event["event_id"], event["entry_id"], event["is_favorite"], event["changed_ts"], event["device"], event["node"]),
            )
            if not cur.rowcount:
                continue
            # A deletion tombstone always wins over any star-state event.
            if _db.execute("SELECT 1 FROM deletions WHERE id=?", (event["entry_id"],)).fetchone():
                continue
            current = _db.execute(
                "SELECT changed_ts,node,event_id FROM favorite_state WHERE entry_id=?", (event["entry_id"],)
            ).fetchone()
            incoming_key = (event["changed_ts"], event["node"], event["event_id"])
            current_key = (int(current["changed_ts"]), current["node"], current["event_id"]) if current else None
            if current_key is None or incoming_key > current_key:
                _db.execute(
                    "INSERT INTO favorite_state(entry_id,is_favorite,changed_ts,device,node,event_id) VALUES(?,?,?,?,?,?) "
                    "ON CONFLICT(entry_id) DO UPDATE SET is_favorite=excluded.is_favorite,changed_ts=excluded.changed_ts, "
                    "device=excluded.device,node=excluded.node,event_id=excluded.event_id",
                    (event["entry_id"], event["is_favorite"], event["changed_ts"], event["device"], event["node"], event["event_id"]),
                )
                changed += 1
    if changed:
        print("[ClipHouse] Получено изменений избранного: %s" % changed)
    return changed


def _add_event(text, set_clipboard=False, source="local"):
    if not isinstance(text, str):
        raise ValueError("Ожидался текст")
    if not text.strip():
        raise ValueError("Пустой текст не добавлен")
    if len(text) > MAX_TEXT_CHARS:
        raise ValueError("Слишком длинный текст: максимум %s символов" % MAX_TEXT_CHARS)

    event = {
        "id": str(uuid.uuid4()),
        "ts": int(time.time() * 1000),
        "text": text,
        "device": HOST_NAME[:100],
        "node": NODE_ID,
        "source": source,
    }
    duplicate = False
    with _db_lock:
        top = _get_latest_locked()
        if top and top.get("text") == text and abs(event["ts"] - int(top.get("ts", 0))) <= 1200:
            event = top
            duplicate = True
        else:
            _insert_event_locked(event)
            _db.commit()
            top_after = _get_latest_locked()
            if top_after and top_after["id"] == event["id"]:
                event = top_after
    if set_clipboard:
        _set_system_clipboard(text)
    if not duplicate:
        print("[ClipHouse] Новый текст: %s символов (%s)" % (len(text), source))
    return event, duplicate


def _merge_events(incoming):
    if not isinstance(incoming, list):
        return 0
    changed = 0
    new_top_text = None
    with _db_lock:
        before = _get_latest_locked()
        with _db:
            for raw in incoming:
                event = _clean_event(raw)
                if event and _insert_event_locked(event):
                    changed += 1
        after = _get_latest_locked()
        if after and (before is None or (int(after["ts"]), str(after["id"])) > (int(before["ts"]), str(before["id"]))):
            if after.get("node") != NODE_ID:
                new_top_text = after["text"]
    if new_top_text is not None:
        _set_system_clipboard(new_top_text)
    if changed:
        print("[ClipHouse] Получено новых записей от другого узла: %s" % changed)
    return changed


def _history_page(page=1, query=""):
    page = max(1, int(page))
    offset = (page - 1) * PAGE_SIZE
    query = (query or "").strip()[:200]
    with _db_lock:
        if not query:
            total = _entry_count_locked()
            rows = _db.execute(
                "SELECT e.seq,e.id,e.ts,e.text,e.device,e.node,e.source,COALESCE(fs.is_favorite,0) AS favorite "
                "FROM entries e LEFT JOIN favorite_state fs ON fs.entry_id=e.id AND fs.is_favorite=1 "
                "ORDER BY e.ts DESC,e.seq DESC LIMIT ? OFFSET ?",
                (PAGE_SIZE, offset),
            ).fetchall()
        elif _FTS_ENABLED:
            # Quoted FTS phrase; malformed/unsupported input falls back to LIKE.
            fts_query = '"' + query.replace('"', '""') + '"'
            try:
                total = int(_db.execute("SELECT COUNT(*) FROM entries_fts WHERE entries_fts MATCH ?", (fts_query,)).fetchone()[0])
                rows = _db.execute(
                    "SELECT e.seq,e.id,e.ts,e.text,e.device,e.node,e.source,COALESCE(fs.is_favorite,0) AS favorite "
                    "FROM entries e JOIN entries_fts f ON f.rowid=e.seq "
                    "LEFT JOIN favorite_state fs ON fs.entry_id=e.id AND fs.is_favorite=1 "
                    "WHERE entries_fts MATCH ? ORDER BY e.ts DESC,e.seq DESC LIMIT ? OFFSET ?",
                    (fts_query, PAGE_SIZE, offset),
                ).fetchall()
            except sqlite3.OperationalError:
                like = "%" + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
                total = int(_db.execute("SELECT COUNT(*) FROM entries WHERE text LIKE ? ESCAPE '\\' COLLATE NOCASE", (like,)).fetchone()[0])
                rows = _db.execute(
                    "SELECT e.seq,e.id,e.ts,e.text,e.device,e.node,e.source,COALESCE(fs.is_favorite,0) AS favorite "
                    "FROM entries e LEFT JOIN favorite_state fs ON fs.entry_id=e.id AND fs.is_favorite=1 "
                    "WHERE e.text LIKE ? ESCAPE '\\' COLLATE NOCASE ORDER BY e.ts DESC,e.seq DESC LIMIT ? OFFSET ?",
                    (like, PAGE_SIZE, offset),
                ).fetchall()
        else:
            like = "%" + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
            total = int(_db.execute("SELECT COUNT(*) FROM entries WHERE text LIKE ? ESCAPE '\\' COLLATE NOCASE", (like,)).fetchone()[0])
            rows = _db.execute(
                "SELECT e.seq,e.id,e.ts,e.text,e.device,e.node,e.source,COALESCE(fs.is_favorite,0) AS favorite "
                "FROM entries e LEFT JOIN favorite_state fs ON fs.entry_id=e.id AND fs.is_favorite=1 "
                "WHERE e.text LIKE ? ESCAPE '\\' COLLATE NOCASE ORDER BY e.ts DESC,e.seq DESC LIMIT ? OFFSET ?",
                (like, PAGE_SIZE, offset),
            ).fetchall()
    entries = [_event_from_row(row) for row in rows]
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    return {"ok": True, "entries": entries, "page": page, "page_size": PAGE_SIZE, "total": total,
            "pages": pages, "has_prev": page > 1, "has_next": page < pages, "query": query}


def _favorites_page(page=1):
    page = max(1, int(page))
    offset = (page - 1) * PAGE_SIZE
    with _db_lock:
        total = int(_db.execute(
            "SELECT COUNT(*) FROM entries e JOIN favorite_state fs ON fs.entry_id=e.id AND fs.is_favorite=1 "
            "LEFT JOIN deletions d ON d.id=e.id WHERE d.id IS NULL"
        ).fetchone()[0])
        rows = _db.execute(
            "SELECT e.seq,e.id,e.ts,e.text,e.device,e.node,e.source,1 AS favorite "
            "FROM entries e JOIN favorite_state fs ON fs.entry_id=e.id AND fs.is_favorite=1 "
            "LEFT JOIN deletions d ON d.id=e.id WHERE d.id IS NULL "
            "ORDER BY fs.changed_ts DESC,e.ts DESC,e.seq DESC LIMIT ? OFFSET ?",
            (PAGE_SIZE, offset),
        ).fetchall()
    entries = [_event_from_row(row) for row in rows]
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    return {"ok": True, "entries": entries, "page": page, "page_size": PAGE_SIZE, "total": total,
            "pages": pages, "has_prev": page > 1, "has_next": page < pages}


def _get_sync_cursor(peer_id):
    with _db_lock:
        row = _db.execute("SELECT last_seq FROM sync_cursors WHERE peer_id=?", (peer_id,)).fetchone()
        return int(row[0]) if row else 0


def _get_sync_delete_cursor(peer_id):
    with _db_lock:
        row = _db.execute("SELECT last_delete_seq FROM sync_cursors WHERE peer_id=?", (peer_id,)).fetchone()
        return int(row[0]) if row else 0


def _set_sync_cursor(peer_id, last_seq):
    with _db_lock, _db:
        _db.execute("INSERT INTO sync_cursors(peer_id,last_seq,last_delete_seq) VALUES(?,?,0) "
                     "ON CONFLICT(peer_id) DO UPDATE SET last_seq=MAX(last_seq,excluded.last_seq)",
                     (peer_id, int(last_seq)))


def _set_sync_delete_cursor(peer_id, last_seq):
    with _db_lock, _db:
        _db.execute("INSERT INTO sync_cursors(peer_id,last_seq,last_delete_seq) VALUES(?,0,?) "
                     "ON CONFLICT(peer_id) DO UPDATE SET last_delete_seq=MAX(last_delete_seq,excluded.last_delete_seq)",
                     (peer_id, int(last_seq)))


def _get_sync_favorite_cursor(peer_id):
    with _db_lock:
        row = _db.execute("SELECT last_favorite_seq FROM sync_cursors WHERE peer_id=?", (peer_id,)).fetchone()
        return int(row[0]) if row else 0


def _set_sync_favorite_cursor(peer_id, last_seq):
    with _db_lock, _db:
        _db.execute("INSERT INTO sync_cursors(peer_id,last_seq,last_delete_seq,last_favorite_seq) VALUES(?,0,0,?) "
                     "ON CONFLICT(peer_id) DO UPDATE SET last_favorite_seq=MAX(last_favorite_seq,excluded.last_favorite_seq)",
                     (peer_id, int(last_seq)))


def _get_push_cursor(peer_id, column):
    allowed = {"last_push_seq", "last_push_delete_seq", "last_push_favorite_seq"}
    if column not in allowed:
        raise ValueError("Invalid push cursor")
    with _db_lock:
        row = _db.execute("SELECT %s FROM sync_cursors WHERE peer_id=?" % column, (peer_id,)).fetchone()
        return int(row[0]) if row else 0


def _set_push_cursor(peer_id, column, last_seq):
    allowed = {"last_push_seq", "last_push_delete_seq", "last_push_favorite_seq"}
    if column not in allowed:
        raise ValueError("Invalid push cursor")
    with _db_lock, _db:
        _db.execute(
            "INSERT INTO sync_cursors(peer_id,%s) VALUES(?,?) "
            "ON CONFLICT(peer_id) DO UPDATE SET %s=MAX(%s,excluded.%s)" %
            (column, column, column, column),
            (peer_id, int(last_seq)),
        )


def _get_sync_batch(after_seq):
    with _db_lock:
        rows = _db.execute(
            "SELECT seq,id,ts,text,device,node,source FROM entries WHERE seq>? ORDER BY seq ASC LIMIT ?",
            (int(after_seq), SYNC_BATCH_SIZE),
        ).fetchall()
    result = []
    for row in rows:
        event = _event_from_row(row)
        event["sync_seq"] = int(row["seq"])
        result.append(event)
    return result


def _get_delete_sync_batch(after_seq):
    with _db_lock:
        rows = _db.execute(
            "SELECT seq,id,deleted_ts,device,node FROM deletions WHERE seq>? ORDER BY seq ASC LIMIT ?",
            (int(after_seq), SYNC_BATCH_SIZE),
        ).fetchall()
    result = []
    for row in rows:
        result.append({"id": row["id"], "deleted_ts": int(row["deleted_ts"]),
                       "device": row["device"], "node": row["node"], "sync_seq": int(row["seq"])})
    return result


def _get_favorite_sync_batch(after_seq):
    with _db_lock:
        rows = _db.execute(
            "SELECT seq,event_id,entry_id,is_favorite,changed_ts,device,node FROM favorite_events "
            "WHERE seq>? ORDER BY seq ASC LIMIT ?",
            (int(after_seq), SYNC_BATCH_SIZE),
        ).fetchall()
    return [{"event_id": row["event_id"], "entry_id": row["entry_id"],
             "is_favorite": int(row["is_favorite"]), "changed_ts": int(row["changed_ts"]),
             "device": row["device"], "node": row["node"], "sync_seq": int(row["seq"])} for row in rows]


# ---------- Native Windows clipboard (ctypes only; no pywin32 needed) ----------
_CF_UNICODETEXT = 13
_GMEM_MOVEABLE = 0x0002


def _read_system_clipboard():
    if os.name != "nt":
        return None
    user32 = ctypes.windll.user32
    kernel32 = ctypes.windll.kernel32
    user32.OpenClipboard.argtypes = [ctypes.c_void_p]
    user32.OpenClipboard.restype = ctypes.c_bool
    user32.GetClipboardData.argtypes = [ctypes.c_uint]
    user32.GetClipboardData.restype = ctypes.c_void_p
    user32.CloseClipboard.restype = ctypes.c_bool
    opened = False
    try:
        for _ in range(4):
            if user32.OpenClipboard(None):
                opened = True
                break
            time.sleep(0.025)
        if not opened:
            return None
        handle = user32.GetClipboardData(_CF_UNICODETEXT)
        if not handle:
            return None
        kernel32.GlobalLock.argtypes = [ctypes.c_void_p]
        kernel32.GlobalLock.restype = ctypes.c_void_p
        kernel32.GlobalUnlock.argtypes = [ctypes.c_void_p]
        ptr = kernel32.GlobalLock(handle)
        if not ptr:
            return None
        try:
            return ctypes.wstring_at(ptr)
        finally:
            kernel32.GlobalUnlock(handle)
    except Exception:
        return None
    finally:
        if opened:
            try:
                user32.CloseClipboard()
            except Exception:
                pass


def _set_system_clipboard(text):
    if os.name != "nt" or not isinstance(text, str) or len(text) > MAX_TEXT_CHARS:
        return False
    user32 = ctypes.windll.user32
    kernel32 = ctypes.windll.kernel32
    user32.OpenClipboard.argtypes = [ctypes.c_void_p]
    user32.OpenClipboard.restype = ctypes.c_bool
    user32.CloseClipboard.restype = ctypes.c_bool
    user32.EmptyClipboard.restype = ctypes.c_bool
    opened = False
    hmem = None
    try:
        for _ in range(5):
            if user32.OpenClipboard(None):
                opened = True
                break
            time.sleep(0.04)
        if not opened:
            return False
        user32.EmptyClipboard()
        raw = (text + "\0").encode("utf-16-le")
        kernel32.GlobalAlloc.argtypes = [ctypes.c_uint, ctypes.c_size_t]
        kernel32.GlobalAlloc.restype = ctypes.c_void_p
        kernel32.GlobalLock.argtypes = [ctypes.c_void_p]
        kernel32.GlobalLock.restype = ctypes.c_void_p
        kernel32.GlobalUnlock.argtypes = [ctypes.c_void_p]
        kernel32.GlobalFree.argtypes = [ctypes.c_void_p]
        kernel32.GlobalFree.restype = ctypes.c_void_p
        user32.SetClipboardData.argtypes = [ctypes.c_uint, ctypes.c_void_p]
        user32.SetClipboardData.restype = ctypes.c_void_p
        hmem = kernel32.GlobalAlloc(_GMEM_MOVEABLE, len(raw))
        if not hmem:
            return False
        ptr = kernel32.GlobalLock(hmem)
        if not ptr:
            kernel32.GlobalFree(hmem)
            hmem = None
            return False
        ctypes.memmove(ptr, raw, len(raw))
        kernel32.GlobalUnlock(hmem)
        if not user32.SetClipboardData(_CF_UNICODETEXT, hmem):
            kernel32.GlobalFree(hmem)
            hmem = None
            return False
        hmem = None  # ownership passes to Windows
        with _clip_lock:
            _suppressed_clipboard[text] = time.monotonic() + 8.0
        return True
    except Exception as exc:
        print("[ClipHouse] Не удалось обновить системный буфер:", exc)
        return False
    finally:
        if hmem:
            try:
                kernel32.GlobalFree(hmem)
            except Exception:
                pass
        if opened:
            try:
                user32.CloseClipboard()
            except Exception:
                pass


def _is_suppressed_clipboard(text):
    now = time.monotonic()
    with _clip_lock:
        for key in [key for key, expiry in _suppressed_clipboard.items() if expiry < now]:
            _suppressed_clipboard.pop(key, None)
        expiry = _suppressed_clipboard.get(text)
        if expiry and expiry >= now:
            _suppressed_clipboard.pop(text, None)
            return True
    return False


def _clipboard_watcher():
    if os.name != "nt":
        print("[ClipHouse] Автозахват системного буфера доступен только в Windows.")
        return
    last_seen = _read_system_clipboard()
    while True:
        time.sleep(0.35)
        value = _read_system_clipboard()
        if value is None or value == last_seen:
            continue
        last_seen = value
        if _is_suppressed_clipboard(value):
            continue
        if value.strip() and len(value) <= MAX_TEXT_CHARS:
            try:
                _add_event(value, set_clipboard=False, source="clipboard")
            except Exception as exc:
                print("[ClipHouse] Ошибка захвата буфера:", exc)


# ---------- Peer discovery and incremental replication on LAN ----------
def _register_peer(peer_id, ip, port, name):
    """Remember a LAN peer learned from UDP discovery or an HTTP sync request.

    Learning the caller during /api/sync makes discovery resilient when one PC's
    Windows Firewall drops UDP broadcasts in only one direction.
    """
    try:
        peer_id = str(peer_id or "").strip()[:100]
        port = int(port)
        ip = str(ip or "").strip()
        name = str(name or "Другой компьютер")[:100]
    except (TypeError, ValueError):
        return False
    if not peer_id or peer_id == NODE_ID or not ip or not (1 <= port <= 65535):
        return False
    # The IP address comes from recvfrom() or the TCP socket peer address, not from
    # a value supplied by the remote request. Do not allow URL/path injection.
    if any(ch not in "0123456789." for ch in ip):
        return False
    with _peers_lock:
        _peers[peer_id] = {"url": "http://%s:%d" % (ip, port), "ip": ip,
                           "name": name, "last_seen": time.monotonic()}
    return True


def _discovery_sender():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    announcement = json.dumps({"app": APP_NAME, "version": APP_VERSION, "id": NODE_ID,
                               "name": HOST_NAME, "port": HTTP_PORT}).encode("utf-8")
    while True:
        try:
            sock.sendto(announcement, ("255.255.255.255", DISCOVERY_PORT))
        except OSError:
            pass
        time.sleep(5.0)


def _discovery_receiver():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", DISCOVERY_PORT))
        sock.settimeout(1.0)
    except OSError as exc:
        print("[ClipHouse] Не удалось запустить автообнаружение:", exc)
        return
    while True:
        try:
            data, addr = sock.recvfrom(4096)
            info = json.loads(data.decode("utf-8", "replace"))
            if (info.get("app") != APP_NAME or int(info.get("version", 0)) != APP_VERSION
                    or not info.get("id") or info.get("id") == NODE_ID):
                continue
            # New database protocol; update all computers to the same project version.
            port = int(info.get("port", HTTP_PORT))
            if not 1 <= port <= 65535:
                continue
            _register_peer(info["id"], addr[0], port, info.get("name", "Другой компьютер"))
        except socket.timeout:
            pass
        except (ValueError, TypeError, OSError):
            pass
        except Exception:
            pass


def _peer_sync_loop():
    while True:
        time.sleep(POLL_SECONDS)
        now = time.monotonic()
        with _peers_lock:
            expired = [pid for pid, peer in _peers.items() if now - peer["last_seen"] > PEER_TIMEOUT]
            for pid in expired:
                _peers.pop(pid, None)
                _sync_error_log.pop(pid, None)
            peers = [(pid, dict(peer)) for pid, peer in _peers.items()]
        for peer_id, peer in peers:
            try:
                # PULL: retrieve changes published by this peer.
                cursor = _get_sync_cursor(peer_id)
                delete_cursor = _get_sync_delete_cursor(peer_id)
                favorite_cursor = _get_sync_favorite_cursor(peer_id)
                sync_query = urllib.parse.urlencode({
                    "after_seq": cursor,
                    "after_delete_seq": delete_cursor,
                    "after_favorite_seq": favorite_cursor,
                    "from_id": NODE_ID,
                    "from_name": HOST_NAME,
                    "from_port": HTTP_PORT,
                    "from_version": APP_VERSION,
                })
                url = peer["url"] + "/api/sync?" + sync_query
                req = urllib.request.Request(url, headers={"User-Agent": "ClipHouse/4.1"})
                with urllib.request.urlopen(req, timeout=3.0) as response:
                    payload = json.loads(response.read(32 * 1024 * 1024).decode("utf-8"))
                if payload.get("node_id") == NODE_ID or int(payload.get("version", 0)) != APP_VERSION:
                    continue
                entries = payload.get("entries", [])
                _merge_events(entries)
                _merge_deletions(payload.get("deletions", []))
                _merge_favorite_events(payload.get("favorites", []))
                next_cursor = int(payload.get("next_cursor", cursor))
                if next_cursor > cursor:
                    _set_sync_cursor(peer_id, next_cursor)
                next_delete_cursor = int(payload.get("next_delete_cursor", delete_cursor))
                if next_delete_cursor > delete_cursor:
                    _set_sync_delete_cursor(peer_id, next_delete_cursor)
                next_favorite_cursor = int(payload.get("next_favorite_cursor", favorite_cursor))
                if next_favorite_cursor > favorite_cursor:
                    _set_sync_favorite_cursor(peer_id, next_favorite_cursor)

                # PUSH: send our own/unseen changes to the peer too. This is important
                # when UDP discovery is asymmetric: A can see B and connect to B, while
                # B may not receive A's UDP broadcast. A can still send changes to B over
                # the same working HTTP connection direction.
                push_cursor = _get_push_cursor(peer_id, "last_push_seq")
                push_delete_cursor = _get_push_cursor(peer_id, "last_push_delete_seq")
                push_favorite_cursor = _get_push_cursor(peer_id, "last_push_favorite_seq")
                push_entries = _get_sync_batch(push_cursor)
                push_deletions = _get_delete_sync_batch(push_delete_cursor)
                push_favorites = _get_favorite_sync_batch(push_favorite_cursor)
                if push_entries or push_deletions or push_favorites:
                    push_payload = {
                        "version": APP_VERSION,
                        "node_id": NODE_ID,
                        "entries": push_entries,
                        "deletions": push_deletions,
                        "favorites": push_favorites,
                    }
                    push_body = json.dumps(push_payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                    push_req = urllib.request.Request(
                        peer["url"] + "/api/replicate", data=push_body,
                        headers={"Content-Type": "application/json", "User-Agent": "ClipHouse/4.1"},
                        method="POST",
                    )
                    with urllib.request.urlopen(push_req, timeout=3.0) as response:
                        push_result = json.loads(response.read(1024 * 1024).decode("utf-8"))
                    if not push_result.get("ok"):
                        raise RuntimeError(str(push_result.get("error", "peer rejected replication")))
                    if push_entries:
                        _set_push_cursor(peer_id, "last_push_seq", push_entries[-1]["sync_seq"])
                    if push_deletions:
                        _set_push_cursor(peer_id, "last_push_delete_seq", push_deletions[-1]["sync_seq"])
                    if push_favorites:
                        _set_push_cursor(peer_id, "last_push_favorite_seq", push_favorites[-1]["sync_seq"])
                _sync_error_log.pop(peer_id, None)
            except Exception as exc:
                # A peer may be sleeping, rebooting, or blocked by firewall. Log
                # occasionally so one-way connectivity can be diagnosed without flooding.
                last_log = _sync_error_log.get(peer_id, 0.0)
                if time.monotonic() - last_log >= 15.0:
                    print("[ClipHouse] Синхронизация с %s (%s) не удалась: %s" %
                          (peer.get("name", peer_id), peer.get("ip", "?"), str(exc)[:180]))
                    _sync_error_log[peer_id] = time.monotonic()


def _get_lan_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("192.0.2.1", 9))  # route lookup only; sends no packet
        ip = s.getsockname()[0]
        if not ip.startswith("127."):
            return ip
    except OSError:
        pass
    finally:
        s.close()
    try:
        for item in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET, socket.SOCK_STREAM):
            ip = item[4][0]
            if not ip.startswith("127."):
                return ip
    except OSError:
        pass
    return "127.0.0.1"


HTML_PAGE = r'''<!doctype html>
<html lang="ru"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#17191d"><title>ClipHouse</title>
<style>
:root{color-scheme:dark;--bg:#111317;--panel:#1b1e24;--line:#30343d;--muted:#a8afbc;--accent:#8ab4ff;--text:#f2f4f8;--yellow:#ffd84d;--danger:#ff929b}
*{box-sizing:border-box}html{scroll-behavior:auto}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif}main{max-width:860px;margin:auto;padding:10px 12px 22px}.top{display:flex;align-items:center;justify-content:space-between;gap:10px;margin:2px 0 10px;min-height:34px}.brand{font-weight:800;font-size:19px;letter-spacing:-.3px}.record-count{font-size:12px;color:var(--muted);white-space:nowrap;background:#20232a;border:1px solid var(--line);border-radius:999px;padding:4px 9px}.panel{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:11px;margin:9px 0}label{display:block;font-weight:700;margin-bottom:7px}textarea,input{display:block;width:100%;background:#111317;color:var(--text);border:1px solid var(--line);border-radius:8px;padding:10px;font:inherit;outline:none}textarea{min-height:92px;resize:vertical}textarea:focus,input:focus{border-color:var(--accent)}button{display:inline-flex;align-items:center;justify-content:center;gap:6px;background:#292e38;color:var(--text);border:1px solid #414754;border-radius:8px;padding:8px 10px;font:inherit;cursor:pointer;touch-action:manipulation}button:disabled{opacity:.48;cursor:default}.primary{background:#315f9e;border-color:#4c7dc0}.actions{display:flex;gap:7px;margin-top:8px}.actions button{min-height:36px}.latest-title{font-weight:700;margin-bottom:7px;display:flex;align-items:center;justify-content:space-between;gap:8px}.entry-text{white-space:pre-wrap;overflow-wrap:anywhere;background:#111317;border-radius:7px;padding:9px;max-height:180px;overflow:auto;font:13.5px/1.42 ui-monospace,SFMono-Regular,Consolas,monospace;margin:0}.history-head{font-weight:700;cursor:pointer;display:flex;align-items:center;gap:8px;list-style:none;min-height:28px}.history-head::-webkit-details-marker{display:none}.history-head:after{content:'＋';margin-left:auto;color:var(--muted);font-size:16px;font-weight:400}details[open]>.history-head:after{content:'−'}.search-row{display:flex;align-items:center;gap:6px;margin-top:10px}.search-row input{flex:1;min-width:0;padding:8px 10px;height:36px}.history-info{display:flex;justify-content:space-between;align-items:center;gap:8px;color:var(--muted);font-size:11px;margin:7px 0}.entry{display:grid;grid-template-columns:23px minmax(0,1fr);gap:7px;border-top:1px solid var(--line);padding:9px 0}.entry:first-child{border-top:0}.entry-number{color:#838b99;font-size:11px;font-variant-numeric:tabular-nums;text-align:right;padding-top:7px}.entry-content{min-width:0}.entry-bar{display:flex;align-items:center;gap:7px;min-width:0;margin-bottom:5px}.entry-time{font-size:11px;color:var(--muted);white-space:nowrap;font-variant-numeric:tabular-nums}.entry-charcount{font-size:10px;color:#777f8c;white-space:nowrap;margin-left:auto}.entry-actions{display:flex;align-items:center;gap:4px;flex-shrink:0}.icon-btn{width:32px;height:32px;min-width:32px;padding:0;border-radius:7px;background:transparent;border-color:transparent;color:#a6adba}.icon-btn svg{width:17px;height:17px;display:block;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}.icon-btn:hover{background:#2a2e36;border-color:#414754;color:var(--text)}.icon-btn.star-toggle{font-size:22px;line-height:1;font-family:Arial,sans-serif;color:#717987}.icon-btn.star-toggle svg{display:none}.icon-btn.star-toggle.active{color:var(--yellow);text-shadow:0 0 7px #ffc92844;background:#332b15;border-color:#6e5b2c}.copy-btn.copied{color:#91d4a7}.delete-btn{color:#b9a0a5}.delete-btn:hover{color:var(--danger);background:#321f23;border-color:#704047}.entry-text{min-width:0}.favorite-summary-star{color:var(--yellow);font-size:17px}.favorite-panel{border-color:#514629}.favorite-entry{background:linear-gradient(90deg,#28231335,transparent 82%);border-radius:7px;padding-left:5px;padding-right:4px}.favorite-count{color:var(--yellow);font-size:12px}.empty{color:var(--muted);font-size:13px;padding:9px 0}.pager{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-top:9px}.pager button{font-size:12px;padding:7px 10px;min-height:32px}.toast{position:fixed;bottom:16px;left:50%;transform:translateX(-50%);padding:9px 13px;border-radius:9px;background:#e4f2e8;color:#123821;box-shadow:0 4px 16px #0006;display:none;max-width:90vw;z-index:5;font-size:13px}.small-icon{width:17px;height:17px;fill:none;stroke:currentColor;stroke-width:1.8;stroke-linecap:round;stroke-linejoin:round}.search-button,.clear-search{width:36px;height:36px;padding:0;flex:0 0 36px}.clear-search{color:var(--muted)}
@media(min-width:650px){main{padding-top:18px}.panel{padding:14px}textarea{min-height:112px}.entry{padding:11px 0}.entry-text{max-height:240px}}
@media(max-width:380px){main{padding:8px}.panel{padding:9px;margin:7px 0}.brand{font-size:18px}.entry{grid-template-columns:19px minmax(0,1fr);gap:5px}.icon-btn{width:29px;height:30px;min-width:29px}.icon-btn svg{width:16px;height:16px}.entry-bar{gap:4px}.entry-time{font-size:10px}.entry-charcount{display:none}.entry-actions{gap:2px}.entry-text{font-size:12.5px;padding:8px}}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important;scroll-behavior:auto!important}}
</style></head><body><main>
<div class="top"><div class="brand">📋 ClipHouse</div><div class="record-count" id="recordCount">Записей: —</div></div>
<section class="panel"><label for="newtext">Добавить текст</label><textarea id="newtext" placeholder="Вставь или напиши текст…" maxlength="1000000"></textarea><div class="actions"><button class="primary" id="publish">Добавить</button><button id="clearbox">Очистить</button></div></section>
<section class="panel"><div class="latest-title"><span>Последний текст</span><button class="icon-btn copy-btn" id="copylatest" title="Копировать последний текст" aria-label="Копировать последний текст" style="display:none"><svg viewBox="0 0 24 24" aria-hidden="true"><rect x="8" y="8" width="13" height="13" rx="2"/><path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3"/></svg></button></div><pre id="latest" class="entry-text">Пока пусто.</pre></section>
<section class="panel favorite-panel"><details id="favoritesDetails"><summary class="history-head"><span class="favorite-summary-star">★</span> Избранное <span class="favorite-count" id="favoriteCount"></span></summary><div class="history-info"><span id="favoritePageLabel"></span><span></span></div><div id="favorites"><div class="empty">Загрузи избранное, нажав на заголовок.</div></div><div class="pager"><button id="prevFavorites" disabled>← Назад</button><button id="nextFavorites" disabled>Дальше →</button></div></details></section>
<section class="panel"><details id="historyDetails"><summary class="history-head">История</summary><div class="search-row"><input id="search" type="search" placeholder="Поиск" aria-label="Поиск"><button id="searchButton" class="search-button" title="Найти" aria-label="Найти"><svg class="small-icon" viewBox="0 0 24 24" aria-hidden="true"><circle cx="10.8" cy="10.8" r="6.8"/><path d="m16 16 5 5"/></svg></button><button id="clearSearch" class="clear-search" title="Очистить поиск" aria-label="Очистить поиск"><svg class="small-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="m6 6 12 12M18 6 6 18"/></svg></button></div><div class="history-info"><span id="resultCount">Последние записи</span><span id="pageLabel"></span></div><div id="history"><div class="empty">Нажми, чтобы загрузить последние записи.</div></div><div class="pager"><button id="prevPage" disabled>← Новее</button><button id="nextPage" disabled>Старее →</button></div></details></section>
</main><div class="toast" id="toast" role="status" aria-live="polite"></div>
<script>
const $=id=>document.getElementById(id);let knownTopId=null;let knownTotal=null;let knownFavoriteRevision=null;let knownTop='';let currentPage=1;let currentQuery='';let pageCount=1;let historyLoaded=false;let requestToken=0;let favoritesPage=1;let favoritesPages=1;let favoritesLoaded=false;let favoritesRequestToken=0;let stateTimer=null;let isMobile=/Android|iPhone|iPad|iPod|Mobile/i.test(navigator.userAgent)||(navigator.maxTouchPoints>1&&/Macintosh/i.test(navigator.userAgent));
const COPY_ICON='<svg viewBox="0 0 24 24" aria-hidden="true"><rect x="8" y="8" width="13" height="13" rx="2"/><path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3"/></svg>';
const DELETE_ICON='<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M3 6h18M8 6V4h8v2M19 6l-1 14H6L5 6M10 11v5M14 11v5"/></svg>';
function esc(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function toast(msg){const t=$('toast');t.textContent=msg;t.style.display='block';clearTimeout(window._toastTimer);window._toastTimer=setTimeout(()=>t.style.display='none',1600)}
function pluralRecords(n){return 'Записей: '+Number(n||0)}
async function postText(text){const r=await fetch('/api/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text})});const d=await r.json();if(!r.ok||!d.ok)throw new Error(d.error||'Не удалось отправить текст');return d}
async function copyText(text){try{let copyPromise;if(navigator.clipboard&&navigator.clipboard.writeText){copyPromise=navigator.clipboard.writeText(text)}else{const x=document.createElement('textarea');x.value=text;x.style.position='fixed';x.style.opacity='0';document.body.appendChild(x);x.focus();x.select();const ok=document.execCommand('copy');x.remove();if(!ok)throw new Error('Браузер не разрешил копирование');copyPromise=Promise.resolve()}if(isMobile)await postText(text);await copyPromise;toast('Скопировано')}catch(e){const x=document.createElement('textarea');x.value=text;x.style.position='fixed';x.style.left='10px';x.style.top='10px';x.style.opacity='1';document.body.appendChild(x);x.focus();x.select();toast('Выделено — скопируй вручную')}}
function renderEntries(data,rootId,favoritesMode=false){
 const entries=data.entries||[];const root=$(rootId);
 if(!entries.length){root.innerHTML='<div class="empty">'+(favoritesMode?'Избранных записей пока нет. Отметь ★ нужный текст в истории.':'Ничего не найдено.')+'</div>';return}
 const pageBase=((Number(data.page)||1)-1)*50;
 root.innerHTML=entries.map((e,i)=>{
  const dt=new Date(e.ts);const when=dt.toLocaleDateString([],{day:'2-digit',month:'2-digit',year:'numeric'})+' '+dt.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
  const starred=!!e.favorite;const num=pageBase+i+1;
  return '<article class="entry '+(starred?'favorite-entry':'')+'"><span class="entry-number">'+num+'</span><div class="entry-content"><div class="entry-bar"><span class="entry-time">'+esc(when)+'</span><span class="entry-charcount">'+String(e.text.length)+' симв.</span><div class="entry-actions"><button class="icon-btn star-toggle '+(starred?'active':'')+'" data-favorite="'+i+'" title="'+(starred?'Убрать из избранного':'Добавить в избранное')+'" aria-label="'+(starred?'Убрать из избранного':'Добавить в избранное')+'">'+(starred?'★':'☆')+'</button><button class="icon-btn copy-btn" data-copy="'+i+'" title="Копировать" aria-label="Копировать">'+COPY_ICON+'</button><button class="icon-btn delete-btn" data-delete="'+i+'" title="Удалить везде" aria-label="Удалить везде">'+DELETE_ICON+'</button></div></div><pre class="entry-text">'+esc(e.text)+'</pre></div></article>'
 }).join('');
 root.querySelectorAll('[data-copy]').forEach(btn=>btn.addEventListener('click',async()=>{const e=entries[Number(btn.dataset.copy)];if(e){await copyText(e.text);btn.classList.add('copied');setTimeout(()=>btn.classList.remove('copied'),700)}}));
 root.querySelectorAll('[data-favorite]').forEach(btn=>btn.addEventListener('click',async()=>{const entry=entries[Number(btn.dataset.favorite)];if(!entry)return;btn.disabled=true;try{const r=await fetch('/api/favorite',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:entry.id,favorite:!entry.favorite})});const d=await r.json();if(!r.ok||!d.ok)throw new Error(d.error||'Ошибка избранного');toast(d.favorite?'В избранном ★':'Убрано из избранного');if($('historyDetails').open&&historyLoaded)await loadHistory(currentPage,currentQuery);if($('favoritesDetails').open||favoritesLoaded)await loadFavorites(favoritesPage);await loadState()}catch(err){toast(err.message);btn.disabled=false}}));
 root.querySelectorAll('[data-delete]').forEach(btn=>btn.addEventListener('click',async()=>{const entry=entries[Number(btn.dataset.delete)];if(!entry||!confirm('Удалить запись на всех устройствах ClipHouse?'))return;btn.disabled=true;try{const r=await fetch('/api/delete',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({ids:[entry.id]})});const d=await r.json();if(!r.ok||!d.ok)throw new Error(d.error||'Ошибка удаления');toast('Удалено везде');await loadState();if($('historyDetails').open)await loadHistory(currentPage,currentQuery);if($('favoritesDetails').open||favoritesLoaded)await loadFavorites(favoritesPage)}catch(err){toast('Не удалось удалить: '+err.message);btn.disabled=false}}));
}
function renderHistory(data){pageCount=data.pages||1;currentPage=data.page||1;$('resultCount').textContent='Записей: '+Number(data.total||0);$('pageLabel').textContent=data.pages>1?'Стр. '+currentPage+'/'+pageCount:'';$('prevPage').disabled=!data.has_prev;$('nextPage').disabled=!data.has_next;renderEntries(data,'history',false)}
function renderFavorites(data){favoritesPage=data.page||1;favoritesPages=data.pages||1;$('favoriteCount').textContent='('+Number(data.total||0)+')';$('favoritePageLabel').textContent=data.pages>1?'Стр. '+favoritesPage+'/'+favoritesPages:'';$('prevFavorites').disabled=!data.has_prev;$('nextFavorites').disabled=!data.has_next;renderEntries(data,'favorites',true);favoritesLoaded=true}
async function loadFavorites(page=1){const token=++favoritesRequestToken;favoritesPage=page;try{const r=await fetch('/api/favorites?page='+page,{cache:'no-store'});const d=await r.json();if(token!==favoritesRequestToken)return;if(!r.ok||!d.ok)throw new Error(d.error||'Ошибка загрузки избранного');if(page>(d.pages||1))return loadFavorites(d.pages||1);renderFavorites(d)}catch(e){$('favorites').innerHTML='<div class="empty">Не удалось загрузить избранное: '+esc(e.message)+'</div>'}}
async function loadHistory(page=1,query=currentQuery){const token=++requestToken;currentPage=page;currentQuery=query;const params=new URLSearchParams({page:String(page),q:query});try{const r=await fetch('/api/history?'+params.toString(),{cache:'no-store'});const d=await r.json();if(token!==requestToken)return;if(!r.ok||!d.ok)throw new Error(d.error||'Ошибка загрузки истории');renderHistory(d);historyLoaded=true}catch(e){$('history').innerHTML='<div class="empty">Ошибка загрузки: '+esc(e.message)+'</div>'}}
async function loadState(){try{const r=await fetch('/api/state',{cache:'no-store'});const d=await r.json();if(!r.ok||!d.ok)return;const total=Number(d.total||0);$('recordCount').textContent=pluralRecords(total);const latest=d.latest||null;const latestId=latest?latest.id:'';const totalChanged=knownTotal!==null&&knownTotal!==total;const latestChanged=knownTopId!==null&&knownTopId!==latestId;const favRevision=Number(d.favorite_revision||0);const favoriteChanged=knownFavoriteRevision!==null&&favRevision!==knownFavoriteRevision;knownTotal=total;knownTopId=latestId;knownFavoriteRevision=favRevision;if(latest){knownTop=latest.text;if($('latest').textContent!==latest.text)$('latest').textContent=latest.text;$('copylatest').style.display='inline-flex'}else{knownTop='';$('latest').textContent='Пока пусто.';$('copylatest').style.display='none'}if((totalChanged||latestChanged)&&$('historyDetails').open&&historyLoaded)await loadHistory(currentPage,currentQuery);if(favoriteChanged&&$('favoritesDetails').open&&favoritesLoaded)await loadFavorites(favoritesPage)}catch(e){/* transient local-network failure: retry next cycle without replacing the page */}}
function scheduleStatePoll(){clearTimeout(stateTimer);stateTimer=setTimeout(async()=>{await loadState();scheduleStatePoll()},5000)}
$('publish').addEventListener('click',async()=>{const text=$('newtext').value;if(!text.trim()){toast('Введи текст');return}const b=$('publish');b.disabled=true;try{await postText(text);$('newtext').value='';toast('Добавлено');await loadState();if($('historyDetails').open)await loadHistory(1,currentQuery)}catch(e){toast(e.message)}finally{b.disabled=false}});
$('clearbox').addEventListener('click',()=>$('newtext').value='');$('copylatest').addEventListener('click',()=>copyText(knownTop));
$('historyDetails').addEventListener('toggle',()=>{if($('historyDetails').open)loadHistory(1,currentQuery)});
$('searchButton').addEventListener('click',()=>loadHistory(1,$('search').value.trim()));$('search').addEventListener('keydown',e=>{if(e.key==='Enter')loadHistory(1,$('search').value.trim())});$('clearSearch').addEventListener('click',()=>{$('search').value='';loadHistory(1,'');$('search').blur()});
$('prevPage').addEventListener('click',()=>{if(currentPage>1)loadHistory(currentPage-1,currentQuery)});$('nextPage').addEventListener('click',()=>{if(currentPage<pageCount)loadHistory(currentPage+1,currentQuery)});
$('favoritesDetails').addEventListener('toggle',()=>{if($('favoritesDetails').open)loadFavorites(favoritesPage)});$('prevFavorites').addEventListener('click',()=>{if(favoritesPage>1)loadFavorites(favoritesPage-1)});$('nextFavorites').addEventListener('click',()=>{if(favoritesPage<favoritesPages)loadFavorites(favoritesPage+1)});
loadState().then(scheduleStatePoll);
</script></body></html>'''


class ClipHouseHandler(BaseHTTPRequestHandler):
    server_version = "ClipHouse/4.2.0"

    def log_message(self, fmt, *args):
        return

    def _send(self, code, body, content_type="application/json; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, code, data):
        self._send(code, json.dumps(data, ensure_ascii=False, separators=(",", ":")))

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            raise ValueError("Некорректный размер запроса")
        if length < 1 or length > MAX_REQUEST_BYTES:
            raise ValueError("Размер запроса должен быть от 1 байта до 5 МБ")
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            raise ValueError("Некорректный JSON")

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path in ("/", "/index.html"):
            self._send(200, HTML_PAGE, "text/html; charset=utf-8")
            return
        if parsed.path == "/api/state":
            with _db_lock:
                latest = _get_latest_locked()
                total = _entry_count_locked()
                favorite_revision_row = _db.execute("SELECT COALESCE(MAX(seq),0) FROM favorite_events").fetchone()
                favorite_revision = int(favorite_revision_row[0] or 0)
            with _peers_lock:
                now = time.monotonic()
                peer_names = [p["name"] for p in _peers.values() if now - p["last_seen"] <= PEER_TIMEOUT]
            self._json(200, {"ok": True, "node_id": NODE_ID, "host": HOST_NAME, "peer_count": len(peer_names),
                             "peers": peer_names, "latest": latest, "total": total,
                             "favorite_revision": favorite_revision})
            return
        if parsed.path == "/api/history":
            query = urllib.parse.parse_qs(parsed.query)
            try:
                page = int(query.get("page", ["1"])[0])
            except (ValueError, TypeError):
                page = 1
            search = query.get("q", [""])[0]
            try:
                self._json(200, _history_page(page, search))
            except Exception as exc:
                self._json(500, {"ok": False, "error": "Ошибка поиска: " + str(exc)[:180]})
            return
        if parsed.path == "/api/favorites":
            query = urllib.parse.parse_qs(parsed.query)
            try:
                page = int(query.get("page", ["1"])[0])
            except (ValueError, TypeError):
                page = 1
            try:
                self._json(200, _favorites_page(page))
            except Exception as exc:
                self._json(500, {"ok": False, "error": "Ошибка загрузки избранного: " + str(exc)[:180]})
            return
        if parsed.path == "/api/sync":
            query = urllib.parse.parse_qs(parsed.query)
            # A successful sync request is also a reliable way to learn the caller.
            # It repairs asymmetric UDP discovery: if A sees B but B misses A's
            # broadcast, B learns A from this HTTP request and begins pulling back.
            try:
                caller_version = int(query.get("from_version", ["0"])[0])
                caller_id = query.get("from_id", [""])[0]
                caller_name = query.get("from_name", ["Другой компьютер"])[0]
                caller_port = int(query.get("from_port", [str(HTTP_PORT)])[0])
                if caller_version == APP_VERSION and caller_id:
                    _register_peer(caller_id, self.client_address[0], caller_port, caller_name)
            except (ValueError, TypeError, IndexError):
                pass
            try:
                after_seq = max(0, int(query.get("after_seq", ["0"])[0]))
            except (ValueError, TypeError):
                after_seq = 0
            try:
                after_delete_seq = max(0, int(query.get("after_delete_seq", ["0"])[0]))
            except (ValueError, TypeError):
                after_delete_seq = 0
            try:
                after_favorite_seq = max(0, int(query.get("after_favorite_seq", ["0"])[0]))
            except (ValueError, TypeError):
                after_favorite_seq = 0
            entries = _get_sync_batch(after_seq)
            next_cursor = entries[-1]["sync_seq"] if entries else after_seq
            deletions = _get_delete_sync_batch(after_delete_seq)
            next_delete_cursor = deletions[-1]["sync_seq"] if deletions else after_delete_seq
            favorites = _get_favorite_sync_batch(after_favorite_seq)
            next_favorite_cursor = favorites[-1]["sync_seq"] if favorites else after_favorite_seq
            self._json(200, {"ok": True, "version": APP_VERSION, "node_id": NODE_ID,
                             "entries": entries, "next_cursor": next_cursor,
                             "deletions": deletions, "next_delete_cursor": next_delete_cursor,
                             "favorites": favorites, "next_favorite_cursor": next_favorite_cursor,
                             "batch_size": SYNC_BATCH_SIZE})
            return
        if parsed.path == "/api/health":
            self._json(200, {"ok": True, "app": APP_NAME, "version": APP_VERSION, "node_id": NODE_ID})
            return
        self._json(404, {"ok": False, "error": "Не найдено"})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path not in ("/api/add", "/api/delete", "/api/favorite", "/api/replicate"):
            self._json(404, {"ok": False, "error": "Не найдено"})
            return
        try:
            data = self._read_json()
            if not isinstance(data, dict):
                raise ValueError("Некорректные данные")
            if parsed.path == "/api/replicate":
                if int(data.get("version", 0)) != APP_VERSION:
                    self._json(409, {"ok": False, "error": "Несовместимая версия ClipHouse; обнови все узлы"})
                    return
                source_node = str(data.get("node_id", ""))[:100]
                if not source_node or source_node == NODE_ID:
                    self._json(400, {"ok": False, "error": "Некорректный отправитель"})
                    return
                merged_entries = _merge_events(data.get("entries", []))
                merged_deletions = _merge_deletions(data.get("deletions", []))
                merged_favorites = _merge_favorite_events(data.get("favorites", []))
                self._json(200, {"ok": True, "entries_received": merged_entries,
                                 "deletions_received": merged_deletions,
                                 "favorites_received": merged_favorites})
                return
            if parsed.path == "/api/delete":
                result = _delete_events(data.get("ids"))
                self._json(200, {"ok": True, **result})
                return
            if parsed.path == "/api/favorite":
                result = _set_favorite(data.get("id"), data.get("favorite"))
                self._json(200, {"ok": True, **result})
                return
            text = data.get("text")
            if not isinstance(text, str):
                raise ValueError("Не найден текст")
            event, duplicate = _add_event(text, set_clipboard=True, source="web")
            self._json(200, {"ok": True, "duplicate": duplicate, "id": event["id"], "ts": event["ts"]})
        except ValueError as exc:
            self._json(400, {"ok": False, "error": str(exc)})
        except Exception as exc:
            self._json(500, {"ok": False, "error": "Внутренняя ошибка: " + str(exc)[:200]})


def _start_http_server():
    global _http_server
    _http_server = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), ClipHouseHandler)
    _http_server.daemon_threads = True
    _http_server.request_queue_size = 32
    _http_server.timeout = 0.5
    threading.Thread(target=_http_server.serve_forever, name="ClipHouse-HTTP", daemon=True).start()


def main():
    global NODE_ID
    if sys.version_info < (3, 8):
        print("ClipHouse требует Python 3.8 или новее.")
        input("Нажми Enter для выхода…")
        return
    NODE_ID = _load_node_id()
    try:
        _initialize_database()
    except (OSError, sqlite3.Error) as exc:
        print("[ClipHouse] Не удалось открыть SQLite базу %s: %s" % (DATABASE_PATH, exc))
        input("Нажми Enter для выхода…")
        return
    try:
        _start_http_server()
    except OSError as exc:
        print("[ClipHouse] Не удалось открыть порт %s: %s" % (HTTP_PORT, exc))
        print("Возможно, ClipHouse уже запущен или порт занят другой программой.")
        input("Нажми Enter для выхода…")
        return

    threads = [
        threading.Thread(target=_discovery_receiver, name="ClipHouse-Discovery-RX", daemon=True),
        threading.Thread(target=_discovery_sender, name="ClipHouse-Discovery-TX", daemon=True),
        threading.Thread(target=_peer_sync_loop, name="ClipHouse-Sync", daemon=True),
    ]
    if AUTO_CAPTURE_CLIPBOARD:
        threads.append(threading.Thread(target=_clipboard_watcher, name="ClipHouse-Clipboard", daemon=True))
    for thread in threads:
        thread.start()

    ip = _get_lan_ip()
    print("=" * 62)
    print("ClipHouse %s (сетевой протокол %s) запущен на компьютере: %s" % (APP_DISPLAY_VERSION, APP_VERSION, HOST_NAME))
    print("Только на этом ПК: http://127.0.0.1:%s/" % HTTP_PORT)
    print("Для других устройств: http://%s:%s/" % (ip, HTTP_PORT))
    print("Короткое имя после настройки DNS на роутере: http://%s:%s/" % (LOCAL_NAME, HTTP_PORT))
    print("SQLite:             %s" % DATABASE_PATH)
    print("Автозахват буфера:  %s" % ("ВКЛ" if AUTO_CAPTURE_CLIPBOARD else "ВЫКЛ (текст добавляется через сайт)"))
    print("История: по 50 записей на страницу; SQLite хранит архив")
    print("Избранное: жёлтая звезда; изменения синхронизируются между узлами")
    print("Поиск:              %s" % ("FTS5 (полнотекстовый индекс)" if _FTS_ENABLED else "SQLite LIKE"))
    print("Синхронизация:       UDP %s; двусторонний PULL + PUSH партиями по %s записей" % (DISCOVERY_PORT, SYNC_BATCH_SIZE))
    print("Настройки:          %s" % CONFIG_PATH)
    print("Оставь это окно запущенным. Ctrl+C в ЭТОМ окне останавливает сервер;")
    print("в других программах Ctrl+C продолжает обычное копирование.")
    print("Не открывай порты ClipHouse в интернет на роутере.")
    print("=" * 62)
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nClipHouse завершает работу…")
        if _http_server:
            _http_server.shutdown()
            _http_server.server_close()
        if _db:
            with _db_lock:
                _db.close()


if __name__ == "__main__":
    main()
