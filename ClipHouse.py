#!/usr/bin/env python3
# ClipHouse - a tiny LAN clipboard and text history for Windows.
# Python standard library only. Designed for trusted home networks.

import configparser
import ctypes
import json
import os
import re
import socket
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

APP_NAME = "ClipHouse"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.ini")
APP_STATE_DIR = os.path.join(os.environ.get("LOCALAPPDATA", os.path.expanduser("~")), APP_NAME)
NODE_PATH = os.path.join(APP_STATE_DIR, "node-id.txt")
MAX_REQUEST_BYTES = 2 * 1024 * 1024
POLL_SECONDS = 2.5
PEER_TIMEOUT = 25.0
HOST_NAME = socket.gethostname()
NODE_ID = ""


def _write_default_config():
    cfg = configparser.ConfigParser(interpolation=None)
    cfg["server"] = {
        "http_port": "8787",
        "discovery_port": "8788",
    }
    default_history = r"%LOCALAPPDATA%\ClipHouse" if os.name == "nt" else APP_STATE_DIR
    cfg["storage"] = {
        "history_dir": default_history,
        "max_history": "60",
    }
    cfg["clipboard"] = {
        "auto_capture": "true",
        "max_text_chars": "100000",
    }
    with open(CONFIG_PATH, "w", encoding="utf-8-sig", newline="") as f:
        cfg.write(f)


def _expand_path(raw_value, fallback):
    raw_value = (raw_value or "").strip().strip('"')
    if not raw_value:
        return fallback
    # Support Windows %VARIABLE% and Unix $VARIABLE notation without dependencies.
    expanded = os.path.expandvars(os.path.expanduser(raw_value))
    def replace_percent(match):
        return os.environ.get(match.group(1), match.group(0))
    expanded = re.sub(r"%([A-Za-z_][A-Za-z0-9_]*)%", replace_percent, expanded)
    if re.search(r"%[A-Za-z_][A-Za-z0-9_]*%", expanded):
        print("[ClipHouse] Не удалось раскрыть переменную в history_dir; использую путь по умолчанию.")
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

    default_data_dir = os.path.join(os.environ.get("LOCALAPPDATA", os.path.expanduser("~")), APP_NAME)
    raw_history_dir = cfg.get("storage", "history_dir", fallback=default_data_dir)
    history_dir = _expand_path(raw_history_dir, default_data_dir)
    settings = {
        "http_port": get_int("server", "http_port", 8787, 1, 65535),
        "discovery_port": get_int("server", "discovery_port", 8788, 1, 65535),
        "history_dir": history_dir,
        "max_history": get_int("storage", "max_history", 60, 1, 500),
        "auto_capture": get_bool("clipboard", "auto_capture", True),
        "max_text_chars": get_int("clipboard", "max_text_chars", 100000, 1, 1000000),
    }
    return settings


_SETTINGS = _read_config()
HTTP_PORT = _SETTINGS["http_port"]
DISCOVERY_PORT = _SETTINGS["discovery_port"]
DATA_DIR = _SETTINGS["history_dir"]
HISTORY_PATH = os.path.join(DATA_DIR, "history.json")
MAX_HISTORY = _SETTINGS["max_history"]
AUTO_CAPTURE_CLIPBOARD = _SETTINGS["auto_capture"]
MAX_TEXT_CHARS = _SETTINGS["max_text_chars"]

_state_lock = threading.RLock()
_clip_lock = threading.Lock()
_peers_lock = threading.RLock()
_events = []
_peers = {}  # node_id -> {url, name, ip, last_seen}
_suppressed_clipboard = {}  # exact clipboard text -> monotonic expiry
_http_server = None


def _ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def _ensure_state_dir():
    os.makedirs(APP_STATE_DIR, exist_ok=True)


def _load_node_id():
    # Keep the node identity independent from the configurable history location.
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
    except OSError:
        pass
    return value


def _event_key(event):
    return (int(event.get("ts", 0)), str(event.get("id", "")))


def _clean_event(raw):
    if not isinstance(raw, dict):
        return None
    text = raw.get("text")
    ident = raw.get("id")
    ts = raw.get("ts")
    if not isinstance(text, str) or not isinstance(ident, str):
        return None
    if not text.strip() or len(text) > MAX_TEXT_CHARS or len(ident) > 100:
        return None
    try:
        ts = int(ts)
    except (TypeError, ValueError):
        return None
    return {
        "id": ident,
        "ts": ts,
        "text": text,
        "device": str(raw.get("device", "PC"))[:100],
        "node": str(raw.get("node", ""))[:100],
        "source": str(raw.get("source", ""))[:30],
    }


def _save_events_locked():
    tmp_path = HISTORY_PATH + ".tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(_events, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp_path, HISTORY_PATH)
    except OSError as exc:
        print("[ClipHouse] Не удалось сохранить историю:", exc)
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass


def _load_events():
    global _events, DATA_DIR, HISTORY_PATH
    try:
        _ensure_data_dir()
    except OSError as exc:
        fallback_dir = os.path.join(APP_STATE_DIR, "History")
        print("[ClipHouse] Не удалось создать папку истории (%s): %s" % (DATA_DIR, exc))
        print("[ClipHouse] Переключаюсь на запасной путь: %s" % fallback_dir)
        DATA_DIR = fallback_dir
        HISTORY_PATH = os.path.join(DATA_DIR, "history.json")
        _ensure_data_dir()
    try:
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError, TypeError):
        data = []
    clean = []
    seen = set()
    if isinstance(data, list):
        for raw in data:
            event = _clean_event(raw)
            if event and event["id"] not in seen:
                seen.add(event["id"])
                clean.append(event)
    clean.sort(key=_event_key, reverse=True)
    with _state_lock:
        _events = clean[:MAX_HISTORY]
        _save_events_locked()


def _current_top_locked():
    return _events[0] if _events else None


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
    with _state_lock:
        # Skip a duplicate generated by near-simultaneous browser/native clipboard handling.
        # A deliberate repeat is still recorded after this short 1.2 second window.
        top = _current_top_locked()
        if top and top.get("text") == text and abs(event["ts"] - int(top.get("ts", 0))) <= 1200:
            existing = dict(top)
            duplicate = True
        else:
            _events.append(event)
            _events.sort(key=_event_key, reverse=True)
            del _events[MAX_HISTORY:]
            _save_events_locked()
            existing = dict(event)
            duplicate = False

    if set_clipboard:
        _set_system_clipboard(text)
    if not duplicate:
        print("[ClipHouse] Новый текст: %s символов (%s)" % (len(text), source))
    return existing, duplicate


def _merge_events(incoming):
    global _events
    """Merge peer history. If a genuinely newer shared entry arrives, publish it to Windows clipboard."""
    if not isinstance(incoming, list):
        return 0
    changed = 0
    new_top = None
    with _state_lock:
        before = _current_top_locked()
        by_id = {e["id"]: e for e in _events}
        for raw in incoming:
            event = _clean_event(raw)
            if not event or event["id"] in by_id:
                continue
            by_id[event["id"]] = event
            changed += 1
        if changed:
            _events = list(by_id.values())
            _events.sort(key=_event_key, reverse=True)
            del _events[MAX_HISTORY:]
            _save_events_locked()
        after = _current_top_locked()
        if after and (before is None or _event_key(after) > _event_key(before)):
            if after.get("node") != NODE_ID:
                new_top = after["text"]
    if new_top is not None:
        _set_system_clipboard(new_top)
    return changed


def _snapshot_events():
    with _state_lock:
        return [dict(e) for e in _events]


# ---------- Native Windows clipboard (ctypes only; no pywin32 required) ----------
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
    if os.name != "nt" or not isinstance(text, str):
        return False
    if len(text) > MAX_TEXT_CHARS:
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
        # On success, Windows owns hmem.
        hmem = None
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
        expired = [key for key, expiry in _suppressed_clipboard.items() if expiry < now]
        for key in expired:
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


# ---------- Peer discovery and replication on the LAN ----------
def _discovery_sender():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    announcement = json.dumps({"app": APP_NAME, "id": NODE_ID, "name": HOST_NAME, "port": HTTP_PORT}).encode("utf-8")
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
            if info.get("app") != APP_NAME or not info.get("id") or info.get("id") == NODE_ID:
                continue
            port = int(info.get("port", HTTP_PORT))
            if not 1 <= port <= 65535:
                continue
            peer_id = str(info["id"])
            with _peers_lock:
                _peers[peer_id] = {
                    "url": "http://%s:%d" % (addr[0], port),
                    "ip": addr[0],
                    "name": str(info.get("name", "Другой компьютер"))[:100],
                    "last_seen": time.monotonic(),
                }
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
            peers = [(pid, dict(peer)) for pid, peer in _peers.items()]

        own_ids = [event["id"] for event in _snapshot_events()]
        query = urllib.parse.urlencode({"have": ",".join(own_ids)})
        for peer_id, peer in peers:
            try:
                req = urllib.request.Request(peer["url"] + "/api/sync?" + query, headers={"User-Agent": "ClipHouse/1.0"})
                with urllib.request.urlopen(req, timeout=1.5) as response:
                    payload = json.loads(response.read(32 * 1024 * 1024).decode("utf-8"))
                if payload.get("node_id") == NODE_ID:
                    continue
                _merge_events(payload.get("entries", []))
            except Exception:
                # Peers can sleep, reboot, or be temporarily blocked by the firewall.
                pass


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
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#17191d">
<title>ClipHouse — общий буфер</title>
<style>
:root{color-scheme:dark;--bg:#111317;--panel:#1b1e24;--line:#30343d;--muted:#a8afbc;--accent:#78a9ff;--good:#82d9a0}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:#f4f5f7;font:16px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif}
main{max-width:900px;margin:0 auto;padding:16px 14px 44px}.top{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:12px}.brand{font-weight:750;font-size:22px;letter-spacing:.2px}.status{font-size:12px;color:var(--muted);text-align:right}.panel{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:14px;margin:12px 0}
label{display:block;font-weight:650;margin:0 0 8px}textarea{font:inherit;color:#f4f5f7;background:#101216;border:1px solid #414754;border-radius:10px;display:block;width:100%;min-height:112px;padding:12px;resize:vertical;outline:none}textarea:focus{border-color:var(--accent)}
button{font:inherit;cursor:pointer;border:1px solid #465063;background:#252b36;color:#fff;border-radius:9px;padding:9px 13px;min-height:42px}button:active{transform:translateY(1px)}button.primary{background:#dce8ff;border-color:#dce8ff;color:#111827;font-weight:700}button.copy{background:#252b36;min-width:88px}button:disabled{opacity:.6;cursor:default}.actions{display:flex;align-items:center;gap:9px;flex-wrap:wrap;margin-top:10px}.hint{font-size:12px;color:var(--muted);margin-top:8px}.history-head{display:flex;align-items:center;justify-content:space-between;gap:12px}.history-head summary{font-size:17px;font-weight:700;cursor:pointer;list-style:none}.history-head summary::-webkit-details-marker{display:none}summary.history-head:after{content:'＋';margin-left:auto;padding-left:12px;color:var(--muted)}details[open]>summary.history-head:after{content:'−'}#history{margin-top:12px}.entry{border-top:1px solid var(--line);padding:12px 0}.entry:first-child{border-top:0}.entry-meta{display:flex;align-items:center;gap:8px;justify-content:space-between;font-size:12px;color:var(--muted);margin-bottom:6px}.entry-text{white-space:pre-wrap;overflow-wrap:anywhere;background:#111317;border-radius:8px;padding:10px;max-height:180px;overflow:auto;font:14px/1.45 ui-monospace,SFMono-Regular,Consolas,monospace;margin:0}.entry-actions{display:flex;justify-content:flex-end;margin-top:7px}.empty{color:var(--muted);font-size:14px;padding:8px 0}.foot{font-size:12px;line-height:1.5;color:var(--muted);margin-top:16px}.toast{position:fixed;bottom:18px;left:50%;transform:translateX(-50%);padding:10px 14px;border-radius:10px;background:#e4f2e8;color:#123821;box-shadow:0 8px 30px #0008;display:none;max-width:90vw;z-index:5}.top-copy{float:right;margin-top:-3px}
@media(min-width:650px){main{padding-top:24px}.panel{padding:18px}textarea{min-height:130px}}
</style></head>
<body><main>
<div class="top"><div class="brand">📋 ClipHouse</div><div class="status" id="status">Подключение…</div></div>
<section class="panel"><label for="newtext">Добавить текст в общий буфер</label><textarea id="newtext" placeholder="Вставь или напиши текст здесь…" maxlength="100000"></textarea><div class="actions"><button class="primary" id="publish">Добавить в общий буфер</button><button id="clearbox">Очистить поле</button></div><div class="hint">Текст попадёт в историю и разойдётся на остальные запущенные компьютеры. Максимум 100 000 символов.</div></section>
<section class="panel"><div style="font-weight:700;margin-bottom:8px">Последний общий текст <button class="copy top-copy" id="copylatest" style="display:none">Копировать</button></div><pre id="latest" class="entry-text">Пока история пуста.</pre></section>
<section class="panel"><details id="historyDetails"><summary class="history-head"><span>История буфера</span></summary><div id="history"><div class="empty">Пока пусто.</div></div></details></section>
<div class="foot">Работает в домашней сети. Если в config.ini включено auto_capture=true, Windows-программа автоматически сохраняет скопированный текст; при false добавляй текст вручную. На телефоне можно добавить текст или скопировать его кнопкой. Не открывай этот порт в интернет на роутере.</div>
</main><div class="toast" id="toast"></div>
<script>
const $=id=>document.getElementById(id);let knownTop='';let isMobile=/Android|iPhone|iPad|iPod|Mobile/i.test(navigator.userAgent)||(navigator.maxTouchPoints>1&&/Macintosh/i.test(navigator.userAgent));
function esc(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function toast(msg){const t=$('toast');t.textContent=msg;t.style.display='block';clearTimeout(window._toastTimer);window._toastTimer=setTimeout(()=>t.style.display='none',1900)}
async function postText(text){const r=await fetch('/api/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({text})});const d=await r.json();if(!r.ok||!d.ok)throw new Error(d.error||'Не удалось отправить текст');return d}
async function copyText(text){try{let copyPromise;if(navigator.clipboard&&navigator.clipboard.writeText){copyPromise=navigator.clipboard.writeText(text)}else{const x=document.createElement('textarea');x.value=text;x.style.position='fixed';x.style.opacity='0';document.body.appendChild(x);x.focus();x.select();const ok=document.execCommand('copy');x.remove();if(!ok)throw new Error('Браузер не разрешил копирование');copyPromise=Promise.resolve()}
 if(isMobile){await postText(text)}await copyPromise;toast('Скопировано');}catch(e){const x=document.createElement('textarea');x.value=text;x.style.position='fixed';x.style.opacity='1';x.style.left='10px';x.style.top='10px';document.body.appendChild(x);x.focus();x.select();toast('Выделил текст — нажми «Копировать»');}}
$('publish').addEventListener('click',async()=>{const text=$('newtext').value;if(!text.trim()){toast('Сначала введи текст');return}const b=$('publish');b.disabled=true;try{await postText(text);$('newtext').value='';toast('Добавлено в общий буфер')}catch(e){toast(e.message)}finally{b.disabled=false;loadState()}});
$('clearbox').addEventListener('click',()=>$('newtext').value='');
$('copylatest').addEventListener('click',()=>copyText(knownTop));
function render(data){const entries=data.entries||[];const top=entries[0];$('status').innerHTML=esc(data.host||'Этот компьютер')+'<br>Узлов в сети: '+Number(data.peer_count||0);if(top){knownTop=top.text;$('latest').textContent=top.text;$('copylatest').style.display='inline-block'}else{$('latest').textContent='Пока история пуста.';$('copylatest').style.display='none'}
 const root=$('history');if(!entries.length){root.innerHTML='<div class="empty">Пока пусто. Скопированный на компьютере текст появится здесь автоматически.</div>';return}
 root.innerHTML=entries.map((e,i)=>{const dt=new Date(e.ts);const when=dt.toLocaleDateString()+' '+dt.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'});const preview=e.text;return '<article class="entry"><div class="entry-meta"><span>'+esc(when)+' · '+esc(e.device||'Компьютер')+(i===0?' · последний':'')+'</span><span>'+String(e.text.length)+' симв.</span></div><pre class="entry-text">'+esc(preview)+'</pre><div class="entry-actions"><button class="copy" data-copy="'+i+'">Копировать</button></div></article>'}).join('');root.querySelectorAll('[data-copy]').forEach(btn=>btn.addEventListener('click',()=>copyText(entries[Number(btn.dataset.copy)].text)))}
async function loadState(){try{const r=await fetch('/api/state?now='+Date.now(),{cache:'no-store'});const d=await r.json();render(d)}catch(e){$('status').textContent='Нет связи с сервером'}}
loadState();setInterval(loadState,2200);
</script></body></html>'''


class ClipHouseHandler(BaseHTTPRequestHandler):
    server_version = "ClipHouse/1.0"

    def log_message(self, fmt, *args):
        # Keep the console quiet; important errors are printed explicitly elsewhere.
        return

    def _send(self, code, body, content_type="application/json; charset=utf-8", headers=None):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
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
            raise ValueError("Размер запроса должен быть от 1 байта до 2 МБ")
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            raise ValueError("Некорректный JSON")

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/" or parsed.path == "/index.html":
            self._send(200, HTML_PAGE, "text/html; charset=utf-8")
            return
        if parsed.path in ("/api/state", "/api/sync"):
            entries = _snapshot_events()
            if parsed.path == "/api/sync":
                query = urllib.parse.parse_qs(parsed.query)
                have_raw = query.get("have", [""])[0]
                have = set(have_raw.split(",")) if have_raw else set()
                entries = [event for event in entries if event["id"] not in have]
            with _peers_lock:
                now = time.monotonic()
                peer_names = [p["name"] for p in _peers.values() if now - p["last_seen"] <= PEER_TIMEOUT]
            self._json(200, {"ok": True, "node_id": NODE_ID, "host": HOST_NAME, "peer_count": len(peer_names), "peers": peer_names, "entries": entries})
            return
        if parsed.path == "/api/health":
            self._json(200, {"ok": True, "app": APP_NAME, "node_id": NODE_ID})
            return
        self._json(404, {"ok": False, "error": "Не найдено"})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/api/add":
            self._json(404, {"ok": False, "error": "Не найдено"})
            return
        try:
            data = self._read_json()
            if not isinstance(data, dict):
                raise ValueError("Некорректные данные")
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
    _load_events()
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
    print("=" * 58)
    print("ClipHouse запущен на компьютере: %s" % HOST_NAME)
    print("Открой на этом ПК: http://127.0.0.1:%s/" % HTTP_PORT)
    print("В локальной сети:  http://%s:%s/" % (ip, HTTP_PORT))
    print("Файл истории:      %s" % HISTORY_PATH)
    print("Автозахват буфера: %s" % ("ВКЛ" if AUTO_CAPTURE_CLIPBOARD else "ВЫКЛ (добавление через сайт)"))
    print("Лимит истории:     %s записей на узел" % MAX_HISTORY)
    print("Автообнаружение узлов: UDP %s; синхронизация истории: включена" % DISCOVERY_PORT)
    print("Настройки:         %s" % CONFIG_PATH)
    print("Оставь это окно запущенным. Ctrl+C в ЭТОМ окне останавливает сервер;")
    print("в других программах Ctrl+C продолжает обычное копирование.")
    print("Не открывай порты ClipHouse в интернет на роутере.")
    print("=" * 58)
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nClipHouse завершает работу…")
        if _http_server:
            _http_server.shutdown()
            _http_server.server_close()


if __name__ == "__main__":
    main()
