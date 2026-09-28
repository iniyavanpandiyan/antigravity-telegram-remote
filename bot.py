#!/usr/bin/env python3
"""
Antigravity (agy) Telegram bridge -- customized.

Drives the official `agy` CLI (headless, stream-json) so a Telegram chat gets a
near-realtime agent experience with:
  * live answer streaming (one message edited as tokens arrive)
  * tool-call / step notifications
  * multiple named + pinned sessions per chat (switch via inline buttons)
  * model picker (inline buttons, grouped)
  * MCQ / option detection -> rendered as tappable inline buttons
  * per-session reasoning effort + workspace
  * IMPORT your IDE (editor) sessions: the CLI binary and the IDE editor keep
    conversations in SEPARATE store dirs, so we symlink the CLI data dirs
    (conversations, brain, implicit) onto the IDE store. `agy` then reads/writes
    the SAME physical .db the editor uses, so Telegram and your laptop IDE share
    one live history (cloud sync propagates both ways). No copy, no source flip.

Dedicated bot (@antigravityfiip_bot), polled via getUpdates (message + callback_query).
"""
import os, sys, json, sqlite3, subprocess, threading, time, shutil, re, pty, select, signal, unicodedata, ssl, http.client
from collections import Counter

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
from ide_preview import ide_preview

def import_reply(name, ide_id):
    """Concise success note for an IDE import (no long context dump)."""
    return (f"✅ Linked <b>{_html_name(name)}</b> to your IDE.\n"
            f"Your next message continues that thread; the IDE shows it too.")

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE, "config.json")
DB_PATH = os.path.join(BASE, "antigravity.db")

cfg = json.load(open(CONFIG_PATH))
TOKEN = cfg.get("telegramBotToken", "")
ALLOWED = set(str(u) for u in cfg.get("allowedUserIds", []))
DEFAULT_WORKSPACE = cfg.get("workspaceBaseDir") or os.path.expanduser("~")
DEFAULT_MODEL = cfg.get("defaultModel", "gemini-3.1-pro-low")
# Fallback chat used only to seed the very first session on a fresh database.
DEFAULT_CHAT = sorted(ALLOWED)[0] if ALLOWED else None

if not TOKEN or TOKEN.endswith(":***") or len(TOKEN) < 20:
    sys.exit("ERROR: config.json telegramBotToken missing/placeholder.")

AGY = shutil.which("agy") or os.path.expanduser("~/.local/bin/agy")
API = f"https://api.telegram.org/bot{TOKEN}"

# agy conversation stores: the CLI binary and the IDE editor keep data in
# SEPARATE dirs. We make the CLI dirs SYMLINKS to the IDE dirs so both binaries
# read/write the SAME physical conversation .db — that is what lets Telegram and
# the laptop IDE share one live thread (cloud sync propagates both ways).
GEMINI_DIR = os.path.expanduser("~/.gemini")
CLI_ROOT = os.path.join(GEMINI_DIR, "antigravity-cli")
IDE_ROOT = os.path.join(GEMINI_DIR, "antigravity-ide")
CLI_CONV_DIR = os.path.join(CLI_ROOT, "conversations")
USER_CONV_DIR = os.path.join(IDE_ROOT, "conversations")

# model catalog: dynamic discovery via `agy models` with caching
_cached_models = None
_cached_models_ts = 0
_models_lock = threading.Lock()

def get_models():
    global _cached_models, _cached_models_ts
    with _models_lock:
        now = time.time()
        if _cached_models and (now - _cached_models_ts < 300):
            return list(_cached_models)
        models = []
        try:
            proc = subprocess.run([AGY, "models"], capture_output=True, text=True, timeout=8)
            if proc.returncode == 0:
                for line in proc.stdout.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(None, 1)
                    if len(parts) == 2:
                        models.append((parts[0].strip(), parts[1].strip()))
                    elif len(parts) == 1:
                        models.append((parts[0].strip(), parts[0].strip()))
        except Exception as e:
            print("get_models dynamic error:", e, file=sys.stderr)
        if not models:
            models = [
                ("gemini-3.8-flash-high",   "Gemini 3.8 Flash (High)"),
                ("gemini-3.8-flash-medium", "Gemini 3.8 Flash (Medium)"),
                ("gemini-3.8-flash-low",    "Gemini 3.8 Flash (Low)"),
                ("gemini-3.7-flash-high",   "Gemini 3.7 Flash (High)"),
                ("gemini-3.7-flash-medium", "Gemini 3.7 Flash (Medium)"),
                ("gemini-3.7-flash-low",    "Gemini 3.7 Flash (Low)"),
                ("gemini-3.6-flash-high",   "Gemini 3.6 Flash (High)"),
                ("gemini-3.1-pro-high",     "Gemini 3.1 Pro (High)"),
                ("claude-sonnet-4-6",       "Claude Sonnet 4.6 (Thinking)"),
                ("claude-opus-4-6-thinking","Claude Opus 4.6 (Thinking)"),
                ("gpt-oss-120b-medium",     "GPT-OSS 120B (Medium)"),
            ]
        _cached_models = models
        _cached_models_ts = now
        return list(_cached_models)

MODELS = get_models()
MODEL_NAMES = {m[0]: m[1] for m in MODELS}

def get_model_name(mid):
    names = {m[0]: m[1] for m in get_models()}
    return names.get(mid, mid)

db = sqlite3.connect(DB_PATH, check_same_thread=False)
db.execute("""CREATE TABLE IF NOT EXISTS sessions(
    chat_id TEXT, name TEXT, conv_id TEXT, model TEXT, workspace TEXT,
    source TEXT DEFAULT 'local', ide_id TEXT,
    PRIMARY KEY(chat_id, name))""")
db.execute("""CREATE TABLE IF NOT EXISTS active(chat_id TEXT PRIMARY KEY, name TEXT)""")
db.execute("""CREATE TABLE IF NOT EXISTS prefs(chat_id TEXT PRIMARY KEY, effort TEXT)""")
# Per-session chat history the BOT records as turns happen. This is what drives
# "/resume" + "/history" — a self-owned, reliable log (independent of agy's
# protobuf-on-disk steps, which are opaque to parse for display).
db.execute("""CREATE TABLE IF NOT EXISTS threads(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id TEXT, session TEXT, role TEXT, text TEXT, ts REAL DEFAULT 0)""")
# Durable forwarder cursor: last IDE step idx already delivered to Telegram,
# per conv_id. Persisted so a bot RESTART does not reseed the cursor to
# MAX(idx) and silently skip everything the IDE produced while we were down.
db.execute("""CREATE TABLE IF NOT EXISTS bridge_cursor(
    conv_id TEXT PRIMARY KEY, last_idx INTEGER DEFAULT -1)""")
# migrate existing installs that predate the source/ide_id/title columns
for col, sql in (("source", "ALTER TABLE sessions ADD COLUMN source TEXT DEFAULT 'local'"),
                 ("ide_id", "ALTER TABLE sessions ADD COLUMN ide_id TEXT"),
                 ("title", "ALTER TABLE sessions ADD COLUMN title TEXT"),
                 ("bridge", "ALTER TABLE sessions ADD COLUMN bridge INTEGER DEFAULT 0"),
                 ("last_telegram_send", "ALTER TABLE sessions ADD COLUMN last_telegram_send REAL DEFAULT 0"),
                 ("tokens_in", "ALTER TABLE sessions ADD COLUMN tokens_in INTEGER DEFAULT 0"),
                 ("tokens_out", "ALTER TABLE sessions ADD COLUMN tokens_out INTEGER DEFAULT 0"),
                 ("tokens_total", "ALTER TABLE sessions ADD COLUMN tokens_total INTEGER DEFAULT 0")):
    try:
        db.execute(f"SELECT {col} FROM sessions LIMIT 1")
    except sqlite3.OperationalError:
        db.execute(sql)
db.commit()

def all_sessions(chat_id):
    return db.execute(
        "SELECT name,conv_id,model,workspace,source,ide_id,title FROM sessions "
        "WHERE chat_id=? ORDER BY rowid", (chat_id,)).fetchall()

def active_name(chat_id):
    r = db.execute("SELECT name FROM active WHERE chat_id=?", (chat_id,)).fetchone()
    return r[0] if r else None

def get_session(chat_id, name=None):
    name = name or active_name(chat_id)
    if name:
        row = db.execute(
            "SELECT conv_id,model,workspace,source,ide_id,title FROM sessions "
            "WHERE chat_id=? AND name=?", (chat_id, name)).fetchone()
        if row:
            return {"name": name, "conv_id": row[0], "model": row[1] or DEFAULT_MODEL,
                    "workspace": row[2] or DEFAULT_WORKSPACE,
                    "source": row[3] or "local", "ide_id": row[4], "title": row[5]}
    return {"name": name or "default", "conv_id": None, "model": DEFAULT_MODEL,
            "workspace": DEFAULT_WORKSPACE, "source": "local", "ide_id": None, "title": None}

def save_session(chat_id, name, conv_id, model, workspace):
    db.execute("INSERT INTO sessions(chat_id,name,conv_id,model,workspace) VALUES(?,?,?,?,?) "
               "ON CONFLICT(chat_id,name) DO UPDATE SET conv_id=excluded.conv_id,"
               "model=excluded.model, workspace=excluded.workspace",
               (chat_id, name, conv_id, model, workspace))
    db.execute("INSERT INTO active(chat_id,name) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET name=excluded.name",
               (chat_id, name))
    db.commit()

def new_session(chat_id, name=None, model=DEFAULT_MODEL, workspace=DEFAULT_WORKSPACE):
    n = name or f"session-{len(all_sessions(chat_id))+1}"
    save_session(chat_id, n, None, model, workspace)
    return n

def set_active(chat_id, name):
    db.execute("INSERT INTO active(chat_id,name) VALUES(?,?) ON CONFLICT(chat_id) DO UPDATE SET name=excluded.name",
               (chat_id, name))
    db.commit()

def get_effort(chat_id):
    r = db.execute("SELECT effort FROM prefs WHERE chat_id=?", (chat_id,)).fetchone()
    return r[0] if r else None

# ---- self-owned chat history (drives /resume + /history) ----
def log_turn(chat_id, session, role, text):
    db.execute("INSERT INTO threads(chat_id,session,role,text,ts) VALUES(?,?,?,?,?)",
               (chat_id, session, role, (text or "")[:8000], time.time()))
    db.commit()

def recent_turns(chat_id, session, n=8):
    rows = db.execute(
        "SELECT role,text FROM threads WHERE chat_id=? AND session=? "
        "ORDER BY id DESC LIMIT ?", (chat_id, session, n)).fetchall()
    return list(reversed(rows))

def send_history(chat_id, session, n=8):
    rows = recent_turns(chat_id, session, n=n)
    if not rows:
        send_long(chat_id, "No history yet for this session — send a message first.")
        return
    lines = [f"🕘 <b>Last {len(rows)} messages</b> (session: <code>{_h(session)}</code>)"]
    for role, text in rows:
        who = "🧑 <b>you</b>" if role == "user" else "🤖 <b>agy</b>"
        body = (text or "").strip().replace("\r", "")
        if len(body) > 700:
            body = body[:700] + "\n…"
        # escape so user/assistant text can't break HTML
        lines.append(f"{who}:\n{_h(body)}")
    send_long(chat_id, "\n\n".join(lines))

# ---- markdown/HTML helpers (Telegram) ----
# We use HTML parse_mode (not MarkdownV2): HTML only reserves < > &, so the
# unescaped . ! ( ) chars in real LLM prose render fine and we don't have to
# escape the whole message. MarkdownV2 was choking on those chars and falling
# back to a double-escaped form that printed literal "*agy*" — hence the
# "markdown shows as raw text" bug.

def _h(text):
    """Escape the 3 chars HTML reserves, so any text is safe inside a message."""
    if text is None:
        return ""
    return (str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _h_unescape(text):
    """Reverse of _h(): turn &lt; &gt; &amp; back into literal <> &.
    Used by the tag-stripping fallback so the user never sees double-escaped
    entities."""
    if text is None:
        return ""
    return (str(text).replace("&lt;", "<").replace("&gt;", ">")
            .replace("&amp;", "&"))

def html(msg, *parts):
    """Build an HTML message from safe (already-escaped) literal fragments.
    Use the <b>/<code>/<pre>/<i> tags directly in msg; wrap dynamic content
    with _h() before interpolating."""
    return msg

_md_bullet = re.compile(r"^\s*[-*+]\s+(.*)$")
_md_ol = re.compile(r"^(\s*\d+[.)])\s+(.*)$")
_md_hr = re.compile(r"^\s*([-*_])\1{2,}\s*$")
_md_table_sep = re.compile(r"^\s*\|?[\s:|-]+\|?\s*$")
_md_block = re.compile(r"^\s*(```|~~~)")
_md_fence = re.compile(r"^\s*(```|~~~)(.*)$")
_md_head = re.compile(r"^(#{1,6})\s+(.*)$")
_md_quote = re.compile(r"^\s*>\s?(.*)$")

def _inline_md(s):
    """Apply inline markdown to one line / one cell.

    1) Inline-code spans (`...`) are extracted FIRST and held in placeholders so
    underscores / asterisks INSIDE code (e.g. `grep_search`, `replace_file_content`)
    are never mis-read as bold/italic by later rules.

    2) Markdown links ([text](url)) across ANY scheme (https, http, file, tg, relative)
    are stashed so their URLs are protected from emphasis rules and internal formatting
    inside labels is properly rendered. file:// URLs are mapped to code chips because
    Telegram Bot API strictly rejects non-HTTP(S)/tg protocols.

    3) Bare URLs (https?:// or file://) are stashed with trailing markdown delimiters
    (*, _, ~, `) and trailing punctuation stripped and preserved outside the link,
    preventing links from swallowing trailing `**` or `**/` and corrupting message tags.

    4) Standard emphasis (bold, italic, strikethrough) is applied safely.
    5) Link and code placeholders are restored as valid HTML tags.
    """
    if not s:
        return s
    # 1) stash inline-code spans before anything can mangle their contents
    code_spans = []
    def _stash_code(m):
        code_spans.append(m.group(1))
        return f"\x00{len(code_spans) - 1}\x00"
    s = re.sub(r"`([^`]+)`", _stash_code, s)

    # 2) stash markdown links: [text](url)
    links = []
    def _stash_link(m):
        label = m.group(1)
        raw_url = m.group(2).strip()
        trailing = ""
        while raw_url and raw_url[-1] in "*_~,.;:!?'\")]>":
            trailing = raw_url[-1] + trailing
            raw_url = raw_url[:-1]
        if raw_url.endswith("**/"):
            raw_url = raw_url[:-3]
            trailing = "**/" + trailing
        elif raw_url.endswith("/**"):
            raw_url = raw_url[:-3]
            trailing = "/**" + trailing

        lbl = _h(label)
        lbl = re.sub(r"\*\*\*(.+?)\*\*\*", r"<b><i>\1</i></b>", lbl)
        lbl = re.sub(r"___(.+?)___", r"<b><i>\1</i></b>", lbl)
        lbl = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", lbl)
        lbl = re.sub(r"__(.+?)__", r"<b>\1</b>", lbl)
        lbl = re.sub(r"(?<!\*)\*(?!\*)(.+?)\*(?!\*)", r"<i>\1</i>", lbl)
        lbl = re.sub(r"(?<!_)_(?!_)(.+?)_(?!_)", r"<i>\1</i>", lbl)
        lbl = re.sub(r"~~(.+?)~~", r"<s>\1</s>", lbl)

        # Telegram Bot API only allows http://, https://, and tg:// in <a href="...">
        if raw_url.startswith("file://"):
            frag = f'<code>{lbl}</code>'
        elif raw_url.startswith(("http://", "https://", "tg://")):
            frag = f'<a href="{_h(raw_url)}">{lbl}</a>'
        else:
            frag = f'<code>{lbl}</code>'
        links.append(frag)
        return f"\x02{len(links) - 1}\x02" + trailing

    s = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", _stash_link, s)

    # 3) stash bare URLs: https?:// or file://
    def _stash_url(m):
        raw = m.group(0)
        trailing = ""
        while raw and raw[-1] in "*_~,.;:!?'\")]>":
            trailing = raw[-1] + trailing
            raw = raw[:-1]
        if raw.endswith("**/"):
            raw = raw[:-3]
            trailing = "**/" + trailing
        elif raw.endswith("/**"):
            raw = raw[:-3]
            trailing = "/**" + trailing
        if not raw:
            return m.group(0)
        if raw.startswith("file://"):
            frag = f'<code>{_h(raw)}</code>'
        else:
            frag = f'<a href="{_h(raw)}">{_h(raw)}</a>'
        links.append(frag)
        return f"\x02{len(links) - 1}\x02" + trailing

    s = re.sub(r"(?:https?|file)://[^\s<>\x00\x02)]+", _stash_url, s)

    # 4) escape plain text outside code/links as HTML safe
    s = _h(s)

    # 5) bold-italic ***x*** / ___x___
    s = re.sub(r"\*\*\*(.+?)\*\*\*", r"<b><i>\1</i></b>", s)
    s = re.sub(r"___(.+?)___", r"<b><i>\1</i></b>", s)
    # bold **x** / __x__
    s = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", s)
    s = re.sub(r"__(.+?)__", r"<b>\1</b>", s)
    # italic *x* / _x_
    s = re.sub(r"(?<!\*)\*(?!\*)(.+?)\*(?!\*)", r"<i>\1</i>", s)
    s = re.sub(r"(?<!_)_(?!_)(.+?)_(?!_)", r"<i>\1</i>", s)
    # strikethrough ~~x~~
    s = re.sub(r"~~(.+?)~~", r"<s>\1</s>", s)

    # 6) restore link placeholders
    s = re.sub(r"\x02(\d+)\x02", lambda m: links[int(m.group(1))], s)

    # 7) restore code spans
    s = re.sub(r"\x00(\d+)\x00", lambda m: f"<code>{_h(code_spans[int(m.group(1))])}</code>", s)
    return s

def _strip_tags(t):
    """Remove inline HTML tags/code markers for width measurement."""
    return re.sub(r"<[^>]+>", "", t or "")

def _render_table(rows):
    """Render a markdown table (list of lists) as a Telegram native rich HTML table.

    Telegram Bot API 10.1+ natively supports rich HTML tables (<table bordered striped>)
    via sendRichMessage / editMessageText(rich_message=...), rendering spectacular
    bordered, zebra-striped, column-aligned interactive tables across Desktop, iOS,
    Android, and Web clients.
    """
    if not rows:
        return ""
    ncols = max(len(r) for r in rows)
    hdr = rows[0]
    lines = ["<table bordered striped>"]
    lines.append("<tr>" + "".join(f"<th>{_inline_md(c.strip())}</th>" for c in (hdr + [""] * (ncols - len(hdr)))) + "</tr>")
    for r in rows[1:]:
        cells = r + [""] * (ncols - len(r))
        lines.append("<tr>" + "".join(f"<td>{_inline_md(c.strip())}</td>" for c in cells) + "</tr>")
    lines.append("</table>")
    return "\n".join(lines)

def md_to_html(md):
    """Convert GitHub-flavoured markdown to Telegram-safe HTML.
    Supports: headings, bold/italic/strike/inline-code, fenced code blocks,
    blockquotes, unordered/ordered lists, tables, horizontal rules, paragraphs.
    Unknown constructs fall back to escaped plain text (never raw, so it can't
    break the HTML parse)."""
    if not md:
        return ""
    lines = md.replace("\r\n", "\n").split("\n")
    out = []
    i = 0
    in_code = False
    code_buf = []
    code_lang = ""
    def close_lists():
        # Telegram HTML does NOT support <ul>/<ol>/<li>/<hr>, so we render list
        # items as plain bullets/dashes and never open/close those tags.
        pass
    while i < len(lines):
        line = lines[i]
        if _md_fence.match(line):
            if in_code:
                # end fence
                out.append(f'<pre><code class="language-{code_lang or "sh"}">{_h("".join(code_buf))}</code></pre>')
                in_code = False
                code_buf = []
            else:
                in_code = True
                m = _md_fence.match(line)
                code_lang = (m.group(2) or "").strip()
                code_buf = []
            i += 1
            continue
        if in_code:
            code_buf.append(line + "\n")
            i += 1
            continue
        if not line.strip():
            close_lists()
            i += 1
            continue
        if _md_hr.match(line):
            close_lists()
            out.append("──────────")
            i += 1
            continue
        mh = _md_head.match(line)
        if mh:
            close_lists()
            level = len(mh.group(1))
            title = _inline_md(mh.group(2))
            if out and out[-1] != "":
                out.append("")
            out.append(f"<b>{title}</b>")
            i += 1
            continue
        mq = _md_quote.match(line)
        if mq:
            close_lists()
            # gather consecutive quote lines
            qbuf = [mq.group(1)]
            j = i + 1
            while j < len(lines) and _md_quote.match(lines[j]):
                qbuf.append(_md_quote.match(lines[j]).group(1))
                j += 1
            first_line = qbuf[0].strip()
            alert_match = re.match(r"^\[!(NOTE|TIP|IMPORTANT|WARNING|CAUTION)\]\s*(.*)$", first_line, re.I)
            if alert_match:
                atype = alert_match.group(1).upper()
                arest = alert_match.group(2).strip()
                icon_map = {
                    "NOTE": "ℹ️ <b>Note</b>",
                    "TIP": "💡 <b>Tip</b>",
                    "IMPORTANT": "⚠️ <b>Important</b>",
                    "WARNING": "⚠️ <b>Warning</b>",
                    "CAUTION": "🛑 <b>Caution</b>",
                }
                label = icon_map.get(atype, f"<b>{atype}</b>")
                rest_lines = ([arest] if arest else []) + qbuf[1:]
                body = "\n".join(_inline_md(l) for l in rest_lines if l.strip())
                content = f"{label}\n{body}" if body else label
                out.append(f"<blockquote>{content}</blockquote>")
            else:
                body = "\n".join(_inline_md(l) for l in qbuf if l.strip())
                out.append(f"<blockquote>{body}</blockquote>")
            i = j
            continue
        # table detection: header row | a | b | then separator row
        if line.strip().startswith("|") and i + 1 < len(lines) and _md_table_sep.match(lines[i+1]):
            close_lists()
            hdr = [c.strip() for c in line.strip().strip("|").split("|")]
            i += 2
            rows = [hdr]
            while i < len(lines) and lines[i].strip().startswith("|") and not _md_table_sep.match(lines[i]):
                rows.append([c.strip() for c in lines[i].strip().strip("|").split("|")])
                i += 1
            out.append(_render_table(rows))
            continue
        mb = _md_bullet.match(line)
        if mb:
            bt = mb.group(1)
            if re.match(r"\[\s\]", bt):
                out.append("☐ " + _inline_md(bt[3:].strip()))
            elif re.match(r"\[[xX]\]", bt):
                out.append("☑ " + _inline_md(bt[3:].strip()))
            else:
                out.append("• " + _inline_md(bt))
            i += 1
            continue
        mo = _md_ol.match(line)
        if mo:
            marker = mo.group(1).strip()
            content = mo.group(2).strip()
            out.append(f"{marker} {_inline_md(content)}")
            i += 1
            continue
        # plain paragraph line
        close_lists()
        out.append(_inline_md(line))
        i += 1
    if in_code:
        out.append(f'<pre><code class="language-{code_lang or "sh"}">{_h("".join(code_buf))}</code></pre>')
    close_lists()
    return "\n".join(out).strip()

def _html_safe_chunks(text, limit=3800):
    """Split HTML text into <=limit-char chunks WITHOUT cutting inside a tag
    (which would make Telegram reject the parse and drop the message)."""
    chunks, t = [], text or ""
    while t:
        if len(t) <= limit:
            chunks.append(t); break
        cut = t.rfind("\n", 0, limit)
        if cut <= 0:
            cut = t.rfind(">", 0, limit)   # cut right after a tag boundary
            if cut > 0:
                cut += 1
        if cut <= 0:
            cut = limit
        seg = t[:cut]
        rest = t[cut:]
        # --- never split inside a <pre> block ---
        p = seg.rfind("<pre>")
        if p != -1 and "</pre>" not in seg[p:]:
            close = t.find("</pre>", p)
            if close != -1 and (close + len("</pre>")) <= limit:
                end = close + len("</pre>")
                seg = t[:end]
                rest = t[end:]
            elif p > 0:
                seg = t[:p].rstrip("\n")
                rest = t[p:]
            else:
                cs = t.find("<code", p)
                content_start = (t.index(">", cs) + 1) if cs != -1 else (p + len("<pre>"))
                budget = limit - content_start - len("</code></pre>")
                ic = t.rfind("\n", content_start, content_start + max(budget, 1))
                if ic < content_start:
                    ic = limit - 20
                marker = "</code></pre>"
                seg = t[:ic] + marker
                rest = "<pre><code>" + t[ic:]
        # --- never split inside a <table> block ---
        open_tbl = seg.rfind("<table")
        if open_tbl != -1 and "</table>" not in seg[open_tbl:]:
            end_tbl = t.find("</table>", open_tbl)
            if end_tbl != -1 and (end_tbl + len("</table>")) <= limit:
                end = end_tbl + len("</table>")
                seg = t[:end]
                rest = t[end:]
            elif open_tbl > 0:
                seg = t[:open_tbl].rstrip("\n")
                rest = t[open_tbl:]
            else:
                ic = t.rfind("</tr>", open_tbl, limit - 10)
                if ic != -1:
                    seg = t[:ic + len("</tr>")] + "</table>"
                    rest = "<table>" + t[ic + len("</tr>"):]
        # --- never split inside a <blockquote> block ---
        open_bq = seg.rfind("<blockquote")
        if open_bq != -1 and "</blockquote>" not in seg[open_bq:]:
            end_bq = t.find("</blockquote>", open_bq)
            if end_bq != -1 and (end_bq + len("</blockquote>")) <= limit:
                end = end_bq + len("</blockquote>")
                seg = t[:end]
                rest = t[end:]
            elif open_bq > 0:
                seg = t[:open_bq].rstrip("\n")
                rest = t[open_bq:]
            else:
                ic = t.rfind("\n", open_bq, limit - 20)
                if ic <= open_bq:
                    ic = limit - 20
                seg = t[:ic] + "</blockquote>"
                rest = "<blockquote>" + t[ic:]
        # --- never split inside an <a>...</a> link element ---
        open_a = seg.rfind("<a ")
        if open_a != -1 and "</a>" not in seg[open_a:]:
            end = t.find("</a>", open_a)
            if end != -1 and (end + len("</a>")) <= limit:
                end_a = end + len("</a>")
                seg = t[:end_a]
                rest = t[end_a:]
            elif open_a > 0:
                seg = t[:open_a]
                rest = t[open_a:]
        # --- never split a NATIVE GFM pipe-table across messages (unless the
        # whole contiguous run is itself too big to fit one message, in which
        # case it degrades to plain pipe-text lines rather than a rejected
        # >4096-char message). ---
        elif _looks_like_table_line(seg.rstrip("\n").split("\n")[-1] if seg.rstrip("\n") else ""):
            after = t[cut:]
            pos = 1 if after.startswith("\n") else 0   # skip one leading newline
            absorbed_any = False
            new_cut = cut
            while pos < len(after):
                nl = after.find("\n", pos)
                end = nl if nl != -1 else len(after)
                line = after[pos:end].rstrip("\r")
                if not _looks_like_table_line(line):
                    break
                if (cut + pos) > limit:
                    # table too big to fit — revert to a prose boundary instead
                    rb = t[:cut].rfind("\n")
                    new_cut = (rb + 1) if rb != -1 else cut
                    break
                absorbed_any = True
                if nl == -1:
                    new_cut = len(t)
                    break
                pos = nl + 1
            if absorbed_any and new_cut != len(t):
                new_cut = cut + pos
            seg = seg if new_cut == cut else t[:new_cut]
            rest = t[new_cut:]
        chunks.append(seg); t = rest
    return chunks


def _looks_like_table_line(line):
    """True if a line is part of a GFM pipe-table (has '|' separators or the
    '---' separator row). Used to keep native tables intact across chunking."""
    s = (line or "").strip()
    if not s:
        return False
    # a separator row like |---|---|
    if re.match(r"^\|[\s:\-|]*\|$", s) and "-" in s:
        return True
    return s.startswith("|") and "|" in s[1:]


def _strip_tags_to_text(html):
    """Last-resort fallback: turn a malformed HTML string into clean readable
    text. We REMOVE tags (not escape them) so the user never sees literal
    `&lt;b&gt;` — they just lose formatting instead of seeing garbage."""
    if not html:
        return ""
    out = re.sub(r"</?(?:b|i|s|u|code|pre|a|em|strong|blockquote|tg-spoiler)[^>]*>", "", html)
    out = re.sub(r"<table[^>]*>", "", out, flags=re.I)
    out = re.sub(r"</table>", "\n", out, flags=re.I)
    out = re.sub(r"</tr>", "\n", out, flags=re.I)
    out = re.sub(r"</td>", " | ", out, flags=re.I)
    out = re.sub(r"</th>", " | ", out, flags=re.I)
    out = re.sub(r"<[^>]+>", "", out)
    return _h_unescape(out)


def _has_rich(text):
    if not text:
        return False
    return "<table" in text or "<details" in text or "<tg-" in text


def _md_send(chat_id, text, reply_markup=None):
    """Send rich messages natively (Bot API 10.1 sendRichMessage) when structured
    constructs like <table bordered striped> are present; fall back to HTML parse_mode
    chunking on older clients / standard text."""
    text = text or ""
    if not text.strip() and not reply_markup:
        return None
    if _has_rich(text):
        payload = {"chat_id": chat_id, "rich_message": {"html": text}}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        r = tg("sendRichMessage", **payload)
        if r.get("ok"):
            return r
    chunks = _html_safe_chunks(text)
    last = None
    for i, c in enumerate(chunks):
        kw = {"chat_id": chat_id, "text": c, "parse_mode": "HTML"}
        if i == 0 and reply_markup:
            kw["reply_markup"] = reply_markup
        r = tg("sendMessage", **kw)
        if not r.get("ok"):
            kw2 = {"chat_id": chat_id, "text": _strip_tags_to_text(c)}
            if i == 0 and reply_markup:
                kw2["reply_markup"] = reply_markup
            last = tg("sendMessage", **kw2)
        else:
            last = r
    return last


def _send_doc(chat_id, text, filename, caption=None, reply_markup=None):
    """Send a markdown/text file as a Telegram document — lossless for long
    plan/task files. Falls back to a chunked message on any failure."""
    import json as _json
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption[:1000]
    if reply_markup:
        data["reply_markup"] = _json.dumps(reply_markup)
    try:
        r = requests.post(f"{API}/sendDocument", data=data,
                          files={"document": (filename, text.encode("utf-8"), "text/markdown")},
                          timeout=60).json()
    except Exception as e:
        print("tg_file error", e, file=sys.stderr)
        r = {}
    if not r.get("ok"):
        _md_send(chat_id, f"{caption or filename}\n{text[:3800]}", reply_markup)
    return r

# Empty keyboard payload: Telegram only clears an existing inline keyboard
# when reply_markup is passed explicitly, so finished messages must send this
# (otherwise the 🛑 Abort button survives under the final answer).
_NO_KB = {"inline_keyboard": []}

def _md_edit(chat_id, message_id, text, reply_markup=None):
    if message_id is None:
        return {}

    def _nm(x):
        return "not modified" in str(x.get("description", "")).lower()

    if _has_rich(text):
        payload = {"chat_id": chat_id, "message_id": message_id, "rich_message": {"html": text}}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        r = tg("editMessageText", **payload)
        if r.get("ok") or _nm(r):
            return r
    r = tg("editMessageText", chat_id=chat_id, message_id=message_id,
           text=(text or "")[:4000], parse_mode="HTML", **({"reply_markup": reply_markup}
                                                           if reply_markup else {}))
    if _nm(r):
        # Already identical — return instead of falling through to the
        # plain-text fallback, which would strip the HTML that's in place.
        return r
    if not r.get("ok") and reply_markup:
        # The keyboard payload was rejected — retry the same HTML without it
        # before ever falling back to stripped plain text (which loses formatting).
        r = tg("editMessageText", chat_id=chat_id, message_id=message_id,
               text=(text or "")[:4000], parse_mode="HTML")
        if _nm(r):
            return r
    if not r.get("ok"):
        # HTML rejected — fall back to clean PLAIN text (strip tags), never
        # double-escape, so the user never sees literal &lt;b&gt; garbage.
        r = tg("editMessageText", chat_id=chat_id, message_id=message_id,
              text=_strip_tags_to_text(text)[:4000])
    return r

# ============================================================================
# OpenCode-style code-diff rendering
#
# Mirrors how the opencode-telegram-bot shows code edits in Telegram:
#   • Code / diffs are NEVER rendered inline as HTML <pre> (that is what
#     produced the &lt; &gt; junk). They are shipped as a clean .txt DOCUMENT
#     ATTACHMENT, exactly like opencode (sendDocument with text/plain).
#   • The message itself is a ONE-LINE icon caption, e.g.
#        ✏️ edit src/bot.py (+12 -3)
#     — no prose, no escaping, no truncation.
#   • The diff is normalized the same way opencode does: it strips the
#     @@/---/+++/Index: noise and aligns the +/- markers so it reads clean.
# ============================================================================
import difflib as _difflib

def _normalize_edit(text):
    """OpenCode's normalize_edit: strip the unified-diff header noise
    (@@, ---, +++, Index:) so only the aligned +/- code lines remain."""
    if not text:
        return ""
    lines = text.split("\n")
    out = []
    i = 0
    n = len(lines)
    while i < n:
        nl = lines[i]
        if nl.startswith("@@"):
            # Consume the @@ line and any adjacent --- / +++ file markers
            # (difflib emits them on either side of @@).
            i += 1
            while i < n and (lines[i].startswith("---") or lines[i].startswith("+++")):
                i += 1
            continue
        if nl.startswith("Index:"):
            # Skip Index: + the ==== separator line.
            i += 1
            if i < n and set(lines[i]) <= set("="):
                i += 1
            continue
        if nl.startswith("---") or nl.startswith("+++"):
            # Standalone --- / +++ header line (file marker) → skip.
            i += 1
            continue
        if not nl.startswith("+") and not nl.startswith("-") and not nl.startswith(" "):
            # A context/removed/added line was not prefixed when emitted
            # (e.g. a raw snippet). Keep a plain context line.
            i += 1
            continue
        out.append(nl)
        i += 1
    return "\n".join(out)

def _count_diff_changes(text):
    """OpenCode's countDiffChanges: (#added, #removed) from normalized diff."""
    added = removed = 0
    for ln in (text or "").split("\n"):
        if ln.startswith("+"):
            added += 1
        elif ln.startswith("-"):
            removed += 1
    return added, removed

def _format_diff(old_text, new_text, path=""):
    """Build OpenCode's normalized unified diff string from two code blobs."""
    if old_text is None:
        old_text = ""
    if new_text is None:
        new_text = ""
    old_lines = old_text.split("\n")
    new_lines = new_text.split("\n")
    diff = _difflib.unified_diff(
        old_lines, new_lines,
        fromfile=path or "old", tofile=path or "new",
        lineterm="",
    )
    raw = "\n".join(diff)
    # Strip the @@ / --- / +++ / Index: header noise (opencode-normalized).
    return _normalize_edit(raw)

def _diff_from_tool_info(info):
    """Extract (old, new, path) from an agy tool_info / argumentsJson blob.

    agy persists the FULL new content for write_to_file (CodeContent) and
    — for replace_file_content — the old/new snippets. Build a diff from
    whichever is available; fall back to shipping the new content verbatim
    (matching opencode's full-file fallback when only CodeContent exists)."""
    if not info or not isinstance(info, dict):
        return None
    args = info.get("argumentsJson") or info.get("arguments") or {}
    if isinstance(args, str):
        try:
            # strict=False tolerates literal control chars agy sometimes emits
            args = _json.loads(args, strict=False)
        except Exception:
            args = {}
    path = (args.get("TargetPath") or args.get("FilePath") or args.get("AbsolutePath")
            or info.get("TargetPath") or info.get("AbsolutePath") or "")
    new = args.get("CodeContent") or args.get("ReplacementContent") or args.get("new_string")
    old = args.get("old_string")
    if new is None and old is None:
        return None
    if old is not None and new is not None:
        return (old, new, path, True)        # true edit → real diff
    # Only the new full content (write_to_file): ship verbatim, no diff.
    return (None, new, path, False)

def _send_code_attachment(chat_id, text, caption, filename, reply_markup=None):
    """Ship code/diff as a clean .txt document (OpenCode's anti-junk method).
    Never HTML-escapes code. Falls back to a mention-only message on failure."""
    if not text or not text.strip():
        return
    # .txt so Telegram shows it as a downloadable file, not an escaped <pre>.
    fn = filename if filename.endswith(".txt") else f"{filename}.txt"
    r = _send_doc(chat_id, text, fn, caption=caption, reply_markup=reply_markup)
    if not r.get("ok"):
        # last-resort: a short mention so the user knows something happened
        send_long(chat_id, _h(caption))
    return r

# ---- media helpers: file download + voice/image understanding ----
MEDIA_DIR = os.path.join(BASE, "media")
os.makedirs(MEDIA_DIR, exist_ok=True)

def tg_file_url(file_id):
    """Resolve a Telegram file_id to a downloadable HTTPS URL."""
    r = tg("getFile", file_id=file_id)
    if not r.get("ok"):
        return None
    fp = r.get("result", {}).get("file_path")
    if not fp:
        return None
    return f"https://api.telegram.org/file/bot{TOKEN}/{fp}"

def download_file(file_id, ext=""):
    """Download a Telegram file to MEDIA_DIR; return local path or None."""
    url = tg_file_url(file_id)
    if not url:
        return None
    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code != 200:
            return None
        suffix = ext or ""
        path = os.path.join(MEDIA_DIR, f"{int(time.time()*1000)}_{file_id[:12]}{suffix}")
        with open(path, "wb") as f:
            f.write(resp.content)
        return path
    except Exception as e:
        print("download_file error", e, file=sys.stderr)
        return None

_whisper_model = None
def transcribe_voice(path):
    """Transcribe an audio/video file to text via local faster-whisper."""
    global _whisper_model
    try:
        from faster_whisper import WhisperModel
    except Exception as e:
        return f"(whisper unavailable: {e})"
    if _whisper_model is None:
        try:
            _whisper_model = WhisperModel("base", device="cpu", compute_type="int8")
        except Exception as e:
            return f"(whisper load failed: {e})"
    try:
        segs, _ = _whisper_model.transcribe(path, beam_size=5)
        return " ".join(s.text for s in segs).strip()
    except Exception as e:
        return f"(transcribe failed: {e})"

# Media understanding (image / document) is delegated to the agent itself: the
# bot downloads the inbound file and hands the absolute path to `agy`, which
# views / reads it natively with its own multimodal model. No local Gemma
# captioning step.

# ---- IDE conversation bridge (copy IDE .db into the CLI store) ----
# The IDE editor stores conversations under antigravity-ide/conversations with
# trajectory_meta.source = 1. The CLI binary stores under antigravity-cli with
# source = 17. When agy resumes a conversation whose meta still says source=1, it
# tries to attach to the IDE's *terminal extension server* for shell integration
# (CheckTerminalShellSupport -> has_shell_integration) and the headless Telegram
# bridge has no such extension, so `run_command` fails with
# "Failed to get shell integration". We copy the IDE .db into the CLI store and
# flip the meta source 1 -> 17; agy then uses its own managed PTY shell and the
# bridge's run_command steps work. History is shared (same conversation_id), so
# Telegram and the IDE still share one thread.
def ide_link_path(ide_id):
    return os.path.join(CLI_CONV_DIR, f"{ide_id}.db")

def ensure_ide_link(ide_id):
    """Unified-store bridge (see CLI_CONV_DIR symlink above).

    The CLI `conversations` dir is now a *symlink* to the IDE store, so the CLI
    binary and the IDE editor read/write the SAME physical .db. No copy, no
    source flip, no fork. `agy` resumes the real IDE thread directly (verified
    working at trajectory_meta.source=1 — the earlier "Failed to get shell
    integration" is gone now that the antigravity-ide-server runs locally).

    This function is therefore a no-op: it only asserts the shared file exists.
    """
    if not ide_id:
        return False
    return os.path.exists(os.path.join(USER_CONV_DIR, f"{ide_id}.db"))

def remove_ide_link(ide_id):
    # In the unified-store model the CLI conversations dir IS the shared IDE
    # store, so there is nothing Telegram-private to remove. Kept as a harmless
    # no-op for call-site compatibility.
    return

def ide_title(conv_id):
    """Extract the real IDE title (e.g. 'Studio Page UI Redesign') that the
    editor shows.

    This agy version embeds the title as free text inside a length-prefixed
    framing (0x22 0x22 <len> <utf8>) — BUT the SAME framing also wraps JSON
    property values ("toolAction":"…", etc.) and even shell fragments in the
    metadata blob. We must only return a genuine human title (clean prose),
    never JSON/shell/path junk. Returns None if no genuine title is found, so
    the caller can fall back to the workspace name instead of polluting the
    session label with garbage."""
    db = os.path.join(USER_CONV_DIR, f"{conv_id}.db")
    if not os.path.exists(db):
        return None
    def _clean(t):
        # reject JSON / shell / path junk that shares the 0x22 0x22 framing
        if not t or len(t) < 3 or len(t) > 70:
            return False
        if any(ch in t for ch in '{}"\\/'):
            return False
        if any(k in t for k in (":", ";", "=", "||", "&&", "$", "sleep",
                                "lfuser", "sudo", "rm ", "tcp", "http")):
            return False
        # a real title is prose: needs a space, or is a longer clean word;
        # single camelCase tokens ("IsSkillFile") are JSON keys, not titles
        if " " not in t and len(t) < 14:
            return False
        letters = sum(c.isalpha() for c in t)
        if letters < max(4, len(t) * 0.5):
            return False
        return True
    try:
        con = sqlite3.connect(db)
        for st, payload in con.execute(
                "SELECT step_type, step_payload FROM steps WHERE step_type=23"):
            if not payload:
                continue
            i, n = 0, len(payload)
            while i < n - 2:
                if payload[i] == 0x22 and payload[i + 1] == 0x22:
                    L = payload[i + 2]
                    if 3 <= L <= 120:
                        s = payload[i + 3:i + 3 + L]
                        try:
                            txt = s.decode("utf-8")
                        except Exception:
                            i += 1
                            continue
                        if txt != "sessionID" and _clean(txt):
                            con.close()
                            return txt.strip()
                i += 1
        con.close()
    except Exception as e:
        print("ide_title err", conv_id, e, file=sys.stderr)
    return None

def conv_title(conv_id):
    """Best-effort human title for a conversation, for the pinned status.
    For IDE sessions the real editor title is read via ide_title(); for plain
    local agy runs we synthesise one from the newest reasoning/answer step."""
    if not conv_id:
        return None
    if (t := ide_title(conv_id)):
        return t
    db = os.path.join(USER_CONV_DIR, f"{conv_id}.db")
    if not os.path.exists(db):
        return None
    try:
        con = sqlite3.connect(db)
        # Order: newest ANSWER (22) first (a concise subject), then USER prompt (14).
        # NOTE: despite the old comment, step_type 15 HOLDS THE ANSWER in this
        # agy version (verified 2026-08-22: idx 2525 = full link list, type 15;
        # docstring below line 867 says "14=user, 15=answer"). Include it.
        rows = con.execute(
            "SELECT step_type, step_payload FROM steps "
            "WHERE step_type IN (15,22,14) ORDER BY idx DESC LIMIT 40").fetchall()
        con.close()
        for st, pl in rows:
            txt = _visible_text(pl)
            if not txt or len(txt) < 6:
                continue
            first = txt.split("\n", 1)[0].strip()
            if len(first) > 80:
                first = first[:77] + "…"
            elif len(txt) > 60:
                first = txt[:57] + "…"
            return first
    except Exception:
        pass
    return None

def last_answer(conv_id, min_len=12):
    """Recover the newest agent ANSWER from a conversation .db, or None.

    The real bug this fixes: when agy produces its answer through a
    subagent/manage_task path, the text is written to the conversation .db
    (step_type 15 in this agy version) but does NOT flow back through the
    stream-json `result.response` or `agent_response` deltas. bot.py's
    run_agy then sees final=None AND live_text="", falls into the
    `answer = final or live_text or ""` trap and emits
    "✅ Done (no text response)." — the link/turn silently disappears.

    We scan the newest answer-bearing steps (type 15 on this agy, 22 on
    others — accept both) and return the best prose via _visible_text(),
    whose whole job is to reject ids/hashes/paths/base64 and keep real prose.
    """
    if not conv_id:
        return None
    db = os.path.join(USER_CONV_DIR, f"{conv_id}.db")
    if not os.path.exists(db):
        return None
    try:
        con = sqlite3.connect(db)
        rows = con.execute(
            "SELECT step_payload FROM steps "
            "WHERE step_type IN (15,22) ORDER BY idx DESC LIMIT 6").fetchall()
        con.close()
    except Exception as e:
        print("last_answer err", conv_id, e, file=sys.stderr)
        return None
    for (pl,) in rows:
        txt = _answer_visible(pl)
        if txt and len(txt.strip()) >= min_len:
            return txt.strip()
    return None

def last_reasoning(conv_id, skip_text=None):
    """DEPRECATED / no-op.

    This agy version does NOT persist reasoning/thinking text to the
    conversation .db (verified 2026-08-21: a fresh run writes only step_type
    14=user, 15=answer, 23=metadata; no thinking step). The old code returned
    artifact-metadata JSON garbage, which produced the nonsense "thinking"
    spoiler. The only real thinking signal is the transient `reasoning` event
    during a live _run, which is now captured in-memory and sent once after the
    answer. We keep the shim so nothing breaks, but it always returns None.
    """
    return None

def ide_workspace(db_path):
    """Best-effort label: pull a workspace file:// URI out of the protobuf blob."""
    try:
        with open(db_path, "rb") as f:
            raw = f.read()
        m = re.search(rb"file://[^\x00-\x1f'\"\\]+", raw)
        if m:
            return m.group(0).decode().replace("file://", "")
    except Exception:
        pass
    return "unknown"

def ws_dirs():
    """Distinct, real workspace dirs across every session + every IDE session on
    disk. Only dirs that exist are returned (weeds out the garbage file:// URIs
    that get scraped out of the IDE's binary .db blobs). Newest mtime first."""
    seen = {}  # full_path -> None, preserves order, dedupes
    def _add(d):
        d = (d or "").strip()
        if not d or d == "unknown":
            return
        if not os.path.isdir(d):
            return
        seen[d] = None
    try:
        for (d,) in db.execute("SELECT workspace FROM sessions WHERE workspace IS NOT NULL"):
            _add(d)
    except Exception:
        pass
    for cid, ws, _mt in ide_list():
        _add(ws)
    # Always offer the configured base if it exists
    _add(DEFAULT_WORKSPACE)
    # order by mtime of the dir (best-effort), newest first
    def _mtime(d):
        try:
            return os.path.getmtime(d)
        except Exception:
            return 0.0
    return [d for d in sorted(seen, key=_mtime, reverse=True)]

def ide_list():
    """List IDE conversations newest-first: (id, workspace, mtime)."""
    out = []
    if not os.path.isdir(USER_CONV_DIR):
        return out
    for fn in os.listdir(USER_CONV_DIR):
        if not fn.endswith(".db") or fn.endswith("-wal.db") or fn.endswith("-shm.db"):
            continue
        cid = fn[:-3]
        full = os.path.join(USER_CONV_DIR, fn)
        try:
            st = os.stat(full)
        except OSError:
            continue
        out.append((cid, ide_workspace(full), st.st_mtime))
    out.sort(key=lambda x: -x[2])
    return out

def import_ide_session(chat_id, ide_id, name=None):
    """Copy an IDE conversation .db into the CLI store (flipping its meta
    source 1->17) and register a session that resumes the real IDE thread.
    Returns the session name, or None if the IDE id doesn't exist."""
    src = os.path.join(USER_CONV_DIR, f"{ide_id}.db")
    if not os.path.exists(src):
        return None
    ensure_ide_link(ide_id)
    existing = db.execute("SELECT name FROM sessions WHERE chat_id=? AND ide_id=?",
                          (chat_id, ide_id)).fetchone()
    if existing:
        nm = existing[0]
    else:
        ws = ide_workspace(src) or "ide"
        base = os.path.basename(ws.rstrip("/")) or "ide"
        # Use the IDE/agy's real title (e.g. "Studio Page UI Redesign"), read
        # from the step_type=23 session-metadata step so the Telegram session
        # name matches exactly what the editor/laptop shows.
        title = None
        try:
            title = ide_title(ide_id)
        except Exception:
            pass
        n = name or (f"ide-{base}" if not title else title)
        i = 1
        cand = n
        while db.execute("SELECT 1 FROM sessions WHERE chat_id=? AND name=?", (chat_id, cand)).fetchone():
            i += 1
            cand = f"{n}-{i}"
        nm = cand
        save_session(chat_id, nm, ide_id, DEFAULT_MODEL, ws)
        db.execute("UPDATE sessions SET source='ide', ide_id=?, title=? WHERE chat_id=? AND name=?",
                   (ide_id, title, chat_id, nm))
        db.commit()
    set_active(chat_id, nm)
    # Auto-arm IDE -> Telegram forwarding so the user never misses the agent's
    # answers/plans/tasks as they happen in the IDE. Two-way, nothing missed.
    db.execute("UPDATE sessions SET bridge=1 WHERE chat_id=? AND name=?",
               (chat_id, nm))
    db.commit()
    return nm

# ---- telegram helpers ----
def tg(method, **kw):
    try:
        return requests.post(f"{API}/{method}", json=kw, timeout=30).json()
    except Exception as e:
        print("tg error", method, e, file=sys.stderr)
        return {}

COMMANDS = [
    {"command": "start",     "description": "Show help / bot intro"},
    {"command": "help",      "description": "List commands & usage"},
    {"command": "abort",     "description": "Abort the current running task / thinking"},
    {"command": "stop",      "description": "Stop current task (alias for /abort)"},
    {"command": "steer",     "description": "Guide the running turn without killing it"},
    {"command": "credits",   "description": "Show / enable AI credits (used when quota hits 0)"},
    {"command": "new",       "description": "Start a new named session & switch to it"},
    {"command": "sessions",  "description": "Switch / delete sessions (buttons)"},
    {"command": "model",     "description": "Pick a model (buttons)"},
    {"command": "workspace", "description": "Show or pick the session workspace dir"},
    {"command": "history",   "description": "Show your last 5 messages + the bot's replies"},
    {"command": "effort",    "description": "Set reasoning effort: low|medium|high"},
    {"command": "pin",       "description": "Pin the live status message"},
]

def register_commands():
    """Push the current command list to Telegram so the / menu is up to date."""
    r = tg("setMyCommands", commands=COMMANDS)
    print("register_commands ok=", r.get("ok"), r.get("description"), file=sys.stderr)

def send_long(chat_id, text, reply_markup=None):
    return _md_send(chat_id, text, reply_markup)

def edit_msg(chat_id, message_id, text, reply_markup=None):
    return _md_edit(chat_id, message_id, text, reply_markup)

# ---- live context: codebase (cwd + git branch) + pinned status ----
def codebase_label(chat_id):
    """Return a short 'cwd @branch' label for the active session, or just cwd."""
    s = get_session(chat_id)
    ws = s.get("workspace") or DEFAULT_WORKSPACE
    base = ws.rstrip("/").split("/")[-1] or ws
    branch = ""
    try:
        out = subprocess.run(["git", "-C", ws, "rev-parse", "--abbrev-ref", "HEAD"],
                             capture_output=True, text=True, timeout=5)
        if out.returncode == 0 and out.stdout.strip() and out.stdout.strip() != "HEAD":
            branch = " @ " + out.stdout.strip()
    except Exception:
        pass
    tag = " 💻IDE" if s.get("source") == "ide" else ""
    return f"{base}{branch}{tag}"

pinned = {}  # chat_id -> message_id of the live status message
browse_state = {}  # chat_id -> current dir being browsed in the ws picker
browse_labels = {}  # chat_id -> list[str] of subdir names for the current browse screen

# ---- System & Context Usage for Pinned Status ----
_sys_usage_cache = {"ts": 0.0, "text": ""}
_session_usage_cache = {}  # (chat_id, sess_name) -> {"input": int, "output": int, "total": int}

def _format_token_count(n):
    if not n:
        return "0"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 10_000:
        return f"{round(n / 1_000)}K"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)

def _get_model_context_limit(model_name):
    m = (model_name or "").lower()
    if "claude" in m:
        return 200_000
    if "gpt" in m or "o1" in m or "o3" in m:
        return 128_000
    if "gemini" in m:
        return 1_048_576
    return 1_000_000

def _save_session_usage(chat_id, sess_name, in_tok, out_tok, total_tok):
    _session_usage_cache[(chat_id, sess_name)] = {
        "input": in_tok,
        "output": out_tok,
        "total": total_tok,
    }
    try:
        db.execute(
            "UPDATE sessions SET tokens_in=?, tokens_out=?, tokens_total=? WHERE chat_id=? AND name=?",
            (in_tok, out_tok, total_tok, chat_id, sess_name)
        )
        db.commit()
    except Exception:
        pass

def _get_session_usage_line(chat_id, s):
    sess_name = s.get("name")
    usage = _session_usage_cache.get((chat_id, sess_name))
    if not usage:
        try:
            row = db.execute(
                "SELECT tokens_in, tokens_out, tokens_total FROM sessions WHERE chat_id=? AND name=?",
                (chat_id, sess_name)
            ).fetchone()
            if row and (row[0] or row[1] or row[2]):
                usage = {"input": row[0] or 0, "output": row[1] or 0, "total": row[2] or 0}
                _session_usage_cache[(chat_id, sess_name)] = usage
        except Exception:
            pass

    limit = _get_model_context_limit(s.get("model"))
    if usage and usage.get("total"):
        total = usage["total"]
        pct = round((total / limit) * 100) if limit else 0
        tot_str = _format_token_count(total)
        lim_str = _format_token_count(limit)
        return f"📊 Tokens: {tot_str} / {lim_str} ({pct}%)"

    conv_id = s.get("conv_id") or s.get("ide_id")
    turns = 0
    if conv_id:
        try:
            dbp = os.path.join(IDE_ROOT, "conversations", f"{conv_id}.db")
            if os.path.isfile(dbp):
                c = sqlite3.connect(dbp)
                try:
                    turns = c.execute("SELECT COUNT(*) FROM steps WHERE step_type=15").fetchone()[0]
                finally:
                    c.close()
        except Exception:
            pass
    if turns > 0:
        return f"📊 Context: {turns} turn{'s' if turns != 1 else ''} · 🧠 {_format_token_count(limit)} limit"
    return ""

def _get_system_usage():
    """Return live system resource utilization (CPU, RAM, GPU/VRAM).
    Has pure-Linux fallback via /proc and os.getloadavg so it never fails.
    Cached for 2.5s for zero overhead.
    """
    now = time.time()
    if now - _sys_usage_cache["ts"] < 2.5 and _sys_usage_cache["text"]:
        return _sys_usage_cache["text"]

    parts = []
    # CPU
    try:
        try:
            import psutil
            cpu = int(psutil.cpu_percent())
        except Exception:
            load1, _, _ = os.getloadavg()
            n_cpu = os.cpu_count() or 1
            cpu = min(100, int((load1 / n_cpu) * 100))
        parts.append(f"CPU: {cpu}%")
    except Exception:
        pass

    # RAM
    try:
        used_gb = 0.0
        tot_gb = 0.0
        try:
            import psutil
            mem = psutil.virtual_memory()
            used_gb = mem.used / (1024**3)
            tot_gb = mem.total / (1024**3)
        except Exception:
            minfo = {}
            with open("/proc/meminfo") as f:
                for line in f:
                    p = line.split(":")
                    if len(p) == 2:
                        minfo[p[0].strip()] = int(p[1].split()[0])
            tot_gb = minfo.get("MemTotal", 0) / (1024 * 1024)
            avail_gb = minfo.get("MemAvailable", 0) / (1024 * 1024)
            used_gb = tot_gb - avail_gb
        if tot_gb > 0:
            parts.append(f"RAM: {used_gb:.1f}/{tot_gb:.1f}GB")
    except Exception:
        pass

    # GPU / VRAM (RTX 3060)
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used,memory.total", "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=0.8
        )
        if r.returncode == 0 and r.stdout.strip():
            g_line = r.stdout.strip().split("\n")[0]
            g_parts = [p.strip() for p in g_line.split(",")]
            if len(g_parts) >= 3:
                g_util = g_parts[0]
                v_used = int(g_parts[1]) / 1024
                v_tot = int(g_parts[2]) / 1024
                parts.append(f"GPU: {g_util}% ({v_used:.1f}/{v_tot:.1f}G)")
    except Exception:
        pass

    res = ("💻 " + " · ".join(parts)) if parts else ""
    _sys_usage_cache["ts"] = now
    _sys_usage_cache["text"] = res
    return res


# ---- AG Usage Quota Integration (crsx.ag-usage) ----
_ag_quota_cache = {"ts": 0.0, "data": None}

def get_ag_quota_summary():
    """Query Antigravity language_server for User Quota Summary (5h and weekly limits).
    Matches crsx.ag-usage extension:
    POST /exa.language_server_pb.LanguageServerService/RetrieveUserQuotaSummary
    Cached for 15s to avoid excess calls.
    """
    now = time.time()
    if now - _ag_quota_cache["ts"] < 15.0 and _ag_quota_cache["data"] is not None:
        return _ag_quota_cache["data"]

    try:
        ps = subprocess.run(["ps", "auxww"], capture_output=True, text=True, timeout=2).stdout
        ss_out = subprocess.run(["ss", "-ltnp"], capture_output=True, text=True, timeout=2).stdout

        ls_line = next((l for l in ps.splitlines() if "language_server" in l and "--csrf_token" in l), "")
        csrf = (re.search(r"--csrf_token\s+(\S+)", ls_line) or [None, None])[1]

        pid = None
        for l in ps.splitlines():
            if "language_server" in l and "--csrf_token" in l:
                pm = re.match(r"\S+\s+(\d+)\s", l)
                if pm:
                    pid = int(pm.group(1))
                    break

        if not (pid and csrf):
            return None

        ports = []
        for l in ss_out.splitlines():
            if f"pid={pid}" in l:
                m = re.search(r":(\d+)\s", l)
                if m:
                    ports.append(int(m.group(1)))

        for port in ports:
            for proto in ["http", "https"]:
                try:
                    if proto == "https":
                        c = ssl.create_default_context()
                        c.check_hostname = False
                        c.verify_mode = ssl.CERT_NONE
                        conn = http.client.HTTPSConnection("127.0.0.1", port, context=c, timeout=2)
                    else:
                        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
                    conn.request(
                        "POST",
                        "/exa.language_server_pb.LanguageServerService/RetrieveUserQuotaSummary",
                        body=b"{}",
                        headers={
                            "Content-Type": "application/json",
                            "X-Codeium-Csrf-Token": csrf,
                            "Connect-Protocol-Version": "1",
                        },
                    )
                    r = conn.getresponse()
                    if r.status == 200:
                        res = json.loads(r.read().decode())
                        _ag_quota_cache["ts"] = now
                        _ag_quota_cache["data"] = res
                        return res
                except Exception:
                    pass
    except Exception:
        pass
    return None

def _format_relative_time(iso_str):
    if not iso_str:
        return ""
    try:
        import datetime
        target = datetime.datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        now = datetime.datetime.now(datetime.timezone.utc)
        diff = target - now
        total_sec = int(round(diff.total_seconds()))
        if total_sec <= 0:
            return "ready"
        # Round up seconds to nearest minute for human display
        rounded_sec = total_sec + 30
        days = rounded_sec // 86400
        hours = (rounded_sec % 86400) // 3600
        mins = (rounded_sec % 3600) // 60
        if days > 0:
            return f"{days}d {hours}h"
        if hours > 0:
            return f"{hours}h {mins}m"
        return f"{mins}m"
    except Exception:
        return ""

def _get_ag_quota_line():
    """One-line summary of 5h and weekly quota for the pinned status message."""
    data = get_ag_quota_summary()
    if not data:
        return ""
    groups = data.get("response", {}).get("groups", [])
    parts = []
    for g in groups:
        name = g.get("displayName", "")
        short_name = "Gemini" if "gemini" in name.lower() else "3P"
        b_parts = []
        for b in g.get("buckets", []):
            w = b.get("window", "").lower()
            rem = b.get("remainingFraction")
            if rem is not None:
                pct = round(rem * 100)
                tag = "5h" if "5h" in w else "Wk"
                b_parts.append(f"{pct}% ({tag})")
        if b_parts:
            parts.append(f"{short_name} " + " · ".join(b_parts))
    return ("⚡ Quota: " + " | ".join(parts)) if parts else ""

def format_detailed_quota_msg(data, chat_id=None):
    """Rich breakdown for /quota and /usage commands."""
    lines = ["📊 <b>Antigravity Quotas (AG Usage)</b>\n"]
    if data and data.get("response"):
        groups = data.get("response", {}).get("groups", [])
        for g in groups:
            name = g.get("displayName", "")
            desc = g.get("description", "")
            icon = "🤖" if "gemini" in name.lower() else "🧠"
            lines.append(f"{icon} <b>{name}</b>\n<i>{_h(desc)}</i>")
            for b in g.get("buckets", []):
                dname = b.get("displayName", b.get("window", ""))
                rem = b.get("remainingFraction", 0.0)
                pct = round(rem * 100)
                reset_time = b.get("resetTime")
                rel = _format_relative_time(reset_time)
                reset_str = f" · resets in {rel}" if rel else ""
                lines.append(f"  • <b>{dname}</b>: <b>{pct}%</b> remaining{reset_str}")
            lines.append("")
    else:
        lines.append("⚠️ <i>Could not connect to Antigravity IDE server quota API.</i>\n")

    # Include session token context if chat_id provided
    if chat_id:
        s = get_session(chat_id)
        u_line = _get_session_usage_line(chat_id, s)
        if u_line:
            lines.append(f"<b>Active Session Context</b>:\n{u_line}\n")

    # Include system resources
    sys_u = _get_system_usage()
    if sys_u:
        lines.append(f"<b>System Load</b>:\n{sys_u}")

    return "\n".join(lines).strip()

# ---- AI credits: the agy CLI's "Use AI Credits" (useG1Credits) toggle ----
# This is exactly the setting the agy `/settings` panel persists, so flipping
# it here behaves the same as toggling it in the CLI: once the standard quota
# bucket hits 0 agy falls back to the paid credit balance instead of dying
# with RESOURCE_EXHAUSTED.
AGY_SETTINGS = os.path.expanduser("~/.gemini/antigravity-cli/settings.json")

def _load_agy_settings():
    try:
        with open(AGY_SETTINGS, "r") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}

def _save_agy_settings(d):
    tmp = AGY_SETTINGS + ".tmp"
    with open(tmp, "w") as f:
        json.dump(d, f, indent=2)
        f.write("\n")
    os.replace(tmp, AGY_SETTINGS)

def use_ai_credits_enabled():
    return bool(_load_agy_settings().get("useG1Credits"))

def set_use_ai_credits(on):
    d = _load_agy_settings()
    if on:
        d["useG1Credits"] = True
    else:
        d.pop("useG1Credits", None)
    _save_agy_settings(d)
    return use_ai_credits_enabled()

_credits_cache = {"ts": 0.0, "data": None}

def agy_credits_status(refresh=False):
    """Read-only `agy -p "/credits"` -> {'remaining', 'upgrade', 'raw'}.

    Cached for 60s: a cold run costs ~10-25s of CLI start-up.
    """
    now = time.time()
    if (not refresh and _credits_cache["data"] is not None
            and now - _credits_cache["ts"] < 60.0):
        return _credits_cache["data"]
    res = {"remaining": None, "upgrade": None, "raw": ""}
    try:
        p = subprocess.run([AGY, "-p", "/credits"], capture_output=True,
                           text=True, timeout=45, cwd=os.path.expanduser("~"))
        out = (p.stdout or "").strip()
        res["raw"] = out
        for line in out.splitlines():
            cells = line.split("\t")
            if len(cells) < 2:
                continue
            key = cells[0].strip().lower()
            if key == "remaining credits":
                res["remaining"] = cells[1].strip()
            elif key == "upgrade":
                res["upgrade"] = cells[1].strip()
    except Exception as e:
        res["raw"] = f"error: {e}"
    _credits_cache["ts"] = now
    _credits_cache["data"] = res
    return res

def credits_text():
    on = use_ai_credits_enabled()
    c = agy_credits_status()
    lines = ["💳 <b>G1 Credits</b> <i>(the pool /credits reads)</i>",
             f"Spend credits when quota hits 0: <b>{'ON ✅' if on else 'OFF ⭕'}</b>"]
    rem = c.get("remaining")
    if rem is None:
        lines.append("Balance: <i>unknown — could not read it</i>")
    else:
        lines.append(f"Balance: <b>{_h(str(rem))}</b>")
    if c.get("upgrade"):
        lines.append(f"Top up: <code>{_h(str(c['upgrade']))}</code>")
    lines.append("")
    lines.append("/credits <b>on</b> — use credits once the 5h/weekly quota is spent")
    lines.append("/credits <b>off</b> — stop at the quota limit")
    lines.append("")
    lines.append(quota_block(include_credits=False))
    return "\n".join(lines)

# ============================================================================
# Persistent quota dashboard — lives in the PINNED message.
#
# /quota and /usage were the same number twice; the pin now carries the real
# breakdown all the time: 5-hour + weekly for BOTH the Google (Gemini) group
# and the third-party (Claude/GPT) group, plus the AI-credit state.
#
# Source is `agy -p "/quota"` (the language-server RPC the old handler used
# needs the IDE process, which is usually absent here). That costs a ~15s CLI
# start-up, so it runs on a background thread on a 5-minute cadence and the
# pin only ever reads a cache.
# ============================================================================
QUOTA_REFRESH = 300.0
_quota = {"ts": 0.0, "ok": False, "groups": {}, "err": "",
          "fetching": False, "pending": False}
_quota_lock = threading.Lock()
_quota_order = (("Gemini Models", "🤖 Gemini"),
                ("Claude and GPT models", "🧠 Claude/GPT"))

def _parse_quota_out(text):
    """Parse `agy -p /quota` tab-separated rows into {group: {week|h5: {...}}}."""
    groups = {}
    for line in (text or "").splitlines():
        cells = [c.strip() for c in line.split("\t")]
        if len(cells) < 3 or not cells[0] or not cells[1]:
            continue
        bucket = cells[1].lower()
        if "week" in bucket:
            key = "week"
        elif "five" in bucket or "5 hour" in bucket or "5h" in bucket:
            key = "h5"
        else:
            continue
        g = groups.setdefault(cells[0], {})
        g[key] = {"value": cells[2], "reset": cells[3] if len(cells) > 3 else ""}
    return groups

def _any_run_active():
    with locks_guard:
        return any(bool(l.locked()) for l in locks.values())

def refresh_quota(force=False):
    """Fetch a fresh quota snapshot. Returns True when the cache is usable."""
    with _quota_lock:
        if _quota["fetching"]:
            return _quota["ok"]
        if not force and _quota["ok"] and time.time() - _quota["ts"] < QUOTA_REFRESH:
            return True
        if _any_run_active():        # never spawn a second agy mid-turn
            return _quota["ok"]
        _quota["fetching"] = True
    try:
        p = subprocess.run([AGY, "-p", "/quota"], capture_output=True, text=True,
                           timeout=60, cwd=os.path.expanduser("~"))
        groups = _parse_quota_out(p.stdout or "")
        with _quota_lock:
            _quota.update(ts=time.time(), ok=bool(groups), groups=groups,
                          err=((p.stderr or "").strip() or (p.stdout or "").strip())[:240])
        return bool(groups)
    except Exception as e:
        with _quota_lock:
            _quota.update(ts=time.time(), err=str(e)[:240])
        return False
    finally:
        with _quota_lock:
            _quota["fetching"] = False

def schedule_quota_refresh(delay=2.0):
    """A turn just ended (or the numbers changed) — refetch and repaint the pins.

    Serialized so a burst of finished turns doesn't spawn a stack of `agy`
    processes, and it waits for the chat to go idle before starting one.
    """
    with _quota_lock:
        if _quota.get("pending"):
            return
        _quota["pending"] = True

    def _worker():
        time.sleep(delay)
        try:
            for _ in range(40):          # up to ~2 min waiting for a turn to end
                if not _any_run_active():
                    break
                time.sleep(3)
            if refresh_quota(force=True):
                for cid in list(pinned):
                    try:
                        update_pin(cid)
                    except Exception:
                        pass
        except Exception as e:
            print("quota refresh error:", e, file=sys.stderr)
        finally:
            with _quota_lock:
                _quota["pending"] = False

    threading.Thread(target=_worker, daemon=True).start()

def _fmt_clock(ts):
    try:
        return time.strftime("%H:%M", time.localtime(ts))
    except Exception:
        return ""

def _quota_value(d):
    """-> (display, reset-relative) for one quota bucket."""
    if not d:
        return "—", ""
    v = str(d.get("value") or "").strip()
    low = v.lower()
    if low in ("disabled", "n/a", "none", "-"):
        v = "—"
    elif low in ("unlimited", "∞"):
        v = "∞"
    rel = _format_relative_time(d.get("reset"))
    if rel == "ready":
        rel = "now"
    elif not rel:
        rel = ""
    return _h(v), _h(rel)

def quota_block(include_credits=True):
    """HTML lines for the pinned message / /quota alias. Cache-only, never blocks."""
    with _quota_lock:
        groups = dict(_quota["groups"])
        ok = bool(_quota["ok"])
        ts = _quota["ts"]
        err = _quota["err"]
    lines = []
    if ok:
        age = int(time.time() - ts)
        stamp = _fmt_clock(ts)
        lines.append(f"⚡ <b>Quota</b> <i>· updated {stamp} ({age}s ago)</i>")
    else:
        lines.append("⚡ <b>Quota</b> <i>· loading…</i>")
    for gname, label in _quota_order:
        g = groups.get(gname)
        if not g:
            continue
        wv, wr = _quota_value(g.get("week"))
        hv, _ = _quota_value(g.get("h5"))
        row = f"{label} — 5h <b>{hv}</b> · week <b>{wv}</b>"
        if wr:
            row += f" <i>(resets {wr})</i>"
        lines.append(row)
    if not ok and err:
        lines.append(f"<i>{_h(err)}</i>")
    if include_credits:
        # NOTE: this is the G1 pool only (`agy /credits` -> GetG1Credits). The
        # IDE's Settings -> Models number is a different balance
        # (CreditsProto / userTier.availableCredits), which the CLI cannot read.
        on = use_ai_credits_enabled()
        bal = _credits_cache["data"].get("remaining") if _credits_cache["data"] else None
        bal_s = _h(str(bal)) if bal is not None else "?"
        lines.append(f"💳 G1 credits <b>{'ON' if on else 'OFF'}</b> · balance <b>{bal_s}</b>")
    return "\n".join(lines)

# ---- run lifecycle state (running / stalled / done / failed) ----
_run_state = {}
_run_state_lock = threading.Lock()

def set_run_state(chat_id, **kw):
    with _run_state_lock:
        d = _run_state.setdefault(chat_id, {})
        d.update(kw)
        return dict(d)

def get_run_state(chat_id):
    with _run_state_lock:
        return dict(_run_state.get(chat_id) or {})

def run_state_line(chat_id):
    """One HTML line telling the user whether a turn is running, stalled or over."""
    d = get_run_state(chat_id)
    ph = d.get("phase")
    now = time.time()
    if ph == "running":
        started = d.get("started") or now
        act = d.get("activity") or started
        elapsed = max(0, int(now - started))
        stale = max(0, int(now - act))
        label = _h(d.get("label") or "working")   # tool text -> model-controlled
        if stale >= 45:
            return (f"⚠️ <b>stalled</b> · no output for {stale}s · "
                    f"{label} · running {elapsed}s")
        return f"⏳ <b>running</b> {elapsed}s · {label}"
    if ph in ("done", "error", "aborted"):
        head = {"done": "✅ <b>done</b>", "error": "❌ <b>failed</b>",
                "aborted": "🛑 <b>aborted</b>"}[ph]
        dur = int(d.get("duration") or 0)
        when = d.get("finished")
        stamp = f" <i>{_fmt_clock(when)}</i>" if when else ""
        if ph == "done":
            tools = int(d.get("tools") or 0)
            files = int(d.get("files") or 0)
            tail = f" · {dur}s · {tools} tool{'s' if tools != 1 else ''}"
            if files:
                tail += f" · {files} file{'s' if files != 1 else ''} edited"
            return f"{head}{tail}{stamp}"
        if ph == "error":
            reason = str(d.get("error") or "").strip()
            if reason:
                return f"{head} · {_h(reason[:180])}{stamp}"
            return f"{head} · {dur}s{stamp}"
        return f"{head} · {dur}s{stamp}"
    return "🟢 <b>idle</b>"

_QUOTA_MARKERS = ("individual quota reached", "resource_exhausted",
                  "resource has been exhausted", "exhausted your quota",
                  "quota has been exceeded", "usage limit reached")

def _is_quota_error(err):
    e = str(err or "").lower()
    return any(m in e for m in _QUOTA_MARKERS)

def _conversation_missing(err):
    e = str(err or "").lower()
    return "conversation" in e and "not found" in e

def _keep_conv_after_failure(chat_id, s, conv_id, cwd, err):
    """A failed turn must NOT reset the session's conversation id.

    Losing conv_id makes the next message mint a brand-new thread, so every
    turn after a quota blip starts from zero context. Only agy saying the
    conversation itself is gone justifies clearing it.
    """
    if not conv_id:
        return
    save_session(chat_id, s["name"],
                 None if _conversation_missing(err) else conv_id,
                 s["model"], cwd)

def _quota_error_msg(err):
    on = use_ai_credits_enabled()
    return "\n".join([
        "⚠️ <b>Antigravity quota exhausted</b>",
        f"<i>{_h(str(err or '').strip()[:500])}</i>",
        "",
        f"💳 AI credits: <b>{'ON' if on else 'OFF'}</b>",
        "Tap below to switch credits on (or send <code>/credits on</code>), "
        "then resend your message. <code>/credits</code> shows the balance.",
        "",
        quota_block(),
    ])

def status_text(chat_id, live_state=None):
    s = get_session(chat_id)
    title = s.get("title") or s["name"]
    tag = "💻 IDE" if s.get("source") == "ide" else "🟢 local"

    lines = [f"📌 <b>{_h(str(title))}</b>"]
    # Explicit running / stalled / done / failed marker — the whole point of
    # the pin is that you can tell at a glance without sending anything.
    lines.append(run_state_line(chat_id))
    if live_state:
        lines.append(live_state)
    lines.append(f"{tag} · 🧠 {s['model']}")
    lines.append(f"📁 {codebase_label(chat_id)}")
    lines.append(quota_block())

    tok_line = _get_session_usage_line(chat_id, s)
    if tok_line:
        lines.append(tok_line)

    sys_line = _get_system_usage()
    if sys_line:
        lines.append(sys_line)

    return "\n".join(lines)

def update_pin(chat_id, live_state=None):
    mid = pinned.get(chat_id)
    if mid is None:
        return
    edit_msg(chat_id, mid, status_text(chat_id, live_state))

def ensure_pin_table():
    db.execute("CREATE TABLE IF NOT EXISTS pinned_msg(chat_id TEXT PRIMARY KEY, message_id INTEGER)")
    db.commit()

def save_pin(chat_id, mid):
    db.execute("INSERT INTO pinned_msg(chat_id,message_id) VALUES(?,?) "
               "ON CONFLICT(chat_id) DO UPDATE SET message_id=excluded.message_id",
               (chat_id, mid))
    db.commit()

def _load_pinned():
    """Restore pinned-message ids from DB so the live status survives restarts."""
    for cid, mid in db.execute("SELECT chat_id,message_id FROM pinned_msg").fetchall():
        if mid:
            pinned[cid] = mid
            try:
                update_pin(cid)
            except Exception as e:
                print("Error updating pinned message on boot:", cid, e, file=sys.stderr)

def git_push(chat_id, msg=None):
    """git add -A && commit && push for the active session workspace. Returns status text."""
    s = get_session(chat_id)
    ws = s.get("workspace") or DEFAULT_WORKSPACE
    if not os.path.isdir(os.path.join(ws, ".git")):
        return f"⚠️ No git repo at <code>{_h(ws)}</code> — nothing to push."
    def run(args):
        return subprocess.run(["git", "-C", ws] + args, capture_output=True, text=True, timeout=60)
    st = run(["status", "--porcelain"])
    if not st.stdout.strip():
        # nothing staged/modified; try pushing existing commits
        br = run(["rev-parse", "--abbrev-ref", "HEAD"]).stdout.strip() or "HEAD"
        push = run(["push"])
        if push.returncode == 0:
            return f"✅ Nothing to commit — <code>{_h(os.path.basename(ws.rstrip('/')))}</code> already in sync (pushed <code>{_h(br)}</code>)."
        return f"✅ Nothing to commit in <code>{_h(os.path.basename(ws.rstrip('/')))}</code>. Push skipped: {_h(push.stderr.strip()[:160] or 'no remote?')}"
    if not msg:
        # build a default message from last few commit subjects or session name
        msg = f"update from {s['name']} ({s['model']})"
    a = run(["add", "-A"])
    if a.returncode != 0:
        return f"❌ git add failed: {_h(a.stderr.strip()[:200])}"
    c = run(["commit", "-m", msg])
    if c.returncode != 0:
        return f"❌ git commit failed: {_h(c.stderr.strip()[:200])}"
    p = run(["push"])
    if p.returncode != 0:
        return f"⚠️ Committed locally, but push failed:\n<pre>{_h(p.stderr.strip()[:240])}</pre>"
    br = run(["rev-parse", "--abbrev-ref", "HEAD"]).stdout.strip() or "HEAD"
    n = len(st.stdout.strip().splitlines())
    return f"✅ Pushed <code>{_h(os.path.basename(ws.rstrip('/')))}</code> → <code>{_h(br)}</code> ({n} file{'s' if n!=1 else ''} committed)."

# ---- MCQ / option detection ----
LETTER_RE = re.compile(r'^[ \t]*([A-Da-d])[.)][ \t]+(.+)$')
NUM_RE = re.compile(r'^[ \t]*(\d{1,2})[.)][ \t]+(.+)$')
HINT_RE = re.compile(r'\b(question|choose|options?|which|mcq|select|answer)\b', re.I)

def detect_options(text):
    """Return list of (label, body) for MCQ-style options, or None."""
    lines = text.splitlines()
    letter, num = [], []
    for ln in lines:
        m = LETTER_RE.match(ln)
        if m:
            letter.append((m.group(1).upper(), m.group(2).strip()))
            continue
        m2 = NUM_RE.match(ln)
        if m2:
            num.append((m2.group(1), m2.group(2).strip()))
    if 2 <= len(letter) <= 10:
        return letter
    if HINT_RE.search(text) and 2 <= len(num) <= 10:
        return num
    return None

# ---- per-chat run lock + pending MCQ options ----
locks = {}
locks_guard = threading.Lock()
pending = {}  # chat_id -> [full option strings]

# ---- /steer: guidance for the turn that is already running ----
# Hermes semantics (acp_adapter._cmd_steer): if a turn is in flight, the text is
# queued and released the moment that turn lands instead of interrupting it;
# with no turn in flight it runs as a plain prompt. agy's print mode gives no
# mid-turn injection point, so queueing IS the steer.
_steers = {}
_steers_lock = threading.Lock()

def chat_running(chat_id):
    with locks_guard:
        l = locks.get(chat_id)
    return bool(l and l.locked())

def steer_enqueue(chat_id, text):
    with _steers_lock:
        q = _steers.setdefault(chat_id, [])
        q.append(text)
        return len(q)

def steer_dequeue(chat_id):
    with _steers_lock:
        q = _steers.get(chat_id) or []
        if not q:
            return None
        t = q.pop(0)
        if not q:
            _steers.pop(chat_id, None)
        return t

# Transient agy failures worth ONE automatic retry. `Failed to get shell
# integration` is a race where agy's run_command can't grab a PTY/shell from
# the local IDE server (refreshes on its own) and `context canceled` is a stale
# sub-task kill interrupting an in-flight command. Both are orchestration noise
# that usually succeeds on re-run — WITHOUT a retry the bot stops cold and
# never delivers the answer (user saw exactly that: error then stop, no link).
_TRANSIENT_AGY = ("Failed to get shell integration", "context canceled",
                  "context deadline exceeded", "shell integration")
def _is_transient_agy(err):
    if not err:
        return False
    e = str(err)
    return any(t in e for t in _TRANSIENT_AGY)


# ---- Process & Run Management for Abort / Interrupt ----
_active_runs_lock = threading.Lock()
_active_runs = {}          # chat_id -> dict with proc, pgid, master, live_id, etc.
_interrupted_queue = {}    # chat_id -> str (new text prompt to run in existing chat)

def abort_task(chat_id, reason="aborted", notify=True, new_text=None):
    """Abort the current running task for chat_id.
    If new_text is provided, marks the run as interrupted so it seamlessly
    transitions to the new text in the existing session.
    """
    chat_id = str(chat_id)
    run = None
    with _active_runs_lock:
        run = _active_runs.get(chat_id)
        if run:
            run["aborted"] = True
            run["reason"] = reason
            if new_text:
                _interrupted_queue[chat_id] = new_text

    # Also cancel any background task watcher for this chat
    to_cancel = [k for k in list(_watching_tasks) if k[0] == chat_id]
    for k in to_cancel:
        _watching_tasks.discard(k)

    if not run:
        if new_text:
            return False
        if notify:
            send_long(chat_id, "ℹ️ No task is currently running.")
        return False

    proc = run.get("proc")
    pgid = run.get("pgid")
    live_id = run.get("live_id")
    # Reflect the abort in the pin immediately — _run_inner repaints it with
    # the real duration once the process has fully exited.
    set_run_state(chat_id, phase="aborted", finished=time.time(), error="")

    # Terminate process group cleanly
    if proc and proc.poll() is None:
        try:
            if pgid:
                os.killpg(pgid, signal.SIGTERM)
            else:
                proc.terminate()
        except Exception:
            pass

        t0 = time.time()
        while proc.poll() is None and time.time() - t0 < 0.5:
            time.sleep(0.05)

        if proc.poll() is None:
            try:
                if pgid:
                    os.killpg(pgid, signal.SIGKILL)
                else:
                    proc.kill()
            except Exception:
                pass

    if notify:
        if new_text:
            send_long(chat_id,
                      "⚡ <i>Interrupted — continuing with new instruction in chat…</i>\n"
                      "<i>(Tip: send </i><code>/steer …</code><i> instead to guide the "
                      "running turn without killing it.)</i>")
        else:
            send_long(chat_id, "🛑 <b>Task aborted.</b>")

    if live_id:
        try:
            if not new_text:
                # reply_markup is required to retire the 🛑 Abort button —
                # editing text alone leaves the old keyboard attached.
                edit_msg(chat_id, live_id, "🛑 <i>Aborted by user</i>",
                         reply_markup=_NO_KB)
            else:
                tg("deleteMessage", chat_id=chat_id, message_id=live_id)
        except Exception:
            pass

    update_pin(chat_id)
    return True

# ---- Automated Task Waiting & Continuation ----
_watching_tasks = set()

def _is_task_finished(conv_id, task_id):
    """Check whether a background task has exited or emitted completion in transcript/task logs."""
    tid_tail = task_id.split("/")[-1]
    # Check transcript.jsonl
    tr_path = os.path.expanduser(f"~/.gemini/antigravity-ide/brain/{conv_id}/.system_generated/logs/transcript.jsonl")
    if os.path.exists(tr_path):
        try:
            with open(tr_path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()[-60:]
                for l in lines:
                    if '"type":"SYSTEM_MESSAGE"' in l and tid_tail in l:
                        if "finished with result" in l or "was canceled" in l or "exited with code" in l:
                            return True
        except Exception:
            pass

    # Check task log
    log_path = os.path.expanduser(f"~/.gemini/antigravity-ide/brain/{conv_id}/.system_generated/tasks/{tid_tail}.log")
    if os.path.exists(log_path):
        try:
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                txt = f.read()
                if "The command exited with code" in txt or "finished with result" in txt:
                    return True
        except Exception:
            pass

    return False

def _watch_task_and_resume(chat_id, sess_name, conv_id, task_ids):
    """Watch pending background tasks and automatically trigger agy continuation when complete."""
    key = (chat_id, conv_id)
    if key in _watching_tasks:
        return
    _watching_tasks.add(key)
    tid_disp = ", ".join(t.split("/")[-1] for t in task_ids[:2])
    abort_kb = {"inline_keyboard": [[{"text": "🛑 Abort Task", "callback_data": f"abort:{chat_id}"}]]}
    wait_msg = tg("sendMessage", chat_id=chat_id,
                  text=f"⏳ <b>Waiting for background task</b> (<code>{tid_disp}</code>)…",
                  reply_markup=abort_kb)
    wait_mid = wait_msg.get("result", {}).get("message_id")
    try:
        start_t = time.time()
        # Poll up to 25 minutes
        while time.time() - start_t < 1500:
            if key not in _watching_tasks:
                break
            time.sleep(2.5)
            if key not in _watching_tasks:
                break
            elapsed = int(time.time() - start_t)
            update_pin(chat_id, f"⏳ Waiting for task ({tid_disp}) to finish ({elapsed}s)…")
            
            # Check if all tasks are finished
            all_done = True
            for tid in task_ids:
                if not _is_task_finished(conv_id, tid):
                    all_done = False
                    break
            
            if all_done:
                if wait_mid:
                    try: tg("deleteMessage", chat_id=chat_id, message_id=wait_mid)
                    except Exception: pass
                update_pin(chat_id, "⚡ Task finished · requesting final answer…")
                time.sleep(1.0)
                nudge = "The background task has finished. Please inspect the output and provide the complete final answer."
                run_agy(chat_id, nudge, sess_name)
                break
    finally:
        _watching_tasks.discard(key)
        if wait_mid:
            try: tg("editMessageReplyMarkup", chat_id=chat_id, message_id=wait_mid, reply_markup=None)
            except Exception: pass

def run_agy(chat_id, text, sess_name=None):
    with locks_guard:
        l = locks.get(chat_id)
        if l is None:
            l = locks[chat_id] = threading.Lock()
    if not l.acquire(blocking=False):
        # A task is already active: default interrupt is adding new text to existing chat!
        abort_task(chat_id, reason="interrupted", notify=True, new_text=text)
        return

    interrupted_next = None
    try:
        # Cancel any background task watcher for this chat if new turn started
        s = get_session(chat_id, sess_name)
        conv_id = s.get("conv_id") if s else None
        if conv_id:
            _watching_tasks.discard((chat_id, conv_id))

        for _attempt in range(2):
            outcome = _run(chat_id, text, sess_name)
            if outcome == "aborted":
                with _active_runs_lock:
                    interrupted_next = _interrupted_queue.pop(chat_id, None)
                break
            if outcome != "retry":
                break
            send_long(chat_id, "🔄 agy shell hiccup — restarting the turn…")
            time.sleep(2)
    finally:
        l.release()

    # Steers queued while this turn was in flight — release them now, in order.
    # Each nested run_agy finds the queue empty for its own drain, so this
    # cannot recurse deeper than one extra level per queued steer.
    while True:
        nxt = steer_dequeue(chat_id)
        if not nxt:
            break
        run_agy(chat_id, nxt, sess_name)

    if interrupted_next:
        time.sleep(0.15)
        run_agy(chat_id, interrupted_next, sess_name)

    schedule_quota_refresh()

def _run(chat_id, text, sess_name=None):
    s = get_session(chat_id, sess_name)
    cmd = [AGY, "-p", text, "--output-format", "stream-json", "--model", s["model"],
           "--dangerously-skip-permissions", "--disable-slash-commands", "--print-timeout", "30m"]
    effort = get_effort(chat_id)
    # Don't pass --effort when the model name already encodes an effort level
    # (e.g. gemini-3.7-flash-HIGH / -MEDIUM / -LOW). agy rejects the combo
    # "model already encodes effort + explicit --effort" — which would fail
    # EVERY turn on those models. Only add --effort for models without a suffix.
    model_encodes_effort = bool(re.search(r"-(high|medium|low)$", s["model"], re.I)) \
        or s["model"] in ("claude-sonnet-4-6", "claude-opus-4-6-thinking")
    if effort and not model_encodes_effort:
        cmd += ["--effort", effort]
    if s["conv_id"]:
        cmd += ["--conversation", s["conv_id"]]
    cwd = s["workspace"] or DEFAULT_WORKSPACE
    os.makedirs(cwd, exist_ok=True)

    # IDE-imported sessions resume the REAL IDE thread. With the CLI
    # `conversations` dir symlinked to the IDE store, `agy --conversation
    # <ide_id>` reads/writes the same physical .db the editor uses, so Telegram
    # and the laptop IDE stay in lockstep (cloud sync propagates both ways).
    if s.get("source") == "ide":
        ensure_ide_link(s.get("ide_id"))

    tg("sendChatAction", chat_id=chat_id, action="typing")
    # keep the "typing…" indicator alive until agy is done (TG shows it ~5s)
    stop_typing = threading.Event()
    def _typing():
        while not stop_typing.wait(4.0):
            tg("sendChatAction", chat_id=chat_id, action="typing")
    threading.Thread(target=_typing, daemon=True).start()
    try:
        return _run_inner(chat_id, text, s, cmd, cwd, stop_typing)
    finally:
        stop_typing.set()
        conv_id = s.get("conv_id")
        if conv_id:
            with _active_convs_lock:
                _active_convs.discard(conv_id)
        with _active_runs_lock:
            _active_runs.pop(chat_id, None)

def _run_inner(chat_id, text, s, cmd, cwd, stop_typing):
    # Spawn in a PTY (not a pipe). agy's shell integration needs a real
    # controlling terminal to track command output; a plain PIPE makes
    # `run_command` cascade steps fail. The local antigravity-ide-server
    # provides shell integration for source=1 threads, so the PTY run works
    # directly on the IDE thread. ANSI escapes are stripped and the stream is
    # re-split into JSON lines.
    try:
        master, slave = pty.openpty()
        proc = subprocess.Popen(cmd, cwd=cwd, stdin=slave, stdout=slave,
                                stderr=slave, close_fds=True, preexec_fn=os.setsid)
        os.close(slave)
    except Exception as e:
        send_long(chat_id, f"❌ Failed to launch agy: {e}")
        return

    conv_id = s["conv_id"]
    if conv_id:
        with _active_convs_lock:
            _active_convs.add(conv_id)
    with _active_runs_lock:
        _active_runs[chat_id] = {
            "proc": proc,
            "pgid": os.getpgid(proc.pid),
            "master": master,
            "live_id": None,
            "conv_id": conv_id,
            "sess_name": s["name"],
            "aborted": False,
            "interrupted_by": None,
            "start_time": time.time(),
        }
    live_id = None
    answer_id = None          # separate message that actually holds the answer text
    live_text = ""
    last_edit = 0.0
    final = None
    run_err = None           # non-SUCCESS result.error, reported once at the end
    # Explicit run lifecycle so the pin can say running / stalled / done /
    # failed without the user having to poke the bot.
    set_run_state(chat_id, phase="running", started=time.time(),
                  activity=time.time(), label="starting…", error="",
                  finished=0.0, duration=0, tools=0, files=0)

    def _finish_state(phase, **kw):
        st = get_run_state(chat_id)
        dur = int(max(0, time.time() - (st.get("started") or time.time())))
        set_run_state(chat_id, phase=phase, finished=time.time(),
                      duration=dur, **kw)
    # Real thinking capture: this agy version exposes reasoning ONLY as a
    # transient stream event (never persisted to the .db). Accumulate it here
    # and send it ONCE as a single "💭 thinking" message after the answer, so
    # the user gets the genuine thought process instead of the old garbage
    # spoiler. (No-op safe: stays empty if the model sends no reasoning.)
    reasoning_buf = []
    tool_seen = set()
    tools_run = 0
    files_changed = 0
    _active_tool = {"desc": "", "start": 0.0}
    _pending_tasks = []
    _tool_active_seen = set()
    _ansi = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

    abort_kb = {"inline_keyboard": [[{"text": "🛑 Abort", "callback_data": f"abort:{chat_id}"}]]}

    # Live "what's happening" indicator (OpenCode-style). Edits the run
    # placeholder with a short status and mirrors it into the pinned status
    # line so the user always sees progress, not a frozen "Working…".
    _prog = {"state": "⏳ Working…", "last": 0.0, "pin_last": 0.0}

    def _set_state(st):
        nonlocal live_id
        _prog["state"] = st
        now = time.time()
        set_run_state(chat_id, activity=now, label=st)
        # Tool descriptions come straight from the model's parameters — they can
        # contain <, & or a stray </b>, which would blow up parse_mode=HTML.
        safe = _h(st)
        if live_id is not None and not live_started and now - _prog["last"] >= 2.0:
            edit_msg(chat_id, live_id, safe, reply_markup=abort_kb)
            _prog["last"] = now
        if pinned.get(chat_id) is not None and now - _prog["pin_last"] >= 5.0:
            update_pin(chat_id, safe)
            _prog["pin_last"] = now

    def _paint(force=False):
        """Throttled pin repaint driven by _run_state (running/stalled/…)."""
        if pinned.get(chat_id) is None:
            return
        now = time.time()
        if not force and now - _prog["pin_last"] < 5.0:
            return
        _prog["pin_last"] = now
        try:
            update_pin(chat_id)
        except Exception:
            pass

    _STREAM_EDIT_LIMIT = 3900  # safely under Telegram's 4096 editMessageText limit
    _stream_overflow = []      # message_ids of overflow chunks sent after edit limit hit

    def stream(t):
        nonlocal answer_id, last_edit, live_id, live_text
        live_text = t
        now = time.time()
        if answer_id is None:
            if tools_run > 0:
                # Tools ran above: remove the top status placeholder so it does not linger as an orphaned bubble
                if live_id is not None:
                    try:
                        tg("deleteMessage", chat_id=chat_id, message_id=live_id)
                    except Exception:
                        pass
                    live_id = None
                # Send the full text (send_long handles splitting for Telegram's 4096 sendMessage limit)
                r = send_long(chat_id, t)
                answer_id = r.get("result", {}).get("message_id")
            else:
                # No tools: smoothly reuse the live_id placeholder in place
                answer_id = live_id
                edit_msg(chat_id, answer_id, t)
            last_edit = now
        elif now - last_edit >= 0.6:
            if len(t) <= _STREAM_EDIT_LIMIT:
                # Fits in a single edit — update in place
                edit_msg(chat_id, answer_id, t)
            else:
                # Text exceeds edit limit. Keep editing the first chunk so the
                # user sees live progress; the final completion step delivers all
                # chunks completely via send_long without Telegram rate-limit churn.
                edit_msg(chat_id, answer_id, t[:_STREAM_EDIT_LIMIT])
            last_edit = now

    def _parse_event(raw):
        line = _ansi.sub("", raw).strip()
        if not line:
            return None
        try:
            return json.loads(line)
        except Exception:
            return None

    # NOTE: the user turn is logged once at the single inbound entry point
    # (handle()), so logging it again here would double-log every
    # Telegram-originated prompt in /history.
    # Minimal "working" placeholder (replaced by the real answer as it streams).
    # No token meter, no codebase/IDE tag — just a quiet progress indicator.
    r = tg("sendMessage", chat_id=chat_id, text="⏳ Working…")
    live_id = r.get("result", {}).get("message_id")
    last_edit = time.time()
    live_started = False

    _buf = b""
    try:
        while True:
            with _active_runs_lock:
                ar = _active_runs.get(chat_id)
                if ar and ar.get("aborted"):
                    break
            # Poll with timeout to prevent deadlock when background children inherit slave pty
            rlist, _, _ = select.select([master], [], [], 0.2)
            if rlist:
                try:
                    chunk = os.read(master, 65536)
                except OSError:
                    break
                if not chunk:
                    break
                _buf += chunk
                *lines, _buf = _buf.split(b"\n")
                for raw in lines:
                    raw = raw.decode("utf-8", "replace").rstrip("\r")
                    ev = _parse_event(raw)
                    if not ev:
                        continue
                    et = ev.get("event")
                    if et == "init":
                        cid = ev.get("conversation_id") or ev.get("init", {}).get("conversation_id")
                        if cid:
                            conv_id = cid
                    elif et == "step_update":
                        su = ev.get("step_update", {})
                        st = su.get("step_type")
                        state = su.get("state")
                        if st == "reasoning" and state == "ACTIVE":
                            _set_state("💭 Thinking…")
                            rd = su.get("text_delta") or su.get("reasoning") or su.get("text") or ""
                            if rd:
                                reasoning_buf.append(rd)
                        elif st == "agent_response" and state in ("ACTIVE", "DONE"):
                            d = su.get("text_delta", "")
                            if d:
                                live_started = True
                                _set_state("✍️ Writing answer…")
                                stream(live_text + d)
                        elif st == "tool":
                            if answer_id == live_id:
                                answer_id = None
                                live_text = ""
                                live_started = False
                            _render_tool(chat_id, su, tool_seen)
                            info = su.get("tool_info") or {}
                            name = info.get("name") or su.get("tool_name") or ""
                            params = info.get("parameters") or su.get("parameters") or {}
                            if state == "ACTIVE":
                                if (su.get("step_index"), name) not in _tool_active_seen:
                                    _tool_active_seen.add((su.get("step_index"), name))
                                    tools_run += 1
                                    # Human-friendly tool description for pinned status
                                    summary = params.get("toolSummary") or params.get("toolAction") or ""
                                    cmd = (params.get("CommandLine") or params.get("code") or "").strip()
                                    if summary and summary not in ("Running command", "Viewing file", "Editing file", "Executing command"):
                                        desc = summary
                                    elif cmd:
                                        first = cmd.split()[0] if cmd.split() else ""
                                        desc = f"{first}: {cmd[:30]}…" if len(cmd) > 30 else cmd
                                    elif name:
                                        desc = name
                                    else:
                                        desc = f"Tool ({tools_run})"
                                    is_wait = any(w in desc.lower() for w in ("download", "install", "fetch", "clone", "pull", "render", "build", "wait", "sleep"))
                                    icon = "⏳" if is_wait else "⚡"
                                    _active_tool["desc"] = f"{icon} {desc}"
                                    _active_tool["start"] = time.time()
                                    _set_state(f"{_active_tool['desc']}…")
                                    # Capture task id if launched as task
                                    tid = str(params.get("TaskId") or params.get("taskId") or "").strip()
                                    if tid and tid not in _pending_tasks:
                                        _pending_tasks.append(tid)
                            elif state == "DONE":
                                _active_tool["desc"] = ""
                                _active_tool["start"] = 0.0
                                if name in ("replace_file_content", "multi_replace_file_content",
                                            "write_to_file", "delete_file"):
                                    files_changed += 1
                    elif et == "result":
                        res = ev.get("result", {})
                        cid = res.get("conversation_id")
                        if cid:
                            conv_id = cid
                            with _active_convs_lock:
                                _active_convs.add(conv_id)
                        if res.get("status") != "SUCCESS":
                            if _is_transient_agy(res.get("error")):
                                if answer_id is not None:
                                    pass
                                else:
                                    stop_typing.set()
                                    try: proc.kill()
                                    except Exception: pass
                                    return "retry"
                            else:
                                run_err = str(res.get("error") or res.get("status")
                                             or "unknown error")
                        else:
                            final = res.get("response")
                            u = res.get("usage") or {}
                            if u.get("thinking_tokens"):
                                think_total = int(u["thinking_tokens"])
                            in_tok = int(u.get("input_tokens") or 0)
                            out_tok = int(u.get("output_tokens") or 0)
                            tot_tok = int(u.get("total_tokens") or (in_tok + out_tok))
                            if tot_tok > 0:
                                _save_session_usage(chat_id, s["name"], in_tok, out_tok, tot_tok)
            else:
                # Timeout: check if main agy process completed or update waiting heartbeat
                if proc.poll() is not None:
                    time.sleep(0.05)
                    rlist2, _, _ = select.select([master], [], [], 0.3)
                    if rlist2:
                        try:
                            chunk = os.read(master, 65536)
                            if chunk:
                                _buf += chunk
                        except OSError:
                            pass
                    break
                elif _active_tool["desc"] and _active_tool["start"] > 0:
                    now = time.time()
                    elapsed = int(now - _active_tool["start"])
                    if elapsed >= 3 and now - _prog["pin_last"] >= 3.0:
                        _set_state(f"{_active_tool['desc']} ({elapsed}s)…")
                # Nothing came off the PTY this tick — still repaint the pin so
                # the elapsed-time / stalled indicator keeps moving.
                _paint()
    finally:
        try:
            os.close(master)
        except Exception:
            pass
    if _buf:
        ev = _parse_event(_buf.decode("utf-8", "replace").rstrip("\r"))
        if ev and ev.get("event") == "result":
            res = ev.get("result", {})
            cid = res.get("conversation_id")
            if cid:
                conv_id = cid
            if res.get("status") != "SUCCESS":
                if _is_transient_agy(res.get("error")):
                    if answer_id is not None:
                        # spurious tail cancel after the answer already streamed
                        pass
                    else:
                        stop_typing.set()
                        try: proc.kill()
                        except Exception: pass
                        return "retry"
                else:
                    run_err = str(res.get("error") or res.get("status")
                                 or "unknown error")
            else:
                final = res.get("response")
                u = res.get("usage") or {}
                if u.get("thinking_tokens"):
                    think_total = int(u["thinking_tokens"])
                in_tok = int(u.get("input_tokens") or 0)
                out_tok = int(u.get("output_tokens") or 0)
                tot_tok = int(u.get("total_tokens") or (in_tok + out_tok))
                if tot_tok > 0:
                    _save_session_usage(chat_id, s["name"], in_tok, out_tok, tot_tok)
    try:
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass

    with _active_runs_lock:
        ar = _active_runs.pop(chat_id, None)

    if ar and ar.get("aborted"):
        try:
            os.close(master)
        except Exception:
            pass
        _finish_state("aborted")
        update_pin(chat_id)
        if live_id is not None:
            try:
                tg("deleteMessage", chat_id=chat_id, message_id=live_id)
            except Exception:
                pass
        return "aborted"

    failed = False
    if final is None:
        failed = bool(run_err) or proc.returncode not in (0, None)
        if failed:
            # NEVER wipe conv_id here. Clearing it on a failed run is what made
            # every quota blip mint a brand-new thread and lose all context.
            # Only agy reporting the conversation itself as gone resets it.
            _keep_conv_after_failure(chat_id, s, conv_id, cwd, run_err)
            _finish_state("error", error=str(run_err or "").strip()
                          or f"agy exited {proc.returncode}")
            if _is_quota_error(run_err):
                send_long(chat_id, _quota_error_msg(run_err),
                          reply_markup={"inline_keyboard": [[
                              {"text": "💳 Enable AI credits",
                               "callback_data": "credits:on"}]]})
            elif run_err:
                send_long(chat_id, f"⚠️ <b>agy failed</b>\n<pre>{_h(str(run_err))}</pre>")
            else:
                send_long(chat_id, f"❌ <b>agy exited {proc.returncode}</b>")
        final = ""

    # If agy only STREAMED the answer (no result.response payload), fall back
    # to the live-streamed text so real answers are never replaced by "✅ Done".
    answer = final or live_text or ""
    if not answer.strip() and conv_id:
        # The answer may have been written to the conversation .db by a
        # subagent/manage_task path that bypassed the stream-json result/
        # agent_response events (final=None, live_text=""). Recover the real
        # prose from the DB rather than emitting the silent placeholder.
        recovered = last_answer(conv_id)
        if recovered:
            answer = recovered
    if not answer.strip():
        if failed:
            # failure already reported — don't claim "✅ Done", but still clear
            # the pinned "Writing answer…" state and retire the 🛑 button.
            if live_id is not None:
                edit_msg(chat_id, live_id, "❌ Turn failed", reply_markup=_NO_KB)
            update_pin(chat_id)
            return
        answer = "✅ Done (no text response)."
    _finish_state("done", tools=tools_run, files=files_changed)
    # log the assistant turn too
    log_turn(chat_id, s["name"], "assistant", answer)
    # Send the genuine thought process ONCE, if any real reasoning was captured
    # during the live stream (this agy version exposes reasoning only as a
    # transient event, never persisted). Skipped when empty — no more garbage
    # "thinking" spoiler from the old last_reasoning() shim.
    #
    # DISABLED 2026-08-25 (user request): reasoning_buf is still captured (so we
    # can re-enable easily) but we no longer post the "💭 thinking" spoiler
    # bubble after the answer. The user wants the CoT suppressed — answer only.
    if False and reasoning_buf:
        thought = _h("".join(reasoning_buf).strip()[:3500]).strip()
        if thought:
            send_long(chat_id,
                      f"💭 <b>thinking</b> <tg-spoiler>{thought}</tg-spoiler>")
    # mark that WE just sent this turn, and turn on IDE→Telegram bridging for
    # this session — so the poller won't re-post our own answer back to Telegram
    db.execute("UPDATE sessions SET last_telegram_send=?, bridge=1 "
               "WHERE chat_id=? AND name=?", (time.time(), chat_id, s["name"]))
    db.commit()
    # Advance the forwarder's cursor past everything currently in this conv, so
    # the IDE->Telegram poller won't re-post the answer we just showed inline.
    if conv_id:
        try:
            dbp = os.path.join(IDE_ROOT, "conversations", f"{conv_id}.db")
            if os.path.isfile(dbp):
                _c_st = sqlite3.connect(dbp)
                try:
                    mx = _c_st.execute("SELECT COALESCE(MAX(idx),0) FROM steps").fetchone()[0]
                finally:
                    _c_st.close()
                _bridge_seen[conv_id] = mx
                _bridge_cursor(conv_id, mx)
        except Exception:
            pass
    target_id = live_id
    # Render the FINAL answer as properly-formatted markdown (→ HTML) so
    # headings, lists, tables, code blocks and bold/italic all render nicely.
    rendered = md_to_html(answer)
    # NOTE: thinking is no longer folded into the answer here. This agy version
    # does not persist reasoning to the .db, so last_reasoning() can only ever
    # return garbage. The genuine thought process (captured from the live
    # stream) is sent as its own "💭 thinking" spoiler right after the answer,
    # inside _run(). See reasoning_buf handling.
    # Prepend reasoning thought process as an expandable blockquote if captured
    if reasoning_buf:
        thought_str = "".join(reasoning_buf).strip()
        if thought_str and len(thought_str) > 5:
            t_short = _short(thought_str, 2500)
            rendered = f"<blockquote expandable>💭 <b>Thinking Process</b>\n{_h(t_short)}</blockquote>\n\n" + rendered

    chunks = _html_safe_chunks(rendered, limit=3800)
    if tools_run > 0:
        # Tools executed above: collapse the top placeholder into a single
        # completion summary. Written ONCE here — a second, differently-worded
        # summary used to be written further down, so the user saw the message
        # flip from "⚡ Ran N tools" to "✅ Done · N tools · M files" with the
        # 🛑 Abort button still attached.
        done_summary = f"✅ Done · {tools_run} tool{'s' if tools_run != 1 else ''}"
        if files_changed > 0:
            done_summary += f" · {files_changed} file{'s' if files_changed != 1 else ''} edited"
        if live_id is not None and live_id != answer_id:
            edit_msg(chat_id, live_id, done_summary, reply_markup=_NO_KB)
    else:
        # No tools: clean up live_id if distinct from answer_id
        if live_id is not None and live_id != answer_id:
            try:
                tg("deleteMessage", chat_id=chat_id, message_id=live_id)
            except Exception:
                pass
            live_id = None

    # Clean up any overflow messages sent during streaming (they're now superseded
    # by the final rendered answer below).
    for ov_id in _stream_overflow:
        try:
            tg("deleteMessage", chat_id=chat_id, message_id=ov_id)
        except Exception:
            pass
    _stream_overflow.clear()

    # Deliver final answer at the bottom:
    # If tools ran and answer_id was never allocated (e.g. no post-tool streaming),
    # answer_id is None, so all chunks are sent fresh via send_long BELOW the tools.
    first_chunk = chunks[0] if chunks else (rendered or "")
    target_text = ""   # exact text currently living in target_id — never truncated
    if answer_id is not None:
        # Final write of the streamed message: also retire the 🛑 Abort button.
        r = edit_msg(chat_id, answer_id, first_chunk, reply_markup=_NO_KB)
        if r.get("ok") or "not modified" in str(r.get("description", "")).lower():
            target_id = answer_id
            target_text = first_chunk
            chunks = chunks[1:]
        else:
            # Edit failed — delete the stale streamed placeholder so it
            # doesn't linger as a duplicate above the fresh chunks.
            try:
                tg("deleteMessage", chat_id=chat_id, message_id=answer_id)
            except Exception:
                pass
            answer_id = None
            target_id = None
    else:
        target_id = None

    for c in chunks:
        rest = send_long(chat_id, c)
        if rest and rest.get("result", {}).get("message_id"):
            target_id = rest["result"]["message_id"]
            target_text = c

    # IDE-imported sessions keep their fixed conversation id (don't let agy
    # re-mint a fresh one, even if the symlink ever failed to resolve)
    if s.get("source") == "ide":
        conv_id = s.get("ide_id") or conv_id
    if conv_id:
        save_session(chat_id, s["name"], conv_id, s["model"], cwd)
        # keep the pinned-status title fresh (local runs get a derived title)
        title = conv_title(conv_id)
        if title:
            db.execute("UPDATE sessions SET title=? WHERE chat_id=? AND name=?",
                       (title, chat_id, s["name"]))
            db.commit()

    # Repaint the pin: run-state line flips to ✅ done (tools/files/duration)
    # and the "Writing answer…" live state is dropped.
    update_pin(chat_id)

    opts = detect_options(answer)
    if opts:
        pending[chat_id] = [f"{label}. {body}" for label, body in opts]
        kb = {"inline_keyboard": [[
            {"text": f"{label}. {body[:32]}", "callback_data": f"opt:{i}"}
        ] for i, (label, body) in enumerate(opts)]}
        if target_id:
            # Re-edit the message that holds the LAST chunk with ITS OWN text.
            # (Previously this wrote rendered[:4000] — the FIRST 4000 chars —
            #  onto the LAST chunk's message, which both truncated long answers
            #  and duplicated the opening paragraph.)
            edit_msg(chat_id, target_id, target_text or first_chunk,
                     reply_markup=kb)
        else:
            send_long(chat_id, "Tap an option to continue:", reply_markup=kb)
    if conv_id:
        with _active_convs_lock:
            _active_convs.discard(conv_id)

    # Automated continuation if a task was launched or answer indicates waiting:
    # Scan for task-NNN patterns in answer and output buffer
    if conv_id:
        t_matches = re.findall(r"(?:task[-_]id:?|TaskId:?|task id:?)\s*\"?([a-zA-Z0-9_-]+/task-\d+|task-\d+)\"?", answer + " " + _buf.decode("utf-8", "replace"), re.I)
        for tm in t_matches:
            if tm not in _pending_tasks:
                _pending_tasks.append(tm)
        
        waiting_patterns = [
            r"will wait for (?:it|the command|the task)",
            r"launched the command and will wait",
            r"running (?:in the background|as a background task)",
            r"wait for (?:the task|completion|it) to finish",
            r"waiting for the (?:timer|task|subagent)",
            r"sent to the background as a task",
        ]
        is_waiting = any(re.search(p, answer, re.I) for p in waiting_patterns)
        
        # If pending tasks found or waiting indicated, start background watcher to deliver final answer
        if _pending_tasks or is_waiting:
            # If no explicit task ID found, find latest task in brain tasks dir
            if not _pending_tasks:
                tdir = os.path.expanduser(f"~/.gemini/antigravity-ide/brain/{conv_id}/.system_generated/tasks")
                if os.path.exists(tdir):
                    tfiles = sorted(os.listdir(tdir), key=lambda x: os.path.getmtime(os.path.join(tdir, x)), reverse=True)
                    if tfiles:
                        _pending_tasks.append(tfiles[0].replace(".log", ""))
            if _pending_tasks:
                threading.Thread(
                    target=_watch_task_and_resume,
                    args=(chat_id, s["name"], conv_id, _pending_tasks),
                    daemon=True
                ).start()

    # (Thinking is now embedded in the answer message as a <tg-spoiler> block —
    # see the render step above — so no separate reply is needed.)


# ---- tool-event rendering (commands, file ops, diffs) ----
def _short(text, n=600):
    text = (text or "").replace("\r", "")
    if len(text) <= n:
        return text
    return text[:n] + f"\n… (+{len(text)-n} more chars)"


def _pretty_path(p):
    """Compact a filesystem path for chat (home -> ~). URLs/queries and
    bare names pass through unchanged so the bubble stays informative."""
    s = (p or "").strip()
    if not s or "://" in s:
        return s
    n = os.path.normpath(s)
    if os.path.isabs(s):
        nh = os.path.normpath(os.path.expanduser("~"))
        if n.startswith(nh):
            return "~" + n[len(nh):]
    return s


def _render_manage_task_done(chat_id, out):
    """Render a manage_task DONE status report as a structured card instead of
    a flat escaped blob.

    agy's manage_task status output looks roughly like:
        Task:  <conv>/task-2126
        Status: RUNNING
        Log:   <path>/task-2126.log
        Log output:
          <terminal tail lines…>
        Last progress: 2s ago

    We turn the header key:value lines into bold labels, and render the
    multi-line terminal/log tail as a REAL <pre> code block (monospace,
    bordered) — code stays code. Any unrecognised body is escaped as prose.
    """
    if not out or not out.strip():
        return
    lines = out.splitlines()
    card = []                 # rendered header lines (HTML)
    code_region = []          # collected log/terminal tail lines
    i, n = 0, len(lines)
    # common header labels that carry key:value pairs (render as bold)
    label_re = re.compile(r"^(Task|Status|Log|State|Progress|Goal|Subtask[\w ]*|[A-Za-z ]*time|Agent|Model|Last progress)\s*:\s*(.*)$")
    code_started = False

    def flush_card():
        if card:
            send_long(chat_id, "\n".join(card))

    while i < n:
        raw = lines[i]
        stripped = raw.strip()
        low = stripped.lower()
        # A code/log section header starts a tail region whose body is code.
        if (low.startswith("log output") or low == "output"
                or low.startswith("log tail") or low.startswith("console")
                or low.startswith("stdout") or low.startswith("tail")):
            code_started = True
            header = "🧾 <b>Log output</b>"
            if card:
                header = "\n" + header
            send_long(chat_id, "\n".join(card) + "\n" + header)
            card = []
            i += 1
            continue
        if code_started:
            # A new header label (Task:/Status:/Last progress:) ends the code tail.
            lm = label_re.match(stripped)
            if lm and (low.startswith("last progress") or low.startswith("status")
                       or low.startswith("task:") or low.startswith("log:")):
                if code_region:
                    send_long(chat_id,
                              "<pre>" + _h("\n".join(code_region).rstrip()) + "</pre>")
                    code_region = []
                code_started = False
                # fall through to render this line as a label below
            else:
                code_region.append(raw)
                i += 1
                continue
        # key : value label line -> bold label + escaped value
        lm = label_re.match(stripped)
        if lm:
            label = lm.group(1).strip()
            value = lm.group(2).strip()
            if label.lower() == "status":
                badge = {"RUNNING": "🟢", "DONE": "✅", "COMPLETE": "✅",
                         "SUCCESS": "✅", "FAILED": "🔴", "ERROR": "🔴",
                         "PENDING": "⏳", "QUEUED": "⏳", "CANCELED": "⛔",
                         "CANCELLED": "⛔"}.get(value.upper(), "·")
                card.append(f"<b>{_h(label)}</b>: {badge} <b>{_h(value)}</b>")
            else:
                v = _h(_short(value, 200))
                card.append(f"<b>{_h(label)}</b>: {v}" if v else f"<b>{_h(label)}</b>")
            i += 1
            continue
        if low in ("", "last progress:") or stripped in ("", "-", "—"):
            if card:
                flush_card()
                card = []
            i += 1
            continue
        # plain body line -> prose (escaped)
        card.append(_h(raw))
        i += 1
    if code_started and code_region:
        send_long(chat_id, "<pre>" + _h("\n".join(code_region).rstrip()) + "</pre>")
    if card:
        flush_card()


def _render_tool(chat_id, su, tool_seen):
    """Render one agy `tool` step_update as a Telegram message.
    Follows the Antigravity IDE chat flow: clean inline tool cards and badges,
    without spamming document attachments for routine file reads or searches."""
    st = su.get("step_type")
    if st != "tool":
        return
    idx = su.get("step_index")
    state = su.get("state")
    info = su.get("tool_info") or {}
    name = info.get("name") or su.get("tool_name") or "tool"
    params = info.get("parameters") or {}
    output = info.get("output")
    key = (idx, name)

    IS_FILE = name in ("replace_file_content", "multi_replace_file_content",
                       "write_to_file", "delete_file")

    if state == "ACTIVE":
        if key in tool_seen:
            return
        tool_seen.add(key)
        if name in ("run_command", "execute_browser_javascript"):
            cmd = (params.get("CommandLine") or params.get("code") or "").strip()
            summary = params.get("toolSummary") or params.get("toolAction") or ""
            hdr = "⚡ <b>Run command</b>" + (f" · <i>{_h(summary)}</i>" if summary and summary != "Running command" else "")
            send_long(chat_id, f"{hdr}\n<pre><code>{_h(_short(cmd, 1200))}</code></pre>")
        elif IS_FILE:
            loc = _pretty_path(params.get("TargetFile") or params.get("TargetPath") or params.get("path")
                               or params.get("FilePath") or params.get("Path") or "")
            desc = params.get("Description") or params.get("Instruction") or params.get("toolSummary") or ""
            hdr = f"✏️ <b>Edit file</b> <code>{_h(loc)}</code>"
            if desc and len(desc) <= 150:
                hdr += f"\n<i>{_h(desc)}</i>"
            send_long(chat_id, hdr)
        elif name in ("view_file", "read_url_content", "open_browser_url"):
            loc = _pretty_path(params.get("AbsolutePath") or params.get("TargetPath") or params.get("path")
                               or params.get("FilePath") or params.get("url")
                               or params.get("URL") or "")
            sl = params.get("StartLine")
            el = params.get("EndLine")
            range_str = f" (L{sl}-{el})" if sl and el else ""
            summary = params.get("toolSummary") or params.get("toolAction") or ""
            hdr = f"📂 <b>View file</b> <code>{_h(loc)}{range_str}</code>"
            if summary and summary not in ("Viewing file", "View file"):
                hdr += f" · <i>{_h(summary)}</i>"
            send_long(chat_id, hdr)
        elif name == "grep_search":
            q = params.get("Query") or ""
            p = _pretty_path(params.get("SearchPath") or "")
            send_long(chat_id, f"🔎 <b>Search</b> <code>{_h(str(q))}</code> in <code>{_h(str(p))}</code>")
        elif name == "list_dir":
            p = _pretty_path(params.get("DirectoryPath") or "")
            send_long(chat_id, f"📁 <b>List directory</b> <code>{_h(str(p))}</code>")
        elif name == "manage_task":
            action = str(params.get("Action") or params.get("action")
                         or params.get("Command") or "").strip()
            human = str(params.get("toolAction") or params.get("toolSummary") or "").strip()
            tid = str(params.get("TaskId") or params.get("taskId") or "").strip()
            tid_tail = tid.rsplit("/", 1)[-1] if tid else ""
            piece = [p for p in (human or action, tid_tail) if p and len(p) <= 40]
            send_long(chat_id, "🗂️ <b>Task</b>"
                      + (f" · {_h(' · '.join(piece))}" if piece else ""))
        elif name == "browser_subagent":
            tname = params.get("TaskName") or params.get("TaskSummary") or ""
            send_long(chat_id, f"🌐 <b>Browser</b> · <i>{_h(tname)}</i>")
        else:
            tgt = next((str(params.get(k) or "").strip()
                        for k in ("TargetFile", "TargetPath", "FilePath", "Path", "path",
                                  "file_path", "file", "target", "filePath")
                        if params.get(k)), "")
            tgt = _pretty_path(tgt)
            send_long(chat_id, f"🔧 <b>{_h(name)}</b>"
                      + (f" · <code>{_h(tgt)}</code>" if tgt else ""))
    elif state == "DONE":
        if IS_FILE:
            d = _diff_from_tool_info(info) or _diff_from_tool_info(params)
            loc = (params.get("TargetFile") or params.get("TargetPath") or params.get("path")
                   or params.get("FilePath") or params.get("Path") or "").strip()
            if d:
                old, new, path, is_edit = d
                base = os.path.basename(path or loc or name)
                if is_edit and old is not None:
                    diff_txt = _format_diff(old, new, path or loc)
                    added, removed = _count_diff_changes(diff_txt)
                    send_long(chat_id, f"✅ <i>Edited <code>{_h(base)}</code> (+{added} −{removed})</i>")
                else:
                    send_long(chat_id, f"✅ <i>Wrote <code>{_h(base)}</code> ({len(new.splitlines())} lines)</i>")
            return
        if output is None:
            return
        out = str(output).replace("\r", "").strip()
        if not out:
            return
        if name == "manage_task":
            _render_manage_task_done(chat_id, out)
            return
        if name == "run_command":
            if any(k in out for k in ("Traceback", "Error", "error")) and len(out) < 600:
                send_long(chat_id, f"⚠️ <pre><code>{_h(out)}</code></pre>")
            return



# ---- pickers ----
def show_workspace_picker(chat_id):
    cur = (get_session(chat_id).get("workspace") or DEFAULT_WORKSPACE)
    kb, text = show_dir(cur, chat_id)
    send_long(chat_id, text, reply_markup=kb)

def list_dir(path):
    """Return (subdirs, error) for a directory. Use os.scandir for speed."""
    out = []
    try:
        with os.scandir(path) as it:
            for e in it:
                if e.is_dir() and not e.name.startswith("."):
                    out.append(e.name)
    except Exception as e:
        return [], str(e)
    return sorted(out), None

def show_dir(path, chat_id, header="📂 Browse for a workspace:"):
    path = os.path.abspath(path)
    sub, err = list_dir(path)
    if err:
        sub = []
    browse_state[chat_id] = path
    browse_labels[chat_id] = sub  # index -> name, used by wscd:<idx>
    rows = []
    # Up: climb one level (never above filesystem root)
    if os.path.relpath(path, "/") not in ("", "."):
        rows.append([{"text": "⬆️ ..", "callback_data": "wscd:up"}])
    for i, d in enumerate(sub):
        rows.append([{"text": "📁 " + d, "callback_data": f"wscd:{i}"}])
    rows.append([{"text": "✅ Select this folder", "callback_data": "wssel:now"}])
    return {"inline_keyboard": rows}, header + f"\n<i>{_h(path)}</i>"

def show_model_picker(chat_id):
    s = get_session(chat_id)
    cur_models = get_models()
    kb = {"inline_keyboard": [[
        {"text": ("✅ " if m[0] == s["model"] else "") + m[1][:45], "callback_data": "m:" + m[0]}
    ] for m in cur_models]}
    send_long(chat_id, "Pick a model (✅ = current):", reply_markup=kb)

def show_session_picker(chat_id):
    sess = all_sessions(chat_id)
    act = active_name(chat_id)
    rows = []
    for i, (name, conv, model, ws, source, ide_id, title) in enumerate(sess):
        mark = "▶ " if name == act else ""
        tag = "💻" if source == "ide" else ""
        label = f"{mark}{tag}{name}  ({model})"[:50]
        rows.append([{"text": label, "callback_data": f"s:{i}"},
                     {"text": "🗑", "callback_data": f"del:{i}"}])
    rows.append([{"text": "➕ New session", "callback_data": "s:new"}])
    # Folded-in IDE linking (was the separate /imports command): list the
    # on-disk IDE threads so you can link+switch to one without a second menu.
    ide = ide_list()
    if ide:
        rows.append([{"text": "💻 Link an IDE thread…", "callback_data": "idelink:open"}])
    send_long(chat_id, "Your sessions (▶ = active, 💻 = IDE-linked, 🗑 = delete):",
              reply_markup={"inline_keyboard": rows})

def show_ide_picker(chat_id):
    ide = ide_list()
    if not ide:
        send_long(chat_id, "No IDE conversations found in ~/.gemini/antigravity-ide/conversations")
        return
    rows = []
    for cid, ws, mtime in ide[:12]:
        label = os.path.basename(ws.rstrip("/")) or ws
        title = None
        try:
            title = ide_title(cid)
        except Exception:
            pass
        disp = (title or label)[-40:]
        btn = f"💻 {disp}  ·  {time.strftime('%m-%d %H:%M', time.localtime(mtime))}"[:48]
        rows.append([{"text": btn, "callback_data": f"imp:{cid}"}])
    rows.append([{"text": "⬅️ Back to sessions", "callback_data": "idelink:back"}])
    send_long(chat_id, "Link an IDE session (tap to link + switch):",
              reply_markup={"inline_keyboard": rows})

# ---- command handling ----
HELP = (
    "🤖 Antigravity (agy) bridge\n"
    "Send a message — it runs in your ACTIVE session and streams the answer live.\n"
    "The 📌 pinned message is your always-on dashboard: run state "
    "(running / stalled / done / failed), 5h + weekly quota for Google and "
    "third-party models, credits and system load. No command needed.\n\n"
    "Essentials:\n"
    "/sessions — switch, delete, or LINK an IDE thread (buttons)\n"
    "/new [name] — start & switch to a fresh session\n"
    "/model — pick a model\n"
    "/workspace — show or pick the session folder\n"
    "/history [n] — your last n messages + the bot's replies (default 5)\n"
    "/effort low|medium|high — set reasoning effort\n"
    "/steer <text> — guide the RUNNING turn without killing it "
    "(queued to run right after)\n"
    "/credits [on|off] — AI credits: spend them once the quota hits 0\n"
    "/abort — abort current running task or thinking (also via 🛑 Abort button)\n"
    "/pin — pin the live status message\n\n"
    "IDE threads auto-link to this chat — no command needed. Use /sessions → 💻 Link to connect one.\n\n"
    "MCQ: answers A/B/C/D become tappable buttons — tap to continue."
)

def handle_command(chat_id, text):
    parts = text.split(None, 1)
    cmd = parts[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""
    s = get_session(chat_id)

    if cmd in ("/start", "/help"):
        kb = {"remove_keyboard": True} if cmd == "/start" else None
        send_long(chat_id, HELP, reply_markup=kb)
    elif cmd in ("/abort", "/stop", "/cancel"):
        abort_task(chat_id, reason="command", notify=True)
        return
    elif cmd in ("/usage", "/quota"):
        # Folded into the pinned dashboard; kept as a typed alias.
        lines = [quota_block()]
        tok = _get_session_usage_line(chat_id, s)
        if tok:
            lines.append(tok)
        sys_u = _get_system_usage()
        if sys_u:
            lines.append(sys_u)
        lines.append("\n<i>Also live in the 📌 pinned message.</i>")
        send_long(chat_id, "\n".join(lines))
        return
    elif cmd == "/steer":
        if not arg:
            send_long(chat_id,
                      "Usage: <code>/steer &lt;guidance&gt;</code>\n\n"
                      "Feeds guidance to the <b>currently running</b> turn without "
                      "killing it — it runs immediately after that turn finishes. "
                      "With no turn running it just runs as a normal prompt.")
            return
        if chat_running(chat_id):
            n = steer_enqueue(chat_id, arg)
            send_long(chat_id,
                      f"⏩ Steer queued — runs right after the current turn. "
                      f"({n} queued)")
        else:
            send_long(chat_id, "⏩ No active turn — running your guidance now.")
            run_agy(chat_id, arg)
        return
    elif cmd == "/credits":
        a = arg.lower()
        if a in ("on", "enable", "true", "1", "yes"):
            state = set_use_ai_credits(True)
            send_long(chat_id,
                      ("✅ AI credits ON — agy falls back to credits when the "
                       "quota hits 0.\n\n" if state else "⚠️ Could not save the "
                       "setting.\n\n") + credits_text())
        elif a in ("off", "disable", "false", "0", "no"):
            state = set_use_ai_credits(False)
            send_long(chat_id,
                      ("🛑 AI credits OFF — agy stops at the quota limit.\n\n"
                       if not state else "⚠️ Could not save the setting.\n\n")
                      + credits_text())
        else:
            send_long(chat_id, credits_text())
        return
    elif cmd == "/new":
        n = new_session(chat_id, arg or None)
        send_long(chat_id, f"✅ New session: {n}\nSend a message to talk to it.")
    elif cmd == "/sessions":
        show_session_picker(chat_id)
    elif cmd == "/import":
        if not arg:
            send_long(chat_id, "Usage: /import <ide-conversation-id>\nOr use /imports to pick one.")
            return
        cid = arg.strip().split()[0]
        nm = import_ide_session(chat_id, cid)
        if nm:
            send_long(chat_id, import_reply(nm, cid))
        else:
            send_long(chat_id, f"❌ No IDE conversation with id {cid}. Use /imports to list.")
    elif cmd == "/rename":
        if not arg:
            send_long(chat_id, "Usage: /rename <new name>")
            return
        name = active_name(chat_id) or "default"
        db.execute("UPDATE sessions SET name=? WHERE chat_id=? AND name=?", (arg, chat_id, name))
        db.execute("UPDATE active SET name=? WHERE chat_id=?", (arg, chat_id))
        db.commit()
        send_long(chat_id, f"✅ Renamed session to: {arg}")
    elif cmd == "/model":
        show_model_picker(chat_id)
    elif cmd == "/workspace":
        if arg:
            w = os.path.expanduser(arg)
            os.makedirs(w, exist_ok=True)
            save_session(chat_id, s["name"], s["conv_id"], s["model"], w)
            send_long(chat_id, f"✅ Workspace: {w}")
            update_pin(chat_id)
        else:
            show_workspace_picker(chat_id)
    elif cmd == "/effort":
        if arg in ("low", "medium", "high"):
            db.execute("INSERT INTO prefs(chat_id,effort) VALUES(?,?) "
                       "ON CONFLICT(chat_id) DO UPDATE SET effort=excluded.effort", (chat_id, arg))
            db.commit()
            send_long(chat_id, f"✅ Effort → {arg}")
        else:
            e = get_effort(chat_id)
            send_long(chat_id, f"Effort: {e or 'default'}\nUsage: /effort low|medium|high")
    elif cmd == "/history":
        n = 5
        if arg.isdigit():
            n = max(2, min(40, int(arg)))
        s = get_session(chat_id)
        send_history(chat_id, s["name"], n=n)
    elif cmd == "/pin":
        if chat_id in pinned:
            edit_msg(chat_id, pinned[chat_id], status_text(chat_id))
            send_long(chat_id, "📌 Status message already pinned — it stays live as you switch sessions/models.")
        else:
            r = send_long(chat_id, status_text(chat_id))
            mid = r.get("result", {}).get("message_id") if r else None
            if mid:
                pinned[chat_id] = mid
                tg("pinChatMessage", chat_id=chat_id, message_id=mid)
                save_pin(chat_id, mid)
                send_long(chat_id, "📌 Pinned! This message auto-updates on model/session switches and after each reply.")
    elif cmd == "/push":
        msg = arg if arg else None
        send_long(chat_id, git_push(chat_id, msg))
    elif cmd == "/id":
        send_long(chat_id, f"chat_id = {chat_id}")
    else:
        send_long(chat_id, f"Unknown command: {cmd}\nTry /help")

# ---- callback handling ----
def handle_callback(cq):
    msg = cq.get("message")
    if not msg:
        return
    chat_id = str(msg["chat"]["id"])
    user_id = str(cq.get("from", {}).get("id", ""))
    tg("answerCallbackQuery", callback_query_id=cq["id"])
    if user_id not in ALLOWED:
        tg("answerCallbackQuery", callback_query_id=cq["id"], text="🚫 Unauthorized", show_alert=True)
        return
    data = cq.get("data", "")
    mid = msg["message_id"]

    if data.startswith("abort:"):
        tg("answerCallbackQuery", callback_query_id=cq["id"], text="🛑 Aborting…")
        abort_task(chat_id, reason="button", notify=True)
        return

    if data in ("credits:on", "credits:off"):
        on = data.endswith(":on")
        set_use_ai_credits(on)
        tg("answerCallbackQuery", callback_query_id=cq["id"],
           text="✅ AI credits ON" if on else "🛑 AI credits OFF")
        edit_msg(chat_id, mid, credits_text())
        return

    if data.startswith("m:"):
        mid_model = data[2:]
        s = get_session(chat_id)
        save_session(chat_id, s["name"], s["conv_id"], mid_model, s["workspace"])
        edit_msg(chat_id, mid, f"✅ Model set → <b>{get_model_name(mid_model)}</b>\n\nSend a message to chat.")
        update_pin(chat_id)
    elif data.startswith("s:"):
        arg = data[2:]
        if arg == "new":
            n = new_session(chat_id, None)
            edit_msg(chat_id, mid, f"✅ Started & switched to: {n}")
        else:
            try:
                idx = int(arg)
            except ValueError:
                return
            sess = all_sessions(chat_id)
            if 0 <= idx < len(sess):
                save_session(chat_id, sess[idx][0], sess[idx][1], sess[idx][2], sess[idx][3])
                edit_msg(chat_id, mid, f"✅ Switched to: {sess[idx][0]}")
        update_pin(chat_id)
    elif data.startswith("wsx:"):  # legacy top-level picker; show_dir now uses wscd:/wssel:
        arg = data[3:]
        s = get_session(chat_id)
        if arg == "custom":
            edit_msg(chat_id, mid, "Send: /workspace <full/path>")
            return
        if arg == "browse:" or arg == "list":
            kb, text = show_dir(s.get("workspace") or DEFAULT_WORKSPACE, chat_id)
            edit_msg(chat_id, mid, text, reply_markup=kb)
            return
        if arg.startswith("browse:"):
            path = arg[len("browse:"):] or (s.get("workspace") or DEFAULT_WORKSPACE)
            kb, text = show_dir(path, chat_id)
            edit_msg(chat_id, mid, text, reply_markup=kb)
            return
        try:
            idx = int(arg)
        except ValueError:
            return
        dirs = ws_dirs()
        if 0 <= idx < len(dirs):
            d = dirs[idx]
            os.makedirs(d, exist_ok=True)
            save_session(chat_id, s["name"], s["conv_id"], s["model"], d)
            edit_msg(chat_id, mid, f"✅ Workspace → {d}")
            update_pin(chat_id)
    elif data.startswith("wscd:"):
        tok = data[len("wscd:"):]
        cur = browse_state.get(chat_id)
        if not cur or not os.path.isdir(cur):
            kb, text = show_dir(get_session(chat_id).get("workspace") or DEFAULT_WORKSPACE, chat_id)
            edit_msg(chat_id, mid, text, reply_markup=kb)
            return
        if tok == "up":
            target = os.path.dirname(cur)
        else:
            try:
                i = int(tok)
            except ValueError:
                return
            labels = browse_labels.get(chat_id, [])
            if not (0 <= i < len(labels)):
                return
            target = os.path.join(cur, labels[i])
        if not os.path.isdir(target):
            tg("answerCallbackQuery", callback_query_id=cq["id"], text="🚫 Not a folder", show_alert=True)
            return
        kb, text = show_dir(target, chat_id)
        edit_msg(chat_id, mid, text, reply_markup=kb)
    elif data.startswith("wssel:"):
        cur = browse_state.get(chat_id)
        if not cur or not os.path.isdir(cur):
            tg("answerCallbackQuery", callback_query_id=cq["id"], text="🚫 Pick a folder first", show_alert=True)
            return
        s = get_session(chat_id)
        save_session(chat_id, s["name"], s["conv_id"], s["model"], cur)
        browse_state.pop(chat_id, None)
        browse_labels.pop(chat_id, None)
        edit_msg(chat_id, mid, f"✅ Workspace → {cur}")
        update_pin(chat_id)
    elif data == "idelink:open":
        show_ide_picker(chat_id)
    elif data == "idelink:back":
        show_session_picker(chat_id)
    elif data.startswith("imp:"):
        cid = data[4:]
        nm = import_ide_session(chat_id, cid)
        if nm:
            edit_msg(chat_id, mid, f"✅ Linked: {nm}")
        else:
            edit_msg(chat_id, mid, f"❌ Could not link {cid}")
        update_pin(chat_id)
    elif data.startswith("del:"):
        try:
            idx = int(data[4:])
        except ValueError:
            return
        sess = all_sessions(chat_id)
        if 0 <= idx < len(sess):
            name, conv, model, ws, source, ide_id, title = sess[idx]
            db.execute("DELETE FROM sessions WHERE chat_id=? AND name=?", (chat_id, name))
            if source == "ide":
                remove_ide_link(ide_id)
            if active_name(chat_id) == name:
                remain = all_sessions(chat_id)
                newact = remain[0][0] if remain else "default"
                set_active(chat_id, newact)
            db.commit()
            edit_msg(chat_id, mid, f"🗑 Deleted session: {name}")
    elif data.startswith("opt:"):
        try:
            idx = int(data[4:])
        except ValueError:
            return
        opts = pending.get(chat_id)
        if opts and 0 <= idx < len(opts):
            run_agy(chat_id, opts[idx])
    elif data.startswith("ide:"):
        # Auto-link: bind this Telegram chat to the IDE conversation referenced
        # by the IDE-activity message. Import (idempotent) + make it active so
        # the user's next reply continues the SAME IDE thread seamlessly.
        # Because CLI_CONV_DIR is symlinked to the IDE store, replies go into
        # the IDE's real .db and show up in the laptop IDE (cloud-synced) — the
        # session is now genuinely two-way (Telegram <-> IDE).
        cid = data[4:]
        nm = import_ide_session(chat_id, cid)
        if nm:
            edit_msg(chat_id, mid, f"💻 <b>{_html_name(nm)}</b> — linked & active\n"
                                   f"Your next Telegram message continues this IDE thread, "
                                   f"and the IDE shows it too. Two-way, nothing missed.")
        else:
            edit_msg(chat_id, mid, f"❌ Could not link IDE {cid}")
    elif data.startswith("proceed:"):
        # One-tap "Proceed": link the IDE thread (so replies also land in the
        # laptop IDE) and tell agy to implement the plan now.
        cid = data[8:]
        nm = import_ide_session(chat_id, cid)
        if nm:
            edit_msg(chat_id, mid, f"✅ Proceeding with the plan in <b>{_html_name(nm)}</b> "
                                   f"— implementing now…")
            run_agy(chat_id,
                    "✅ Proceed: implement the plan in implementation_plan.md (and task.md). "
                    "Start now and report progress as you go.")
        else:
            edit_msg(chat_id, mid, f"❌ Could not link IDE {cid}")

_inbound_buffers = {}       # chat_id -> list of text chunks
_inbound_timers = {}        # chat_id -> threading.Timer
_inbound_lock = threading.Lock()

def _dispatch_buffered(chat_id):
    with _inbound_lock:
        chunks = _inbound_buffers.pop(chat_id, [])
        _inbound_timers.pop(chat_id, None)
    if not chunks:
        return
    full_text = "\n".join(c.strip() for c in chunks if c.strip())
    if not full_text:
        return
    sess = get_session(chat_id)
    sname = sess["name"] if sess else None
    log_turn(chat_id, sname, "user", full_text)
    run_agy(chat_id, full_text)

# ---- inbound dispatch ----
def handle(msg):
    chat_id = str(msg["chat"]["id"])
    user_id = str(msg.get("from", {}).get("id", ""))
    if user_id not in ALLOWED:
        send_long(chat_id, "🚫 Unauthorized.")
        return

    # Ensure this chat has at least one session — a missing row would crash
    # every get_session(chat_id)["name"] below with a KeyError.
    if not get_session(chat_id):
        new_session(chat_id, None)

    # Reflect the user's own message (text + media) into our thread log so
    # /history and the IDE-preview show what they actually sent.
    caption = (msg.get("caption") or "").strip()
    text = (msg.get("text") or "").strip()

    # ---- voice / audio note ----
    voice = msg.get("voice") or msg.get("audio")
    if voice:
        fid = voice.get("file_id")
        send_long(chat_id, "🎙️ Transcribing your voice note…")
        path = download_file(fid, ext=".oga")
        if not path:
            send_long(chat_id, "❌ Could not download the voice note.")
            return
        # normalize to 16k mono wav for whisper
        wav = path + ".wav"
        try:
            subprocess.run(["ffmpeg", "-y", "-i", path, "-ar", "16000",
                            "-ac", "1", wav], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            wav = path
        txt = transcribe_voice(wav)
        log_turn(chat_id, get_session(chat_id)["name"], "user",
                 "[voice] " + txt)
        send_long(chat_id, f"🎙️ <b>You (voice)</b>:\n{_h(txt)}")
        full = (caption + "\n\n" if caption else "") + "[voice transcript]\n" + txt
        run_agy(chat_id, full)
        return

    # ---- photo ----  (download; let the agent view it natively)
    if msg.get("photo"):
        photo = msg["photo"][-1]  # largest
        send_long(chat_id, "🖼️ Saving your image for the agent to view…")
        path = download_file(photo.get("file_id"), ext=".jpg")
        if not path:
            send_long(chat_id, "❌ Could not download the image.")
            return
        log_turn(chat_id, get_session(chat_id)["name"], "user",
                 f"[image saved to {path}] " + (caption or ""))
        send_long(chat_id, f"🖼️ <b>You (image)</b> saved to <code>{_h(path)}</code>"
                   + (f"\n{_h(caption)}" if caption else ""))
        full = (caption + "\n\n" if caption else "") + \
               f"[attached image saved to {path} — please view it directly]"
        run_agy(chat_id, full)
        return

    # ---- document / file ----  (download; let the agent read it natively)
    doc = msg.get("document")
    if doc:
        fid = doc.get("file_id")
        fname = doc.get("file_name", "")
        send_long(chat_id, f"📎 Downloading <b>{_h(fname)}</b>…")
        ext = os.path.splitext(fname)[1] or ""
        path = download_file(fid, ext=ext)
        if not path:
            send_long(chat_id, "❌ Could not download the file.")
            return
        size = os.path.getsize(path)
        is_image = (doc.get("mime_type") or "").startswith("image/")
        verb = "view" if is_image else "read"
        log_turn(chat_id, get_session(chat_id)["name"], "user",
                 f"[file {fname} ({size} bytes) saved to {path}] " + caption)
        send_long(chat_id, f"📎 <b>{_h(fname)}</b> ({size} bytes) saved to "
                   f"<code>{_h(path)}</code>. {_h(caption)}")
        full = (caption + "\n\n" if caption else "") + \
               f"[attached file saved to {path} ({size} bytes) — please {verb} it directly]"
        run_agy(chat_id, full)
        return

    # ---- plain text ----
    if not text:
        return
    if text.startswith("/"):
        log_turn(chat_id, get_session(chat_id)["name"], "user", text)
        handle_command(chat_id, text)
        return

    # Debounce inbound plain text by 0.8s to seamlessly join chunked Telegram messages
    with _inbound_lock:
        if chat_id not in _inbound_buffers:
            _inbound_buffers[chat_id] = []
        _inbound_buffers[chat_id].append(text)
        if chat_id in _inbound_timers:
            try:
                _inbound_timers[chat_id].cancel()
            except Exception:
                pass
        t = threading.Timer(0.8, _dispatch_buffered, args=[chat_id])
        _inbound_timers[chat_id] = t
        t.daemon = True
        t.start()

# ---- poll loop ----

def ensure_unified_store():
    """Make the CLI store's data dirs point at the IDE store so `agy` (CLI)
    and the editlittle IDE editor share one physical conversation .db. This is
    the mechanism that lets Telegram and the laptop IDE stay in lockstep.

    Only shares dirs that exist in the IDE store; dirs that don't (cache,
    presence, scratch on a fresh IDE) stay as real CLI-private dirs so `agy`
    can still write them.
    """
    for d in ("conversations", "brain", "implicit"):
        src = os.path.join(IDE_ROOT, d)          # real IDE dir
        dst = os.path.join(CLI_ROOT, d)          # CLI path we rewrite
        if not os.path.isdir(src):
            continue
        if os.path.islink(dst):
            continue
        if os.path.isdir(dst) and os.listdir(dst):
            # Keep the CLI's own copy; don't clobber. (Shouldn't happen post-migration.)
            continue
        if os.path.exists(dst):
            shutil.move(dst, dst + ".cli.bak")
        os.symlink(src, dst)
        print(f"unified-store: {dst} -> {src}", flush=True)

def main():
    offset = 0
    ensure_unified_store()
    ensure_pin_table()
    _load_pinned()
    threading.Thread(target=bridge_loop, daemon=True).start()

    def _quota_loop():
        # Warm the dashboard once at boot (each `agy -p` costs a ~12-15s CLI
        # start-up, so do it before anybody looks at the pin), then top it up
        # every QUOTA_REFRESH while no turn is running.
        refresh_quota(force=True)
        agy_credits_status(refresh=True)
        ticks = 0
        while True:
            time.sleep(60)
            ticks += 1
            try:
                if time.time() - float(_quota.get("ts") or 0) < QUOTA_REFRESH:
                    continue
                if _any_run_active() or _quota.get("fetching"):
                    continue
                if refresh_quota(force=True):
                    if ticks % 4 == 0:      # balance moves far less often
                        agy_credits_status(refresh=True)
                    for cid in list(pinned):
                        try:
                            update_pin(cid)
                        except Exception:
                            pass
            except Exception as e:
                print("quota loop error:", e, file=sys.stderr)

    threading.Thread(target=_quota_loop, daemon=True).start()
    time.sleep(2)  # warmup to avoid transient 409 after a restart
    print(f"agy-telegram bridge up. agy={AGY} default_model={DEFAULT_MODEL}", flush=True)
    register_commands()
    # Guard: if the DB has no session for ANY chat, create a sensible default now
    # so the very first inbound message has an active session (no blank run).
    try:
        n = db.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
        if n == 0 and DEFAULT_CHAT:
            new_session(DEFAULT_CHAT, None)
            print("created initial default session", file=sys.stderr)
    except Exception as e:
        print("init session check failed:", e, file=sys.stderr)
    while True:
        try:
            r = requests.get(f"{API}/getUpdates",
                             params={"offset": offset, "timeout": 30,
                                     "allowed_updates": json.dumps(["message", "callback_query"])},
                             timeout=35)
            data = r.json()
        except Exception as e:
            print("poll error", e, file=sys.stderr)
            time.sleep(5)
            continue
        if data.get("ok"):
            for u in data.get("result", []):
                offset = u["update_id"] + 1
                if "message" in u:
                    threading.Thread(target=handle, args=(u["message"],), daemon=True).start()
                elif "callback_query" in u:
                    threading.Thread(target=handle_callback, args=(u["callback_query"],), daemon=True).start()
        else:
            print("getUpdates not ok:", data, file=sys.stderr)
            time.sleep(5)

# ============================================================================
# IDE → Telegram passive forwarding (the "true seamless experience")
#
# When you work in your Antigravity IDE editor, the bridge (running here) polls
# the IDE's OWN conversation .db (shared via the conversations symlink) and the
# brain/<conv>/ folder for plan & task files, then pushes new activity to your
# Telegram chat. This is *passive* — it never adds turns to the IDE; it only
# mirrors what already happened. HTML parse_mode is used so real LLM prose
# (which contains . ! ( ) chars) renders cleanly.
#
# Step-type map we forward (from real IDE db inspection):
#   15 = assistant thinking/commentary (longest readable run, not tool-JSON)
#   21 = run_command      -> "⚡ ran: <cmd>"
#   8/17 = view_file      -> "📂 opened: <path>"
#   5  = write_to_file    -> "✏️ wrote: <path/summary>"
#   7  = grep_search      -> "🔎 grep <query> in <path>"
# Plan/task files in brain/<conv>/ are forwarded on mtime change.
# Telegram-originated turns are skipped (last_telegram_send) so no double-post.
# ============================================================================

IDE_ROOT = os.path.expanduser("~/.gemini/antigravity-ide")
BRIDGE_POLL = 3.0          # seconds between IDE polls
_active_convs = set()
_active_convs_lock = threading.Lock()

import json as _json

_bridge_thoughts = {}   # conv_id -> list of intermediate thought strings
_TOOL_NAMES = {
    "run_command", "view_file", "write_to_file", "replace_file_content",
    "multi_replace_file_content", "grep_search", "list_dir", "manage_task",
    "browser_subagent", "read_url_content", "open_browser_url",
    "execute_browser_javascript", "ask_question", "send_message"
}

def _is_opaque_key(s):
    """True when s is an opaque structural identifier (not a real answer).

    The terse fallback / title gate must never treat metadata keys like
    'sessionID' or 'conversationID' as if agy had said them. Genuine terse
    answers are TitleCase/ALLCAPS real words ('Done', 'OK', 'Retrying'),
    which have NO interior mixed-case boundary. camelCase words (s + ...
    + Upperletter) and a small known structural-key set are unambiguous
    metadata — reject them generically so no future key needs enumerating."""
    if not s:
        return True
    if re.search(r"[a-z][A-Z]", s):          # camelCase boundary -> identifier
        return True
    return s.lower() in {"sessionid", "conversationid", "conversation_id",
                         "session_id", "request_id", "run_id", "user",
                         "assistant", "system", "cwd", "workspace",
                         "message_id", "reply_to", "channel_id", "chat_id"}


def _visible_text(payload):
    """Extract the human-readable ANSWER text from a step payload, or None.

    The payload is a protobuf blob that also embeds opaque identifiers,
    hashes, paths, base64 and tool metadata. We must NOT forward those as if
    they were the answer. The rule is generic (works for ANY user / thread):
    a real answer is *prose* — it has spaces and real words — whereas ids,
    hashes, tokens and paths are single unbroken (whitespace-free) runs. We
    score candidates by natural-language density and pick the best, rejecting
    non-prose shapes WITHOUT enumerating specific id formats."""
    if not payload:
        return None
    runs = re.findall(rb"[\x20-\x7e]{2,}", payload)
    out = []
    fallback = ""            # best no-space candidate, used only if no prose
    for r in runs:
        s = r.decode("utf-8", "replace").strip().strip('"').strip()
        # drop IDE/protobuf framing residue glued onto a real run: a leading
        # varint length prefix (e.g. "3### ...") and any "(bot-<uuid>:" block
        # pointer. These are step_type frame metadata, never part of the answer.
        s = re.sub(r"\(bot-[0-9a-fA-F]{8,}[-0-9a-fA-F:]*", "", s).strip()
        s = re.sub(r"^\d+(?=#)", "", s)
        if len(s) < 2:
            continue
        # --- Generic non-prose rejections (no specific-id enumeration) ---
        if s.startswith(("b$", "file://", "http", "/", "{", "[", "(", '"', "<")):
            continue                                   # code / path / URL / JSON
        if sum(c.isdigit() for c in s) / len(s) > 0.5:
            continue                                   # mostly digits -> id/ts
        if re.match(r"^[A-Za-z0-9+/=]+$", s) and len(s) > 20:
            continue                                   # pure base64 / opaque token
        if not re.search(r"[A-Za-z]{2,}", s):
            continue                                   # must contain a real word
        # Real answers are PROSE — they contain spaces. The binary protobuf
        # embeds short run-length/metadata tokens (glyph names, framings,
        # opaque ids) like "ay$M", "pWh", "~`%px" that have NO spaces and
        # previously slipped through a len(s) > 16 gate. Reject every no-space
        # run outright; keep the single best one as a fallback so a genuinely
        # terse (single-word) answer is never silently dropped.
        if " " not in s:
            # fallback is ONLY for a genuinely terse ALPHABETIC answer
            # (e.g. "Done", "OK", "Retrying"). Reject every token/id/framing
            # fragment outright: protobuf frame prefixes, (bot-<uuid>, tool
            # names (manage_task), and opaque tokens with digits / underscores
            # (xhyIap..._8QMX) all carry non-letter chars and must never leak
            # into /history as if they were the answer.
            # 2026-08-22: also require at least one LOWERCASE letter, so
            # ALL-CAPS base64/signature substrings (PZZ, PYZ, JX — slices of
            # the embedded opaque tokens) can never qualify as a "real"
            # terse answer. Real ones ("Done", "Retrying", "ok") contain
            # lowercase; the caps-only fragments do not.
            if len(s) >= 2 and re.search(r"[a-z]", s) and len(s) > len(fallback) \
                    and not _is_opaque_key(s):
                fallback = s
            continue
        # PROSE gate: a run only counts as a genuine answer if it contains at
        # least one real alphabetic word of length >= 4. This rejects the tiny
        # 2-4 char binary-residue fragments (`:b Uz`, "dz :", "` TS;") that
        # happen to contain a space but carry no real word — without those,
        # multi-KB real answers (full of 4+ letter words) still pass whole.
        words = re.findall(r"[A-Za-z]{4,}", s)
        if not words:
            continue
        # KEEP every genuine prose run — do NOT collapse to the single
        # "highest-scoring" run. agy's answer blob is interleaved prose +
        # metadata, so the real answer spans MANY whitespace runs; returning
        # only the best one silently truncates multi-KB answers. Joining them
        # back preserves the full text (it is re-rendered through md_to_html,
        # which is tolerant of the stray fragments these runs may contain).
        out.append(s)
    return "\n".join(out) if out else (fallback or None)


def _answer_visible(payload):
    """URL-preserving answer extractor.

    _visible_text() is the RIGHT tool for prose rendering (history/titles) but it
    intentionally drops every run that starts with 'http' (to keep raw tool URLs
    and paths out of rendered prose). That makes it the WRONG tool for last_answer
    recovery: when the agent's whole answer is a list of links (e.g. "give me the
    link"), _visible_text returns the bold labels but strips the actual URLs —
    exactly the silent loss we're fixing.

    This variant walks the same protobuf payload but KEEPS answer URLs
    (edu.fixitinpost.in, 192.168.*, localhost, 127.0.0.1, bare LAN IPs) while still
    rejecting ids/hashes/base64/tool-noise (fonts.googleapis, cdnjs, w3.org, ...
    are NOT in the whitelist so they fall out as non-prose). The final answer steps
    in this agy version carry ONLY the intended links, so the whitelist is safe.
    It also de-dups the payload, which stores the answer text TWICE (once as the
    answer, once echoed), by truncating at the second occurrence of the first line.
    """
    if not payload:
        return None
    if isinstance(payload, str):
        payload = payload.encode()
    runs = re.findall(rb"[\x20-\x7e]{2,}", payload)
    out = []
    for r in runs:
        s = r.decode("utf-8", "replace").strip().strip('"').strip()
        # remove any (bot-<uuid> frame marker (opt. leading varint + trailing proto byte)
        s = re.sub(r"\d*\(bot-[0-9a-fA-F-]+B?", "", s).strip()
        s = re.sub(r"^\d+(?=#)", "", s).strip()
        if len(s) < 2:
            continue
        if s in ("sessionID", "J{") or s.startswith(("b$", "$", ":$", "file://", "http", "/", "{", "[", "(", '"', "<")):
            continue
        if any(k in s for k in ('"toolAction"', '"toolSummary"', '"CommandLine"', '"AbsolutePath"', '"TargetFile"', '"StartLine"')):
            continue
        if s.startswith("-375"):
            continue
        if re.match(r"^[A-Za-z0-9+/=]{20,}$", s):
            continue
        if re.match(r"^https?://", s):
            # keep only the answer-link hosts
            if re.match(
                r"^https?://(edu\.fixitinpost\.in|192\.168\.|localhost|"
                r"127\.0\.0\.1|\d+\.\d+\.\d+\.\d+|\d+\.\d+\.\d+\.\d+:\d+)", s):
                out.append(s)
            continue
        if " " not in s:
            continue
        if not re.search(r"[A-Za-z]{4,}", s):
            continue
        if sum(c.isdigit() for c in s) / len(s) > 0.5:
            continue
        out.append(s)
    # the payload stores the answer twice — truncate at the 2nd copy of the first line
    if out:
        first = out[0]
        for i in range(1, len(out)):
            if out[i] == first:
                out = out[:i]
                break
    return "\n".join(out) if out else None


def _tool_payload_dict(payload):
    """Recover a parsed JSON dict from a tool-call step payload (bytes BLOB).

    Tries the whole payload as JSON first (handles nested braces in code
    content), then falls back to scanning for the first single-level JSON
    object — the same echo shape _tool_meta scans."""
    if not payload:
        return None
    if isinstance(payload, (bytes, bytearray)):
        try:
            s = payload.decode("utf-8", "replace")
        except Exception:
            return None
    else:
        s = str(payload)
    try:
        d = _json.loads(s, strict=False)
        if isinstance(d, dict):
            return d
    except Exception:
        pass
    raw = payload if isinstance(payload, (bytes, bytearray)) else s.encode("utf-8", "replace")
    m = re.search(rb"\{[^{}]+\}", raw)
    if not m:
        return None
    try:
        return _json.loads(m.group(0).decode("utf-8", "replace"), strict=False)
    except Exception:
        return None


def _tool_meta(payload):
    """Parse a tool-call JSON echo; return (kind, summary, detail) or None."""
    d = _tool_payload_dict(payload)
    if not isinstance(d, dict):
        return None
    if "CommandLine" in d:
        return ("cmd", d.get("toolSummary", ""), d["CommandLine"])
    # If explicit tool or CodeContent/ReplacementContent is present, it is a write/edit
    tool_name = str(d.get("Tool") or d.get("name") or "")
    if tool_name in ("replace_file_content", "write_to_file", "multi_replace_file_content") or "CodeContent" in d or "ReplacementContent" in d:
        return ("write", d.get("Description", d.get("Instruction", d.get("toolSummary", ""))),
                d.get("TargetFile") or d.get("AbsolutePath") or d.get("EndLine", ""))
    if "TargetFile" in d:
        return ("write", d.get("Description", d.get("Instruction", d.get("toolSummary", ""))), d["TargetFile"])
    if "AbsolutePath" in d:
        return ("file", d.get("toolSummary", ""), d["AbsolutePath"])
    if "Query" in d and "SearchPath" in d:
        return ("grep", d.get("toolSummary", ""), f'{d["Query"]} in {d["SearchPath"]}')
    if "ReplacementContent" in d or "Instruction" in d:
        return ("write", d.get("Description", d.get("Instruction", "")),
                d.get("TargetFile") or d.get("AbsolutePath") or d.get("EndLine", ""))
    return None


def _bridge_tool_step(chat_id, pl):
    """Forward one non-answer IDE step (file edit / command / grep) to
    Telegram as a SINGLE compact text line — one tidy bubble per step, no
    separate .txt attachment window. Matches how the IDE feed reads on
    Telegram: a lightweight stream of events instead of a wall of documents.
    HTML parse_mode keeps prose clean; inline content is HTML-escaped with
    _h so no raw < > & leaks through."""
    meta = _tool_meta(pl)
    if not meta:
        return
    kind, summary, detail = meta

    def short_esc(s, n=400):
        return _h(_short(s, n))

    if kind in ("file", "write"):
        # Single-line: file path + net line delta — never an attachment.
        path = detail or summary or "file"
        d = _tool_payload_dict(pl)
        added = removed = None
        if d:
            diff = _diff_from_tool_info({"argumentsJson": d})
            if diff:
                old, new, p, is_edit = diff
                path = p or path
                if is_edit and old is not None:
                    dt = _format_diff(old, new, p or "")
                    added, removed = _count_diff_changes(dt)
                else:
                    added = len(new.splitlines()); removed = 0
        base = os.path.basename(str(path)) or str(path)
        if added is not None and removed is not None:
            line = f"✏️ edit <code>{_h(base)}</code> <i>(+{added} −{removed})</i>"
        else:
            line = f"✏️ edit <code>{_h(base)}</code>"
        # keep the summary phrase if it's short and prose-like, else drop it
        if summary and len(summary) <= 140:
            line += " · " + short_esc(summary)
        send_long(chat_id, line)
    elif kind == "cmd":
        # Single line: the command itself. Drop the full stdout attachment —
        # the answer step that follows carries the real output context.
        send_long(chat_id, "⚡ <code>" + short_esc(detail, 600) + "</code>")
    elif kind == "grep":
        send_long(chat_id, "🔎 grep <code>" + short_esc(detail, 300) + "</code>")


def _split_md_code(content):
    """Split markdown into (prose_text, [code_blocks]).

    Plan/task files mix prose with fenced code/diff blocks. We render the
    prose inline (Telegram-HTML) and ship each code/diff block as a clean
    .txt attachment (opencode-style) — never HTML-escaped inside a <pre>.
    Unfenced diffs (lines starting with ---/+++/@@) are also treated as code."""
    lines = (content or "").split("\n")
    prose, blocks = [], []
    cur = None          # current code block contents (list) or None
    fence = None        # fence token (```lang) or the sentinel '\x00diff'
    for ln in lines:
        stripped = ln.strip()
        if fence == "\x00diff":
            # continuation of a bare (unfenced) diff block
            if stripped == "" or stripped[0:1] in (" ", "+", "-") \
                    or stripped.startswith(("@@", "---", "+++", "Index:", "diff ")):
                cur.append(ln)
                continue
            # diff block ended -> flush it, then fall through to prose
            if cur:
                blocks.append("\n".join(cur))
            cur = None
            fence = None
            # (do NOT 'continue' — this line is prose)
        if fence and fence != "\x00diff":
            if stripped.startswith("```"):      # close fence (any language)
                fence = None
                if cur:
                    blocks.append("\n".join(cur))
                cur = None
                continue
            cur.append(ln)
            continue
        if stripped.startswith("```"):
            fence = stripped[3:].strip() or "```"
            if cur is not None:          # flush any prior bare-diff block
                blocks.append("\n".join(cur))
            cur = []
            continue
        # start a bare unfenced diff block?
        if stripped.startswith(("--- ", "+++ ", "@@", "Index:", "diff ")) and cur is None:
            cur = [ln]
            fence = "\x00diff"
            continue
        prose.append(ln)
    if cur is not None:
        blocks.append("\n".join(cur))
    blocks = [b.strip("\n") for b in blocks if b and b.strip()]
    prose = "\n".join(prose).strip()
    return prose, blocks


def _brain_files(conv_id):
    d = os.path.join(IDE_ROOT, "brain", conv_id)
    out = {}
    for name in ("implementation_plan.md", "task.md"):
        p = os.path.join(d, name)
        if os.path.isfile(p):
            try:
                out[name] = (os.path.getmtime(p), open(p).read())
            except Exception:
                pass
    return out


def _junk_session_name(name):
    """True for auto-generated 'garbage' IDE session names we must NOT import
    or surface (named after a file like index.htmlj, unnamed 'ide-unknown',
    code fragments, UUIDs, empty/whitespace)."""
    if not name:
        return True
    n = name.strip()
    if len(n) < 3:
        return True
    if "\n" in n or n.startswith(("\n", ";", "//", "/", "{", "b'", '"', "'",
                                   "file://", "http")):
        return True
    if re.search(r"ide-index\.htmlj|ide-unknown|ide-file\)\.|ide-\.syst", n):
        return True
    if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", n):
        return True
    return False


def _is_title_frame(s):
    """True if s is a bare step-TITLE/LABEL frame — a heading or bold label
    with no body (IDE step metadata like "### Whisper Sync"), NOT an answer.
    CONSERVATIVE on purpose (2026-08-22): only a single-line markdown heading
    (`# ...`) or a lone bold label (`**...**` exactly) qualifies. Bullet
    content (`- **Actual...**: Captured...`), multi-line text, and terse plain
    answers ("Done", "OK") all return False so real /history rows are kept."""
    if not s:
        return True
    t = s.strip()
    if not t:
        return True
    if "\n" in t:
        return False
    # opaque structural identifier (camelCase / known key like 'sessionID')
    # is metadata junk, never a terse real answer ("Done", "OK" are TitleCase
    # with no interior mixed-case boundary, so they pass _is_opaque_key==False)
    if _is_opaque_key(t):
        return True
    # heading frame: starts with a markdown '#' heading
    if re.match(r"^#{1,6}\s+\S", t):
        return True
    # lone bold label (entire single line is exactly **...**)
    if re.fullmatch(r"\*\*[^*]+\*\*", t):
        return True
    return False


def _bridge_once():
    # NOTE: automatic IDE-session *import* was removed on purpose (2026-08-21).
    # /sessions (and /imports to pick one) is the single, deterministic way to
    # attach an IDE thread — auto-import was redundant and kept re-polluting the
    # chat with junk/abandoned sessions. We ONLY forward activity for sessions
    # the user explicitly linked, and keep the title in sync if the IDE renames
    # one. (Verified 2026-08-21: this agy version NEVER persists reasoning
    # text to the .db, so there is no thinking to forward — only the answer.)
    try:
        primary = next(iter(sorted(ALLOWED)), None)
        if primary:
            # Title sync only: if a LINKED session's IDE title changed, rename
            # it so /sessions + the pinned status match the editor. No import.
            for cid, _ws, _mt in ide_list():
                cur = db.execute(
                    "SELECT name,title FROM sessions WHERE chat_id=? AND ide_id=? AND source='ide'",
                    (primary, cid)).fetchone()
                if not cur:
                    continue
                real = ide_title(cid)
                if real and not _junk_session_name(real) and cur[0] != real:
                    # only sync when the stored title is still the fallback
                    # (no real title yet) so we never clobber an explicit rename
                    if not cur[1]:
                        db.execute("UPDATE sessions SET name=?, title=? WHERE chat_id=? AND ide_id=?",
                                   (real, real, primary, cid))
                        db.commit()
        rows = db.execute(
            "SELECT chat_id,name,ide_id,conv_id,last_telegram_send FROM sessions "
            "WHERE bridge=1 AND (source='ide' OR conv_id IS NOT NULL)"
        ).fetchall()
    except Exception:
        return
    for chat_id, name, ide_id, conv_id, last_tg in rows:
        conv_id = conv_id or ide_id
        if not conv_id:
            continue
        last_tg = last_tg or 0.0
        with _active_runs_lock:
            if str(chat_id) in _active_runs or chat_id in _active_runs:
                continue
        with _active_convs_lock:
            if conv_id in _active_convs:
                continue
        if time.time() - last_tg < 8.0:
            continue
        dbp = os.path.join(IDE_ROOT, "conversations", f"{conv_id}.db")
        if not os.path.isfile(dbp):
            continue
        try:
            c = sqlite3.connect(dbp)
            try:
                # First time we see this conv: seed cursor from the DURABLE
                # bridge_cursor table. On the very first ever run, seed to current
                # MAX so we only forward FUTURE activity (don't replay the backlog
                # into TG). After a restart, the persisted cursor resumes exactly
                # where we left off -- no skipped turns.
                if conv_id not in _bridge_seen:
                    row = db.execute("SELECT last_idx FROM bridge_cursor WHERE conv_id=?",
                                     (conv_id,)).fetchone()
                    _bridge_seen[conv_id] = row[0] if row else c.execute(
                        "SELECT COALESCE(MAX(idx),0) FROM steps").fetchone()[0]
                new = c.execute(
                    "SELECT idx,step_type,step_payload FROM steps "
                    "WHERE idx>? ORDER BY idx ASC", (_bridge_seen.get(conv_id, 0),)
                ).fetchall()
            finally:
                c.close()
        except Exception:
            continue
        if not new:
            continue
        # Forward EVERY answer turn. Consecutive st==15 rows are one answer
        # streaming as chunks -> collapse to its final (longest) form. Separate
        # answer turns each get their own message. No fragile time-gate: dedup
        # is deterministic via run_agy advancing _bridge_seen past TG turns.
        cursor = _bridge_seen.get(conv_id, 0)
        run = []
        def _flush(is_final=False):
            """Flush the accumulated st==15 run.
            If is_final is False (a tool call follows), any prose here is an
            intermediate thought/commentary: accumulate into _bridge_thoughts rather than
            fragmenting into multiple standalone chat bubbles.
            If is_final is True, this is the real answer turn: prepend any accumulated
            thoughts inside a native <blockquote expandable> and deliver the complete
            answer in one clean message (matching the IDE and OpenCode UX).
            """
            nonlocal cursor
            if not run:
                return False  # empty run (tool-step transition) is a no-op
            best = ""
            run_max = 0
            for idx, st, pl in run:
                t = _visible_text(pl)
                # filter out empty/noise; short real answers (OK, Done) still pass
                if t and len(t) > 1 and len(t) > len(best):
                    best = t
                run_max = max(run_max, idx)
            if best and not _is_title_frame(best) and best.strip() not in _TOOL_NAMES:
                if not is_final:
                    # Intermediate thought before a tool call: fold into thought buffer
                    _bridge_thoughts.setdefault(conv_id, []).append(best.strip())
                    cursor = max(cursor, run_max)
                    _pending_clear(conv_id, run_max)
                    _bridge_cursor(conv_id, cursor)
                    return False
                else:
                    # Final answer turn: combine accumulated thoughts in expandable blockquote
                    thoughts = _bridge_thoughts.pop(conv_id, [])
                    clean_thoughts = []
                    for th in thoughts:
                        th_s = th.strip()
                        if th_s and th_s not in clean_thoughts and th_s != best.strip() and th_s not in _TOOL_NAMES:
                            clean_thoughts.append(th_s)
                    if clean_thoughts:
                        thought_text = "\n\n".join(clean_thoughts).strip()
                        thought_block = f"<blockquote expandable>💭 <b>Thinking Process</b>\n{_h(_short(thought_text, 2500))}</blockquote>\n\n"
                        msg_html = thought_block + md_to_html(best)
                    else:
                        msg_html = md_to_html(best)
                    send_long(chat_id, msg_html)
                    # log assistant turns too, so /history shows both sides
                    log_turn(chat_id, name, "assistant", best)
                    cursor = max(cursor, run_max)
                    _pending_clear(conv_id, run_max)
                    _bridge_cursor(conv_id, cursor)
                    return False
            # Could not forward (empty or bare title-frame/tool name). Two causes:
            #  * a real IDE step-title label -> genuine noise; advance after it
            #    stays byte-identical for _PENDING_ADVANCE consecutive polls.
            #  * a mid-write fragment (text changes poll-to-poll) -> PARK the
            #    cursor below it so the next poll re-reads and delivers it.
            _bridge_cursor(conv_id, cursor)  # persist (unchanged) for restart
            if _pending_watch(conv_id, run_max, best):
                # confirmed noise after N identical polls -> skip it
                cursor = max(cursor, run_max)
                _pending_clear(conv_id, run_max)
                _bridge_cursor(conv_id, cursor)
                return False
            return True  # parked: caller must stop this batch
        for idx, st, pl in sorted(new, key=lambda r: r[0]):
            if st == 15:
                run.append((idx, st, pl))
            elif st == 14:
                # User prompt step in IDE: reset any leftover thoughts
                _bridge_thoughts.pop(conv_id, None)
                cursor = max(cursor, idx)
                _bridge_cursor(conv_id, cursor)
            else:
                # flush any pending assistant thought into buffer, then deliver tool
                if _flush(is_final=False):
                    run = []
                    break   # parked below a mid-write fragment; re-read next poll
                run = []
                try:
                    if st in (132, 101, 5, 21, 8, 17, 7):
                        _bridge_tool_step(chat_id, pl)
                except Exception as e:
                    # one malformed step must never kill the poll loop
                    print("bridge tool step error:", e, file=sys.stderr)
                cursor = max(cursor, idx)
                _bridge_cursor(conv_id, cursor)
        _flush(is_final=True)
        _bridge_seen[conv_id] = cursor

        # plan / task file changes
        for fname, (mtime, content) in _brain_files(conv_id).items():
            key = f"{conv_id}:{fname}"
            prev = _bridge_files.get(key)
            if prev is None or prev[0] != mtime:
                _bridge_files[key] = (mtime, content)
                if prev is not None:
                    label = "📋 <b>Plan</b>" if fname == "implementation_plan.md" else "📝 <b>Task</b>"
                    cap = f"{label} updated · <i>{_html_name(name)}</i>"
                    kb = None
                    if fname == "implementation_plan.md":
                        # One-tap "Proceed" — links this IDE thread into Telegram
                        # and tells agy to implement the plan now.
                        kb = {"inline_keyboard": [[
                            {"text": "✅ Proceed (implement plan)",
                             "callback_data": f"proceed:{conv_id}"}
                        ]]}
                    if len(content) <= 3600:
                        # Fits inline. Split into prose vs code/diff blocks so
                        # the plan reads as Telegram-HTML prose, while any
                        # embedded CODE / DIFF is shipped as a clean .txt
                        # attachment (opencode-style) instead of being escaped
                        # into &lt; &gt; junk inside a <pre>.
                        prose, code_blocks = _split_md_code(content)
                        send_long(chat_id, f"{cap}\n{md_to_html(prose)}", reply_markup=kb)
                        for i, blk in enumerate(code_blocks):
                            ext = "txt"
                            if blk.strip().startswith(("---", "+++", "diff", "Index:", "@@")):
                                ext = "diff"
                            _send_code_attachment(
                                chat_id, blk,
                                f"📎 {fname} · code block {i+1}",
                                f"{fname.rsplit('.',1)[0]}_block{i+1}.{ext}")
                    else:
                        # Too long to inline cleanly → send the whole file as a
                        # document attachment (lossless, no cut-off).
                        _send_doc(chat_id, content, fname, caption=cap, reply_markup=kb)


def _html_name(name):
    return _h(str(name)) if name else "session"


_bridge_seen = {}      # conv_id -> last step idx we forwarded (runtime cache)
_bridge_files = {}     # "<conv>:<file>" -> (mtime, content)
_bridge_pending = {}   # conv_id -> {idx: {"txt": str, "strikes": int}}
_PENDING_ADVANCE = 3   # a skipped title-frame that stays byte-identical this
                       # many consecutive polls is genuine noise -> skip it.


def _pending_watch(conv_id, idx, txt):
    """Record a step we could NOT forward (empty/title-frame). If the text is
    identical across consecutive polls it is almost certainly a real IDE title
    label -> return True to skip it. If the text CHANGES between polls it is a
    mid-write fragment of an answer still being assembled -> return False so
    we re-read it until it finalizes into something forwardable."""
    d = _bridge_pending.setdefault(conv_id, {})
    rec = d.get(idx)
    if rec is None or rec.get("txt") != txt:
        d[idx] = {"txt": txt, "strikes": 1}
        return False
    rec["strikes"] += 1
    return rec["strikes"] >= _PENDING_ADVANCE


def _pending_clear(conv_id, upto):
    """Forget watched steps up to and including idx `upto` (now delivered or
    confirmed noise) so the watchlist never holds stale entries."""
    d = _bridge_pending.get(conv_id)
    if d:
        for k in [k for k in d if k <= upto]:
            d.pop(k, None)


def _bridge_cursor(conv_id, new_idx=None):
    """Durable forwarder cursor. With no arg: return last forwarded idx for
    conv_id (default -1). With new_idx: persist it. Persisted so restarts
    don't skip IDE activity that happened while the bot was down."""
    if new_idx is None:
        if conv_id in _bridge_seen:
            return _bridge_seen[conv_id]
        row = db.execute("SELECT last_idx FROM bridge_cursor WHERE conv_id=?",
                         (conv_id,)).fetchone()
        v = row[0] if row else -1
        _bridge_seen[conv_id] = v
        return v
    _bridge_seen[conv_id] = new_idx
    db.execute("INSERT INTO bridge_cursor(conv_id,last_idx) VALUES(?,?) "
               "ON CONFLICT(conv_id) DO UPDATE SET last_idx=excluded.last_idx",
               (conv_id, new_idx))
    db.commit()


def bridge_loop():
    """Background thread: poll IDE sessions that have bridging enabled."""
    print("bridge: IDE→Telegram forwarder started", flush=True)
    while True:
        try:
            _bridge_once()
        except Exception as e:
            print("bridge error:", e, file=sys.stderr)
        time.sleep(BRIDGE_POLL)


if __name__ == "__main__":
    main()

