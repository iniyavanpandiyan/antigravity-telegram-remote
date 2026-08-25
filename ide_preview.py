import sqlite3, re, os


def _clean(s):
    s = (s.replace('\\u0026', '&').replace('\\u003c', '<')
         .replace('\\u003e', '>').replace('\\u0027', "'").replace('\\n', '\n'))
    # protobuf blobs append binary framing after the readable run text;
    # cut at the first control char (incl. DEL / backspace) so we don't
    # capture terminal garbage.
    s = re.split(r'[\x00-\x1f\x7f]', s)[0]
    return s.strip()


def _derive_title(first_user):
    """Short subject derived from the conversation's FIRST user prompt."""
    if not first_user:
        return None
    t = re.sub(r'\s+', ' ', first_user).strip()
    # take the first sentence/phrase
    head = re.split(r'(?<=[.?!])\s', t)[0]
    return head[:42].strip() or None


def ide_preview(conv_id, n_user=4):
    """Return (title, ws, last_user_msgs, assistant_snippet) for an IDE session db.

    title          = short subject from the conversation's FIRST user prompt
    ws             = workspace path (from the file:// URI in the blob)
    last_user_msgs = the most recent N user prompts (newest first)
    assistant      = snippet of the last assistant prose run
    """
    HOME = os.path.expanduser("~")
    IDE_DIR = os.path.join(HOME, ".gemini", "antigravity-ide", "conversations")
    db = f"{IDE_DIR}/{conv_id}.db"
    if not os.path.exists(db):
        return None
    con = sqlite3.connect(db)
    cur = con.cursor()
    ws = "unknown"
    try:
        raw = open(db, 'rb').read()
        m = re.search(rb'file://[^\x00-\x1f\'"\\\\]+', raw)
        if m:
            ws = m.group(0).decode().replace('file://', '')
    except Exception:
        pass
    user_msgs, assistant = [], ""
    for st, payload in cur.execute(
            "SELECT step_type, step_payload FROM steps ORDER BY idx ASC").fetchall():
        if not payload:
            continue
        text = payload.decode('utf-8', 'replace')
        if st == 14:
            # user prompt run begins with '!'
            m = re.search(r'!(.{5,}?)(?:\n|$)', text)
            if m:
                u = _clean(m.group(1))
                if u:
                    user_msgs.append(u)
        elif st in (5, 23):
            runs = re.findall(r'[\x20-\x7e]{30,}', text)
            if runs:
                assistant = _clean(runs[-1])
    con.close()
    title = _derive_title(user_msgs[0]) if user_msgs else None
    last = user_msgs[-n_user:][::-1]  # newest first
    return (title, ws, last, assistant)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        print(ide_preview(sys.argv[1]))
