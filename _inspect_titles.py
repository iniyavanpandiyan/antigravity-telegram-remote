import sqlite3, os, glob
D='/home/fiipadmin/.gemini/antigravity-ide/conversations/'

def pb_get(payload, field):
    i, n = 0, len(payload)
    while i < n:
        tag = 0; shift = 0
        while i < n:
            b = payload[i]; i += 1
            tag |= (b & 0x7F) << shift; shift += 7
            if not (b & 0x80): break
        wt = tag & 7
        if wt == 0:
            while i < n and payload[i] & 0x80: i += 1
            i += 1
        elif wt == 1: i += 8
        elif wt == 5: i += 4
        elif wt == 2:
            ln = 0; shift = 0
            while i < n:
                b = payload[i]; i += 1
                ln |= (b & 0x7F) << shift; shift += 7
                if not (b & 0x80): break
            sub = payload[i:i+ln]; i += ln
            if (tag >> 3) == field: return sub
        else:
            return None
    return None

def field_at(payload, path):
    """path like [30,4] -> nested pb_get"""
    cur = payload
    for f in path:
        if cur is None: return None
        cur = pb_get(cur, f)
    return cur

# Look at all step_type=23 conversations, dump .30.4 and .30.5 (first 80 chars)
dbs = sorted(glob.glob(os.path.join(D, "*.db")))[:15]
for db in dbs:
    cid = os.path.basename(db)[:-3]
    try:
        con = sqlite3.connect(db)
        rows = list(con.execute("SELECT step_type, step_payload FROM steps WHERE step_type=23 LIMIT 1"))
        # also conversation_summaries
        summ = None
        try:
            s = con.execute("SELECT * FROM conversation_summaries LIMIT 1").fetchone()
            cols = [d[0] for d in con.execute("PRAGMA table_info(conversation_summaries)")]
            summ = (cols, s)
        except Exception:
            summ = "NO_TABLE"
        # title column if exists
        title_col = None
        try:
            t = con.execute("SELECT title FROM conversations LIMIT 1").fetchone()
            title_col = t
        except Exception:
            title_col = "no conversations table/title col"
    except Exception as e:
        continue
    if rows:
        st, pl = rows[0]
        f4 = field_at(pl, [30,4])
        f5 = field_at(pl, [30,5])
        try: t4 = f4.decode("utf-8")[:70] if f4 else None
        except: t4 = repr(f4)[:70]
        try: t5 = f5.decode("utf-8")[:70] if f5 else None
        except: t5 = repr(f5)[:70]
        print(f"{cid[:8]} | .30.4={t4!r} | .30.5={t5!r} | summ={str(summ)[:60]}")
    con.close()
