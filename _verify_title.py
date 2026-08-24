import sqlite3, os
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

def ide_title(cid):
    db = os.path.join(D, f"{cid}.db")
    if not os.path.exists(db): return None
    con = sqlite3.connect(db)
    for st, pl in con.execute("SELECT step_type, step_payload FROM steps WHERE step_type=23"):
        if not pl: continue
        meta = pb_get(pl, 30)
        if meta is None: continue
        tb = pb_get(meta, 4)
        if tb:
            try: t = tb.decode("utf-8").strip()
            except Exception: t = ""
            if t and t != "sessionID":
                con.close(); return t
    con.close(); return None

for cid in ["1d3a5c49-012a-420e-8895-017225c5f0b1","91486e35-6527-4ad7-b7a9-f0850aa1e900"]:
    print(cid[:8], "->", repr(ide_title(cid)))
