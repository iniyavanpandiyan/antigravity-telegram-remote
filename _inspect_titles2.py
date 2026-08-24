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

dbs = sorted(glob.glob(os.path.join(D, "*.db")))
print("total dbs:", len(dbs))
checked = 0
for db in dbs[:15]:
    cid = os.path.basename(db)[:-3]
    try:
        con = sqlite3.connect(db)
        n23 = con.execute("SELECT COUNT(*) FROM steps WHERE step_type=23").fetchone()[0]
        has_summ = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='conversation_summaries'").fetchone()
        title_col = con.execute("PRAGMA table_info(conversations)").fetchall()
        cols = [c[1] for c in title_col]
        print(f"{cid[:8]} step23={n23} summ_table={bool(has_summ)} conv_cols={cols}")
        checked += 1
    except Exception as e:
        print(f"{cid[:8]} ERR {e}")
    con.close()
print("checked:", checked)
