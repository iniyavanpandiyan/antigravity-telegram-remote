import bot, sqlite3, os, json, re

# Reproduce last_reasoning() and a FIXED version, compare on real session.
def OLD(conv_id):
    db=os.path.join(bot.USER_CONV_DIR,f"{conv_id}.db")
    con=sqlite3.connect(db)
    rows=con.execute("SELECT step_type,step_payload FROM steps WHERE step_type=15 ORDER BY idx DESC LIMIT 30").fetchall()
    con.close()
    for st,pl in rows:
        t=bot._visible_text(pl)
        if t and len(t)>=40: return t
    return None

def NEW(conv_id):
    db=os.path.join(bot.USER_CONV_DIR,f"{conv_id}.db")
    con=sqlite3.connect(db)
    rows=con.execute("SELECT idx,step_type,step_payload FROM steps WHERE step_type IN (15,22) ORDER BY idx DESC LIMIT 60").fetchall()
    con.close()
    for idx,st,pl in rows:
        raw=pl.decode("utf-8","replace")
        # reject pure json / artifact metadata
        s=raw.strip()
        if s.startswith("{") and ("ArtifactMetadata" in raw or "Summary" in raw or "\"toolAction\"" in raw):
            continue
        t=bot._visible_text(pl)
        if not t: continue
        t=t.strip()
        # reject clearly-structural / non-prose
        if len(t)<40: continue
        if t.startswith("sessionID") or t.startswith("Log:"): continue
        # require it to look like prose (has a space and a vowel-ish word)
        if " " not in t[:120]: continue
        if any(k in t[:60] for k in ("CRITICAL INSTRUCTION","toolAction","ArtifactMetadata")): continue
        return t
    return None

for cid,label in [("d815747f-53c2-4870-b2d6-dcb26faa9d74","ide-NexGen"),
                  ("1d3a5c49-012a-420e-8895-017225c5f0b1","Studio")]:
    print("="*60); print(label)
    o=OLD(cid); n=NEW(cid)
    print("OLD[:200]:", repr((o or "")[:200]))
    print("NEW[:200]:", repr((n or "")[:200]))
