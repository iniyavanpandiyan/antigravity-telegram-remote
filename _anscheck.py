import sqlite3, os, re, json
IDE='/home/fiipadmin/.gemini/antigravity-ide/conversations/'
BOT='/home/fiipadmin/.antigravity-telegram-remote/antigravity.db'
cid='1d3a5c49-012a-420e-8895-017225c5f0b1'

# 1) replicate _visible_text from bot.py
def _visible_text(payload):
    if not payload: return None
    runs = re.findall(rb"[\x20-\x7e]{16,}", payload)
    best=None
    for r in runs:
        s=r.decode("utf-8","replace")
        if s.startswith(("b$","file://","http","/","{")): continue
        if s.count("-")>5 or s.startswith(("sessionID","call_")): continue
        s=s.strip().strip('"').strip()
        if not re.search(r"[a-z]{3,}", s): continue
        if best is None or len(s)>len(best): best=s
    return best

# find a real type-15 answer step near the end of the conversation
con=sqlite3.connect(os.path.join(IDE, f"{cid}.db"))
cands=con.execute("SELECT idx,step_payload FROM steps WHERE step_type=15 ORDER BY idx DESC LIMIT 25").fetchall()
print("=== type-15 answer extraction test (Studio Page) ===")
hit=0
for idx,pl in cands:
    t=_visible_text(pl)
    if t and len(t)>40:
        hit+=1
        print(f"  idx={idx} ANSWER-LIKE ({len(t)} chars): {t[:90]!r}")
        if hit>=4: break
if not hit:
    print("  !! No answer-like text recovered from type-15 steps -> answers would be dropped by bridge")
con.close()

# 2) is Studio Page armed in bot DB with bridge=1?
b=sqlite3.connect(BOT)
row=b.execute("SELECT name,conv_id,ide_id,source,bridge FROM sessions WHERE ide_id=?",(cid,)).fetchone()
print("=== bot DB session row for Studio Page ===")
print(" ", row)
b.close()
