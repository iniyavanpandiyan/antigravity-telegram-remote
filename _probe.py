import bot, sqlite3, os

def probe(tid):
    p = os.path.join(bot.USER_CONV_DIR, f"{tid}.db")
    if not os.path.exists(p):
        print("MISSING", tid); return
    con = sqlite3.connect(p)
    print("="*70)
    print("SESSION:", tid)
    print("ide_title() ->", repr(bot.ide_title(tid)))
    print("last_reasoning()[:300] ->", repr((bot.last_reasoning(tid) or "")[:300]))
    tabs = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    if "steps" not in tabs:
        print("no steps table"); con.close(); return
    # show step types present, and for each, the FULL visible text of the newest one
    counts = con.execute("SELECT step_type,COUNT(*) FROM steps GROUP BY step_type ORDER BY step_type").fetchall()
    print("step_type counts:", counts)
    print("-"*70)
    print("FULL newest-visible text per step_type (idx desc):")
    for st,_n in counts:
        rows = con.execute("SELECT idx,step_payload FROM steps WHERE step_type=? ORDER BY idx DESC LIMIT 1",(st,)).fetchall()
        if not rows: continue
        idx,pl = rows[0]
        v = bot._visible_text(pl)
        if v:
            print("\n[st=%s idx=%s] len=%d" % (st, idx, len(v)))
            print("   " + v[:500].replace("\n","\n   "))
        else:
            print("[st=%s idx=%s] (no visible_text)" % (st, idx))
    con.close()

probe("d815747f-53c2-4870-b2d6-dcb26faa9d74")  # ide-NexGen new
probe("1d3a5c49-012a-420e-8895-017225c5f0b1")  # Studio
