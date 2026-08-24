import os, glob, sqlite3
# find sqlite dbs that hold a 'sessions' table
cands = []
for root in [".", os.path.expanduser("~/.config"), os.path.expanduser("~/.local")]:
    for f in glob.glob(root.rstrip("/")+"/**/*.db", recursive=True):
        cands.append(f)
# also known likely spots
for f in ["sessions.db","agy.db","bot.db"]:
    if os.path.exists(f): cands.append(f)
seen=set()
for f in cands:
    if f in seen: continue
    seen.add(f)
    try:
        con=sqlite3.connect(f)
        tabs=[r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        if "sessions" in tabs:
            print("DB:",f)
            print("  tables:",tabs)
            for t in tabs:
                try:
                    n=con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    print(f"    {t}: {n}")
                except Exception as e:
                    print(f"    {t}: ERR {e}")
        con.close()
    except Exception as e:
        pass
