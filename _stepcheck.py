import sqlite3, os, collections
D='/home/fiipadmin/.gemini/antigravity-ide/conversations/'
cid='1d3a5c49-012a-420e-8895-017225c5f0b1'
db=os.path.join(D, f"{cid}.db")
con=sqlite3.connect(db)
# distribution of step_type
dist=collections.Counter(r[0] for r in con.execute("SELECT step_type FROM steps"))
print("step_type distribution:", dict(sorted(dist.items())))
# for each type, show first 1 sample of how text looks
seen=set()
for st, pl in con.execute("SELECT step_type, step_payload FROM steps ORDER BY idx DESC LIMIT 400"):
    if st in seen or not pl: continue
    seen.add(st)
    # crude text extract
    try:
        t=pl.decode('utf-8','ignore')
    except: t=''
    # printable chunk only
    import re
    chunks=re.findall(r'[\x20-\x7e]{20,}', t)
    sample=chunks[0][:90] if chunks else '(binary)'
    print(f"  type {st}: sample={sample!r}")
# Now specifically: which type carries the LATEST assistant answer?
print("---- last 25 steps ----")
for idx, st, pl in con.execute("SELECT idx,step_type,step_payload FROM steps ORDER BY idx DESC LIMIT 25"):
    try: t=pl.decode('utf-8','ignore')
    except: t=''
    import re
    chunks=re.findall(r'[\x20-\x7e]{15,}', t)
    sample=chunks[0][:70] if chunks else '(binary)'
    print(f"  idx={idx} type={st} {sample!r}")
con.close()
