import sqlite3, os
D='/home/fiipadmin/.gemini/antigravity-ide/conversations/'

def parse(payload, depth=0, path=""):
    """Full recursive protobuf walk. Yields (path, field_num, kind, value_or_None)."""
    i, n = 0, len(payload)
    out = []
    while i < n:
        tag = 0; shift = 0
        while i < n:
            b = payload[i]; i += 1
            tag |= (b & 0x7F) << shift; shift += 7
            if not (b & 0x80): break
        fnum = tag >> 3
        wt = tag & 7
        if wt == 0:  # varint
            v = 0; s = 0
            while i < n:
                b = payload[i]; i += 1
                v |= (b & 0x7F) << s; s += 7
                if not (b & 0x80): break
            out.append((path, fnum, "varint", v))
        elif wt == 1:
            out.append((path, fnum, "64bit", None)); i += 8
        elif wt == 5:
            out.append((path, fnum, "32bit", None)); i += 4
        elif wt == 2:
            ln = 0; s = 0
            while i < n:
                b = payload[i]; i += 1
                ln |= (b & 0x7F) << s; s += 7
                if not (b & 0x80): break
            sub = payload[i:i+ln]; i += ln
            out.append((path, fnum, "bytes", sub))
            # peek: is it text or nested?
            try:
                t = sub.decode("utf-8")
                is_text = True
            except Exception:
                is_text = False
            if is_text:
                out.append((path + f".{fnum}", fnum, "text", t[:200]))
            else:
                # recurse
                for r in parse(sub, depth+1, path + f".{fnum}"):
                    out.append(r)
        else:
            out.append((path, fnum, "BAD", None)); break
    return out

for cid in ["1d3a5c49-012a-420e-8895-017225c5f0b1","91486e35-6527-4ad7-b7a9-f0850aa1e900"]:
    db = os.path.join(D, f"{cid}.db")
    con = sqlite3.connect(db)
    print("="*70)
    print("CID", cid[:8])
    for st, pl in con.execute("SELECT step_type, step_payload FROM steps WHERE step_type=23"):
        rows = parse(pl)
        # show all the .30 subtree text/varint/bytes-len
        for p, fn, kind, val in rows:
            if p.startswith(".30") and kind in ("text","varint"):
                if kind == "text" and len(val) < 120:
                    print(f"  {p} [{kind}] = {val!r}")
        print("  --- .30 subfields present:", sorted({p for p,fn,k,v in rows if p.startswith('.30') and k!='bytes'}))
    con.close()
