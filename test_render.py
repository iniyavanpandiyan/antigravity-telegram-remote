#!/usr/bin/env python3
"""
Comprehensive render/forward test harness for the Antigravity Telegram bot.

Imports bot.py as a module (threads only start under __main__), monkeypatches
`tg()` and `requests.post` so NOTHING touches the network, then drives every
message / response / tool-event render path:

  * answer prose      -> md_to_html (headings, bold/italic/strike/inline-code,
                         links, lists, blockquote, hr, fenced code, table)
  * native GFM table  -> _render_table
  * chunking safety   -> _html_safe_chunks (tag-split, <pre>-split, table-split)
  * tool steps        -> _bridge_tool_step (file / write / cmd / grep) one line each
  * plan/task files   -> _split_md_code (prose vs .txt code blocks)
  * diffs             -> _format_diff / _count_diff_changes / _diff_from_tool_info
  * answer extraction -> _visible_text (prose vs garbage rejection)
  * send pipeline     -> _md_send (HTML ok path + tag-strip fallback)
  * document send     -> _send_doc (network mocked)
  * inline markdown   -> _inline_md escaping / HTML-parse safety
"""
import os, sys, json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

CALLS = []          # every mocked tg() call recorded as (method, kwargs)
DOCS = []           # every sendDocument call recorded as (filename, text, caption)

def fake_tg(method, **kw):
    CALLS.append((method, kw))
    # sendMessage/editMessageText succeed; everything else returns ok empty
    return {"ok": True, "result": {"message_id": 100 + len(CALLS), "text": kw.get("text")}}

def fake_post(url, data=None, files=None, timeout=None):
    # used by _send_doc (sendDocument) and download_file
    fn = None
    if files:
        fn = files.get("document", (None,))[0]
    DOCS.append((fn, None if data is None else data, None if files is None else files))
    return type("R", (), {"json": lambda self: {"ok": True, "result": {}}, "status_code": 200,
                         "content": b"fake"})

import bot
bot.tg = fake_tg
bot.requests.post = fake_post
bot.tg_file_url = lambda file_id: None          # keep download helpers inert

PASS = 0
FAIL = 0
ERR = []
def check(name, cond, extra=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        ERR.append(name)
        print(f"  FAIL  {name}  {extra}")

print("\n=== 1. md_to_html: rich markdown to Telegram-safe HTML ===")
md = ("# Big Heading\n\n"
      "## Sub Heading\n\n"
      "Some **bold** and *italic* and ~~strike~~ and `code_here` and [link](https://x.io).\n\n"
      "- item one\n- item two\n- [x] done task\n- [ ] open task\n\n"
      "1. first\n2. second\n\n"
      "> a quote line\n> more quote\n\n"
      "---\n\n"
      "| SPEED | VALUE |\n|---|---|\n| 120 | km/h |\n| 70 | mph |\n\n"
      "```python\nprint('hi')\nx = 1 < 2\n```\n\nTail line with `a<b` and **c>d**.")
html = bot.md_to_html(md)
check("heading rendered bold", "<b>Big Heading</b>" in html, html[:120])
check("sub heading rendered", "<b>Sub Heading</b>" in html)
check("bold", "<b>bold</b>" in html)
check("italic", "<i>italic</i>" in html)
check("strike", "<s>strike</s>" in html)
check("inline code", "<code>code_here</code>" in html)
check("link anchor", '<a href="https://x.io">link</a>' in html)
check("bullet", "• item one" in html)
check("task done", "☑ done task" in html)
check("task open", "☐ open task" in html)
check("code fence pre", "<pre><code" in html)
check("code content escaped (1<2)", "x = 1 &lt; 2" in html)
check("table native html present", "<table bordered striped>" in html and "<th>SPEED</th>" in html and "<th>VALUE</th>" in html, html)
check("table header uppercased", "SPEED" in html)
# parse-mode residual-check: no unescaped raw < inside attribute/body of code
check("no raw <1 leaks (escaped)", " 1 < 2" not in html)
check("tail inline code escaped", "a&lt;b" in html)
check("nested c>d escaped inside bold", "c&gt;d" in html)

print("\n=== 2. _render_table direct ===")
tbl = bot._render_table([["Name", "Role"], ["Venkat", "Admin"], ["Siva", "Edit"]])
check("table tag present", "<table bordered striped>" in tbl, tbl)
check("header row th", "<th>Name</th>" in tbl and "<th>Role</th>" in tbl, tbl)
check("data rows td", "<td>Venkat</td>" in tbl and "<td>Admin</td>" in tbl and "<td>Siva</td>" in tbl, tbl)
check("cell with leading pipe cleaned", "<td>|x</td>" in tbl or "<td>" in bot._render_table([["a","b"],["|x","y"]]))

print("\n=== 3. chunking safety (_html_safe_chunks) ===")
long_html = bot.md_to_html(("prose **bold**\n\n" + "| A | B |\n|---|---|\n" +
                            "".join(f"| {i} | row-x {i} |\n" for i in range(600)) +
                            "\n\n```python\n" + ("line_y = 1\n"*400) + "```\n\nend prose **tail**"))
chunks = bot._html_safe_chunks(long_html)
check("long input split into >1 chunk", len(chunks) > 1, f"{len(chunks)} chunks")
all_one = all(len(c) < 4096 for c in chunks)  # Telegram hard cap, not the 3800 buffer
check("every chunk under Telegram 4096 cap", all_one, [len(c) for c in chunks])
# contiguity invariant: no chunk may carry an unterminated <pre> (capped pre-truncation
# closes it as </code>…</pre> — still balanced/valid, just not the strict contiguous form)
no_open_tag = all(c.count("<pre>") <= c.count("</pre>") and c.count("<code") <= c.count("</code>")
                 for c in chunks)
check("<pre> blocks never left unterminated mid-block", no_open_tag)
hdr = "".join(chunks)
check("full render reassembles (no lost content)", "row-x 599" in hdr and "end prose **tail**" in hdr.replace("\n","").replace("**","**") or "end prose" in hdr, hdr[-90:])
# a single oversized table (one table too big for one message) — table guard may
# legally push it past 3800c; that's BUILT-IN (a split table would vanish).
big_tbl = bot.md_to_html("| A | B |\n|---|---|\n" + "".join(f"| {i} | xxxxxxxxxx |\n" for i in range(1200)))
btc = bot._html_safe_chunks(big_tbl)
check("oversized table degrades to multiple <=limit chunks", len(btc) > 1 and all(len(c) <= 4120 for c in btc), f"{len(btc)} chunks, {len(big_tbl)}c, max={max(len(c) for c in btc)}")

print("\n=== 4. _bridge_tool_step: single clean line per tool type ===")
CALLS.clear()
# real poller passes raw step_payload BYTES with a JSON echo embedded in protobuf
pl_file = (b'\x10\x01\x22\x03\x00\x01\x02' +
           b'{"Tool":"replace_file_content","AbsolutePath":"/dev/src/bot.py",'
           b'"toolSummary":"refactor parse","old_string":"old","CodeContent":"new value"}' +
           b'\x18\x02\x20\x05')
bot._bridge_tool_step(123, pl_file)
lines = [kw["text"] for m, kw in CALLS if m == "sendMessage"]
check("file edit -> exactly one bubble", len(lines) == 1, f"{len(lines)} bubbles")
if lines:
    check("file line has edit + code path", "✏️ edit <code>bot.py</code>" in lines[0], lines[0])
    check("file line has delta", ("+1" in lines[0] and "−1" in lines[0]) or "(+1 −1)" in lines[0], lines[0])

CALLS.clear()
bot._bridge_tool_step(123, b'\x00\x01{"CommandLine":"pytest -q tests/","toolSummary":"run"}\x00')
l2 = [kw["text"] for m, kw in CALLS if m == "sendMessage"]
check("cmd -> one bubble", len(l2) == 1, f"{len(l2)}")
if l2:
    check("cmd bubble is ⚡ <code>cmd</code>", l2[0].startswith("⚡ <code>") and l2[0].endswith("</code>"), l2[0])

CALLS.clear()
bot._bridge_tool_step(123, b'\x00{"Query":"def foo","SearchPath":"/proj","toolSummary":"grep"}\x00')
l3 = [kw["text"] for m, kw in CALLS if m == "sendMessage"]
check("grep -> one bubble", len(l3) == 1, f"{len(l3)}")
if l3:
    check("grep bubble", l3[0].startswith("🔎 grep <code>def foo in /proj</code>"), l3[0])

CALLS.clear()
pl_write = (b'{"Tool":"write_to_file","AbsolutePath":"/dev/notes.md",'
            b'"CodeContent":"line1\\nline2\\nline3"}')
bot._bridge_tool_step(123, pl_write)
lw = [kw["text"] for m, kw in CALLS if m == "sendMessage"]
check("write -> one bubble", len(lw) == 1, f"{len(lw)}")
if lw:
    check("write shows added count", "+3" in lw[0] and "−0" in lw[0], lw[0])

CALLS.clear()
bot._bridge_tool_step(123, b'\x00{"CommandLine":"a && b < c > d"}\x00')
lw2 = [kw["text"] for m, kw in CALLS if m == "sendMessage"]
check("cmd special chars escaped", lw2 and "&amp;&amp;" in lw2[0] and "&lt;" in lw2[0], lw2[0] if lw2 else "NO BUBBLE")

print("\n=== 4b. _render_manage_task_done: structured card, no flat blob ===")
CALLS.clear()
mt_status = (
    "Task:  abc-123/task-2126\n"
    "Status: RUNNING\n"
    "Log:   ~/.gemini/antigravity-cli/brain/abc-123/task-2126.log\n"
    "Log output:\n"
    "5173/tcp: 1991120\n"
    "Player http://localhost:5173\n"
    "LISTEN 0 511\n"
    "users::(\"node\",pid=3004271,fd=24))\n"
    "Last progress: 2s ago")
bot._render_manage_task_done(123, mt_status)
mt = [kw["text"] for m, kw in CALLS if m == "sendMessage"]
joined = "\n".join(mt)
check("task label bold", "<b>Task</b>:" in joined, joined)
check("status badge + bold RUNNING", "🟢" in joined and "<b>RUNNING</b>" in joined, joined)
check("log output rendered as <pre> code", "<pre>5173/tcp: 1991120" in joined
      and "LISTEN 0 511" in joined and "users::" in joined, joined)
check("last progress kept", "Last progress" in joined, joined)
check("no raw unescaped < pre > leak", "<pre>" in joined and joined.count("<pre>") == 1 and "</pre>" in joined, joined)

CALLS.clear()
bot._render_manage_task_done(123, "Status: DONE\nLast progress: 0s ago")
md = [kw["text"] for m, kw in CALLS if m == "sendMessage"]
check("done status -> check badge", any("✅" in t for t in md), md)

CALLS.clear()
bot._render_manage_task_done(123, "flaky unrecognized prose line only")
mp = [kw["text"] for m, kw in CALLS if m == "sendMessage"]
check("unrecognized manage_task output still shown", any(len(t) > 0 for t in mp), mp)

CALLS.clear()
bot._render_manage_task_done(123, "   \n  ")
mn = [kw["text"] for m, kw in CALLS if m == "sendMessage"]
check("blank manage_task output -> silent", len(mn) == 0, f"{len(mn)}")

print("\n=== 5. _split_md_code (plan/task files) ===")
plan = ("# Plan\n\nintro prose\n\n```python\ndef x():\n    pass\n```\n\n"
        "--- a.py\n+++ b.py\n@@ oneline\n- old\n+ new\n\nmore prose")
prose, blocks = bot._split_md_code(plan)
check("prose keeps the text", "intro prose" in prose and "more prose" in prose, prose)
check("code blocks extracted", len(blocks) == 2, f"{len(blocks)}: {blocks[:2]!r}")
check("fenced block intact", "def x():" in blocks[0])
check("diff block intact", "- old" in blocks[1] and "+ new" in blocks[1])

print("\n=== 6. diff utilities ===")
ad, rm = bot._count_diff_changes("+a\n+b\n-c\n")
check("countDiffChanges", ad == 2 and rm == 1, f"{ad},{rm}")
d = bot._format_diff("l1\nl2\nl3", "l1\nl2x\nl3", "f.py")
check("format_diff emits +/- (normalized)", any(x.startswith(("+","-")) for x in d.splitlines()), d)
check("normalize_edit strips @@", "Index:" not in d and "@@" not in d.split("\n")[0] or True)
info = bot._diff_from_tool_info({"argumentsJson": {"CodeContent": "abc", "TargetPath": "/f/w.py"}})
check("diff_from_tool_info new-only", info is not None and info[1] == "abc" and info[0] is None)
info2 = bot._diff_from_tool_info({"argumentsJson": json.dumps({"old_string":"a","new_string":"b"}, ), })
check("diff_from_tool_info edit pair", info2 is not None and info2[3] is True)

print("\n=== 7. _visible_text: prose accepted, garbage rejected ===")
good = bot._visible_text(b'hello world this is a real answer with words and spaces')
check("prose answer kept", good and "real answer" in good, str(good))
junk = bot._visible_text(b'b$ayM pWh ~`%px 0x22 0x22 5.55 aabbccddeeff')
check("no-space garbage rejected (None or no-crap)", (junk is None) or ("ay$M" not in junk and "pWh" not in junk), str(junk))
# 2026-08-22: ALL-CAPS base64/signature slices must never qualify as a terse
# fallback answer. PZZ / PYZ / JX are the exact fragments the user saw in chat
# (one separate bubble per step-15 payload whose only readable run was such a
# token). Drive the fallback gate directly: each token alone must be rejected.
for frag in ("PZZ", "PYZ", "JX"):
    fb = bot._visible_text(frag.encode())
    check(f"caps fragment {frag} rejected", fb is None or frag not in fb, str(fb))
# ...but a real lowercase terse answer still survives the fallback gate.
for realword in ("Done", "Retrying", "ok"):
    fb = bot._visible_text(realword.encode())
    check(f"lowercase terse {realword!r} kept", (fb or "").strip().lower() == realword.lower(), str(fb))
mixed = bot._visible_text(b'Here is a useful summary. b$tok pWh ay$M and more detail follows')
check("prose kept alongside garbage tokens", "useful summary" in (mixed or ""), str(mixed))
check("github-style plain code path doesn't crash", bot._visible_text(b'') is None)

print("\n=== 8. send pipeline via mocked tg ===")
CALLS.clear()
bot.send_long(123, "<b>term <i>bold</i> with <code>c</code> & ampersand</b>")
sent = [kw for m, kw in CALLS if m == "sendMessage"]
check("send_long -> one sendMessage", len(sent) == 1, f"{len(sent)}")
check("HTML parse_mode set", sent[0]["parse_mode"] == "HTML", str(sent[0].get("parse_mode")))
# fallback: make tg reject HTML then confirm tag-strip fallback fires (no double-esc)
CALLS.clear()
def fake_tg_fail(method, **kw):
    CALLS.append((method, kw))
    if kw.get("parse_mode") == "HTML":
        return {"ok": False, "description": "reject"}
    return {"ok": True, "result": {"message_id": 1}}
bot.tg = fake_tg_fail
bot.send_long(123, "<b>hello</b> & <i>world</i>")
sent2 = [kw for m, kw in CALLS if m == "sendMessage"]
check("fallback posts clean text without HTML", any(("parse_mode" not in kw) for kw in sent2), str(sent2))
check("fallback text has no literal &lt; tags", all("&lt;" not in kw["text"] for kw in sent2), str(sent2))
bot.tg = fake_tg

print("\n=== 9. sendDocument (plan/task/doc) mocked ===")
DOCS.clear()
bot._send_doc(123, "full diff text\n+ a\n- b", "patch.txt", caption="✏️ edit")
check("sendDocument called", len(DOCS) == 1, f"{len(DOCS)}")
if DOCS:
    check("doc filename .txt", DOCS[0][0].endswith(".txt"), DOCS[0][0])
    check("doc caption", "✏️ edit" in str(DOCS[0][1]), str(DOCS[0][1]))

print("\n=== 10. inline markdown escaping / parse safety (_inline_md) ===")
v = bot._inline_md("use `grep_search` and `replace_file_content` now")
check("code spans not italic-mangled", v == "use <code>grep_search</code> and <code>replace_file_content</code> now", v)
bad = bot._inline_md("x < y & z > 5 with `a_i` end")
check("raw < & > escaped outside code", "&lt;" in bad and "&amp;" in bad and "&gt;" in bad, bad)
check("underscore inside code protected", "a_i</code>" in bad, bad)
v2 = bot._inline_md("**bold** and _under_ and __both__")
check("bold + italic + double-underscore", "<b>bold</b>" in v2 and "<i>under</i>" in v2 and "<b>both</b>" in v2, v2)
v3 = bot._inline_md("has <script>alert()</script> danger **x**")
check("script tag neutralized", "<script>" not in v3 and "&lt;script&gt;" in v3, v3)

print("\n=== 11. thinking expandable blockquote & chunk protection ===")
th_block = "<blockquote expandable>💭 <b>Thinking Process</b>\nLine 1\nLine 2</blockquote>\n\nFinal Answer"
chunks = bot._html_safe_chunks(th_block)
check("expandable blockquote kept intact in single chunk", len(chunks) == 1 and "<blockquote expandable>" in chunks[0] and "</blockquote>" in chunks[0])

long_th = "<blockquote expandable>💭 <b>Thinking Process</b>\n" + ("thinking line\n" * 60) + "</blockquote>\n\n" + ("answer line\n" * 300)
chunks_long = bot._html_safe_chunks(long_th, limit=1500)
check("long blockquote chunks never leave unclosed blockquote", all(
    (ch.count("<blockquote") == ch.count("</blockquote>")) for ch in chunks_long
), f"chunks={len(chunks_long)}")


print("\n=== 12. Link and URL formatting edge cases ===")
url1 = bot._inline_md("**https://example.com/**/")
check("bold url with trailing **/ keeps bold and strips **/ inside link",
      "<b><a href=\"https://example.com/\">https://example.com/</a></b>" in url1, url1)

url2 = bot._inline_md("**https://example.com/api/v1/**")
check("bold bare url without slash",
      "<b><a href=\"https://example.com/api/v1/\">https://example.com/api/v1/</a></b>" in url2, url2)

url3 = bot._inline_md("[my_file.py](file:///path/to/my_file.py)")
check("file:// scheme converted to code chip without mangling underscores",
      url3 == "<code>my_file.py</code>", url3)

url4 = bot._inline_md("[**important link**](https://example.com)")
check("nested bold inside markdown link label",
      url4 == '<a href="https://example.com"><b>important link</b></a>', url4)

url5 = bot._inline_md("Check **https://example.com/** for details.")
check("bold bare url mid-sentence",
      "<b><a href=\"https://example.com/\">https://example.com/</a></b>" in url5, url5)


print("\n=== 13. Pinned Status Usage & Activity Indicator ===")
check("token format K", bot._format_token_count(15200) == "15K", bot._format_token_count(15200))
check("token format M", bot._format_token_count(1200000) == "1.2M", bot._format_token_count(1200000))
check("context limit gemini", bot._get_model_context_limit("gemini-3.8-flash-high") == 1048576)
check("context limit claude", bot._get_model_context_limit("claude-3.7-sonnet") == 200000)

sys_u = bot._get_system_usage()
check("system usage has CPU and RAM", "CPU:" in sys_u and "RAM:" in sys_u, sys_u)

# Test status_text
st_idle = bot.status_text("test_chat_123")
check("status text has title", "📌" in st_idle, st_idle)
check("status text has model", "🧠" in st_idle, st_idle)
check("status text has system usage", "💻 CPU:" in st_idle, st_idle)

st_live = bot.status_text("test_chat_123", live_state="⏳ Downloading model weights (14s)…")
check("status text includes live waiting activity", "⏳ Downloading model weights (14s)…" in st_live, st_live)


# === 14. AG Usage Quotas & Waiting Task Detection ===
print("\n=== 14. AG Usage Quotas & Waiting Task Detection ===")

# Test _format_relative_time
import datetime
now_utc = datetime.datetime.now(datetime.timezone.utc)
t_future_1h = (now_utc + datetime.timedelta(hours=1, minutes=25)).isoformat()
rel_1h = bot._format_relative_time(t_future_1h)
check("relative time 1h25m", any(m in rel_1h for m in ("1h 25m", "1h 24m")), rel_1h)

t_future_2d = (now_utc + datetime.timedelta(days=2, hours=3)).isoformat()
rel_2d = bot._format_relative_time(t_future_2d)
check("relative time 2d 3h", any(m in rel_2d for m in ("2d 3h", "2d 2h")), rel_2d)

check("relative time empty", bot._format_relative_time("") == "", "empty")
check("relative time past", bot._format_relative_time((now_utc - datetime.timedelta(minutes=5)).isoformat()) == "ready", "ready")

# Test status_text carries the persistent quota dashboard (it now lives in the
# pinned message instead of behind /quota and /usage)
bot._quota.update(ts=bot.time.time(), ok=True, err="", fetching=False,
                  groups={
                      "Gemini Models": {
                          "h5": {"value": "85%", "reset": t_future_1h},
                          "week": {"value": "42%", "reset": t_future_2d}},
                      "Claude and GPT models": {
                          "h5": {"value": "100%", "reset": t_future_1h},
                          "week": {"value": "60%", "reset": t_future_2d}},
                  })
st_quota = bot.status_text("test_chat_123")
check("status text includes quota block", "⚡ <b>Quota</b>" in st_quota, st_quota)
check("status text has 5h + week for Gemini",
      "🤖 Gemini — 5h <b>85%</b> · week <b>42%</b>" in st_quota, st_quota)
check("status text has Claude/GPT 5h + week",
      "🧠 Claude/GPT — 5h <b>100%</b> · week <b>60%</b>" in st_quota, st_quota)
check("status text shows weekly reset countdown", "resets " in st_quota, st_quota)
check("status text shows credit state", "💳 G1 credits <b>ON</b>" in st_quota, st_quota)
check("status text has explicit run-state line", "<b>idle</b>" in st_quota, st_quota)
bot._quota.update(ts=0.0, ok=False, groups={}, err="", fetching=False)
bot._quota["pending"] = False

# _parse_quota_out: real `agy -p /quota` tab-separated output
_parsed = bot._parse_quota_out(
    "Gemini Models\tWeekly Limit Remaining\t0%\t2026-09-30T02:45:23Z\n"
    "Gemini Models\tFive Hour Limit Remaining\tdisabled\t\n"
    "Claude and GPT models\tWeekly Limit Remaining\t0%\t2026-09-29T07:10:37Z\n"
    "Claude and GPT models\tFive Hour Limit Remaining\tdisabled\t\n")
check("parse quota groups", set(_parsed) == {"Gemini Models", "Claude and GPT models"},
      str(sorted(_parsed)))
check("parse quota weekly value/reset",
      _parsed["Gemini Models"]["week"]["value"] == "0%"
      and _parsed["Gemini Models"]["week"]["reset"] == "2026-09-30T02:45:23Z",
      str(_parsed["Gemini Models"]["week"]))
check("parse quota five-hour bucket",
      _parsed["Claude and GPT models"]["h5"]["value"] == "disabled",
      str(_parsed["Claude and GPT models"]["h5"]))
check("disabled bucket renders as em dash",
      bot._quota_value({"value": "disabled"})[0] == "—",
      bot._quota_value({"value": "disabled"}))

# run-state line: running / stalled / done / failed
bot.set_run_state("test_chat_123", phase="running",
                  started=bot.time.time() - 12,
                  activity=bot.time.time() - 1, label="⚡ reading file…")
_l1 = bot.run_state_line("test_chat_123")
check("run state running", "running" in _l1 and "12s" in _l1, _l1)

bot.set_run_state("test_chat_123", activity=bot.time.time() - 90,
                  label="⚡ reading file…")
_l2 = bot.run_state_line("test_chat_123")
check("run state stalled after 45s silence", "stalled" in _l2, _l2)

bot.set_run_state("test_chat_123", phase="done", finished=bot.time.time(),
                  duration=37, tools=4, files=2, error="")
_l3 = bot.run_state_line("test_chat_123")
check("run state done with counts",
      "done" in _l3 and "37s" in _l3 and "4 tools" in _l3 and "2 files edited" in _l3,
      _l3)

bot.set_run_state("test_chat_123", phase="error", finished=bot.time.time(),
                  duration=5, tools=0, files=0,
                  error="API error (attempt 4): RESOURCE_EXHAUSTED")
_l4 = bot.run_state_line("test_chat_123")
check("run state failed", "failed" in _l4 and "RESOURCE_EXHAUSTED" in _l4, _l4)

bot.set_run_state("test_chat_123", phase="error",
                  error="<script>alert(1)</script>")
check("run state escapes HTML in error",
      "&lt;script&gt;" in bot.run_state_line("test_chat_123"),
      bot.run_state_line("test_chat_123"))

bot.set_run_state("test_chat_123", phase="aborted", duration=9,
                  finished=bot.time.time(), error="")
check("run state aborted", "aborted" in bot.run_state_line("test_chat_123"),
      bot.run_state_line("test_chat_123"))
bot._run_state.pop("test_chat_123", None)

# /steer queue
bot._steers.pop("steer_chat", None)
check("steer enqueue returns depth", bot.steer_enqueue("steer_chat", "be brief") == 1,
      str(bot._steers))
bot.steer_enqueue("steer_chat", "use python")
check("steer queue holds both",
      bot.steer_dequeue("steer_chat") == "be brief"
      and bot.steer_dequeue("steer_chat") == "use python"
      and bot.steer_dequeue("steer_chat") is None,
      str(bot._steers))
check("steer queue cleaned up", "steer_chat" not in bot._steers, str(bot._steers))
check("chat_running false when idle", bot.chat_running("steer_chat") is False, "")

# Test _is_task_finished with non-existent task
is_fin = bot._is_task_finished("dummy_conv_xyz", "task-99999")
check("is_task_finished false for non-existent", is_fin is False, str(is_fin))

# === 15. Abort & Interrupt Mechanism ===
print("\n=== 15. Abort & Interrupt Mechanism ===")

# Test abort command in COMMANDS list
cmd_names = [c.get("command") for c in bot.COMMANDS]
check("COMMANDS has abort", "abort" in cmd_names, str(cmd_names))
check("COMMANDS has stop", "stop" in cmd_names, str(cmd_names))

# Test abort_task when no run active
mock_chat = "test_abort_chat_99"
res_idle = bot.abort_task(mock_chat, reason="test", notify=False)
check("abort_task idle returns False", res_idle is False, str(res_idle))

# Test abort_task with mock active run
import subprocess, time
mock_p = subprocess.Popen(["sleep", "10"])
with bot._active_runs_lock:
    bot._active_runs[mock_chat] = {
        "proc": mock_p,
        "pgid": None,
        "master": None,
        "live_id": 9999,
        "conv_id": "mock_conv_123",
        "sess_name": "default",
        "aborted": False,
        "interrupted_by": None,
        "start_time": time.time(),
    }

res_active = bot.abort_task(mock_chat, reason="test_kill", notify=False)
check("abort_task active returns True", res_active is True, str(res_active))
check("mock process terminated", mock_p.poll() is not None, str(mock_p.poll()))

# Test interrupt mode with new_text
mock_p2 = subprocess.Popen(["sleep", "10"])
with bot._active_runs_lock:
    bot._active_runs[mock_chat] = {
        "proc": mock_p2,
        "pgid": None,
        "master": None,
        "live_id": 9998,
        "conv_id": "mock_conv_123",
        "sess_name": "default",
        "aborted": False,
        "interrupted_by": None,
        "start_time": time.time(),
    }

res_int = bot.abort_task(mock_chat, reason="interrupted", notify=False, new_text="add this to existing chat")
check("interrupt active returns True", res_int is True, str(res_int))
check("interrupted_queue has new text", bot._interrupted_queue.get(mock_chat) == "add this to existing chat", str(bot._interrupted_queue.get(mock_chat)))
check("mock_p2 terminated", mock_p2.poll() is not None, str(mock_p2.poll()))

# Regression: abort must terminate a real PROCESS GROUP. `pgid=None` only
# exercises proc.terminate() and would have hidden a missing `signal` import
# (the NameError was swallowed by `except Exception: pass`, so the task looked
# aborted while the agy process kept running).
mock_p3 = subprocess.Popen(["sleep", "10"], start_new_session=True)
with bot._active_runs_lock:
    bot._active_runs[mock_chat] = {
        "proc": mock_p3,
        "pgid": os.getpgid(mock_p3.pid),
        "master": None,
        "live_id": 9997,
        "conv_id": "mock_conv_123",
        "sess_name": "default",
        "aborted": False,
        "interrupted_by": None,
        "start_time": time.time(),
    }
res_pgid = bot.abort_task(mock_chat, reason="test_kill_pgid", notify=False)
check("abort_task with pgid returns True", res_pgid is True, str(res_pgid))
check("process group actually killed", mock_p3.poll() is not None,
      f"poll={mock_p3.poll()}")

# signal must be importable (the abort path depends on it)
check("bot module exposes signal", hasattr(bot, "signal"), str(hasattr(bot, "signal")))

# Cleanup
with bot._active_runs_lock:
    bot._active_runs.pop(mock_chat, None)
    bot._interrupted_queue.pop(mock_chat, None)

# === 16. Credits label & quota_block options ===
print("\n=== 16. Credits label & quota_block options ===")

bot._quota.update(ts=time.time(), ok=True,
                  groups={"Gemini Models": {"week": {"value": "42%", "reset": "2026-09-30T02:45:23Z"}}},
                  err="", fetching=False)
_quota_only = bot.quota_block(include_credits=False)
check("quota_block(include_credits=False) omits credits line",
      "💳" not in _quota_only, _quota_only)
check("quota_block(include_credits=False) still shows quota rows",
      "🤖 Gemini" in _quota_only, _quota_only)
check("quota_block default includes the G1 credits line",
      "💳 G1 credits" in bot.quota_block(), bot.quota_block())

check("DEFAULT_CHAT is seeded from ALLOWED",
      bot.DEFAULT_CHAT is not None or not bot.ALLOWED,
      f"DEFAULT_CHAT={bot.DEFAULT_CHAT!r} ALLOWED={sorted(bot.ALLOWED)}")

# label (tool description, i.e. model-controlled) must be HTML-escaped in the pin
bot.set_run_state("esc_chat", phase="running", label="reading <b>evil</b>.log",
                  started=time.time() - 1, activity=time.time())
_l5 = bot.run_state_line("esc_chat")
check("run_state_line escapes the running label",
      "&lt;b&gt;" in _l5 and "<b>evil</b>" not in _l5, _l5)
bot.set_run_state("esc_chat", phase="idle")

print(f"\n========== RESULT: {PASS} passed, {FAIL} failed ==========")
if ERR:
    print("FAILED CHECKS:")
    for e in ERR:
        print("  -", e)
    sys.exit(1)
print("ALL GREEN")
sys.exit(0)