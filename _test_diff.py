import sys
sys.path.insert(0, ".")
import bot

# --- diff helpers ---
d = bot._format_diff("def f():\n  return 1\n", "def f():\n  return 2\n  # note\n")
assert not d.startswith("+++") and not d.startswith("---"), "header not stripped: " + repr(d)
assert "\n-  return 1\n" in d and "\n+  return 2\n" in d and "\n+  # note\n" in d
print("format_diff OK:\n" + d)

raw = "@@ -1,3 +1,3 @@\n--- a/foo.py\n+++ b/foo.py\n-import os\n+import sys\n print('x')"
assert bot._normalize_edit(raw) == "-import os\n+import sys\n print('x')"
a, r = bot._count_diff_changes(bot._normalize_edit(raw))
assert (a, r) == (1, 1)

info = {"name": "replace_file_content",
        "argumentsJson": '{"TargetPath":"/a/b.py","old_string":"x = 1","new_string":"x = 2"}'}
res = bot._diff_from_tool_info(info)
assert res[0] == "x = 1" and res[1] == "x = 2" and res[3] is True

info2 = {"name": "write_to_file",
         "argumentsJson": '{"TargetPath":"/a/new.py","CodeContent":"line1\\nline2\\nline3"}'}
res2 = bot._diff_from_tool_info(info2)
assert res2[3] is False and len(res2[1].splitlines()) == 3
print("diff helpers OK")

# --- _split_md_code ---
md = "# Plan\nDo the thing.\n```python\nx = 1\ny = 2\n```\nThen test.\n@@ -1 +1 @@\n-old\n+new\nTail prose."
prose, blocks = bot._split_md_code(md)
assert "Do the thing" in prose and len(blocks) == 2
assert blocks[0] == "x = 1\ny = 2" and blocks[1].startswith("@@ -1 +1 @@")
assert bot._split_md_code("text\n```js\nconst a=1\n")[1] == ["const a=1"]
print("split_md_code OK")

# --- E2E via mocked senders ---
sent = []
def fake_send_long(chat_id, text, **kw):
    sent.append(("msg", text, kw))
def fake_send_doc(chat_id, content, fn, **kw):
    sent.append(("doc", content[:60], fn, kw))
def fake_code_attach(chat_id, code, cap, fn, **kw):
    sent.append(("code_attach", cap, fn, code[:80]))

bot.send_long = fake_send_long
bot._send_doc = fake_send_doc
bot._send_code_attachment = fake_code_attach

su = {"step_type": "tool", "step_index": 3, "state": "DONE",
      "tool_info": {"name": "replace_file_content",
                    "argumentsJson": '{"TargetPath":"/proj/app.py","old_string":"x = 1","new_string":"x = 2"}',
                    "output": "applied"}}
bot._render_tool(996288865, su, set())
print("\n--- _render_tool DONE (edit) ---")
for s in sent:
    print(s)

sent.clear()
su2 = {"step_type": "tool", "step_index": 4, "state": "DONE",
       "tool_info": {"name": "run_command", "parameters": {"CommandLine": "ls"},
                     "output": "<html>\n<body>hi</body>\n</html>"}}
bot._render_tool(996288865, su2, set())
print("\n--- _render_tool DONE (cmd, html output) ---")
for s in sent:
    print(s)

sent.clear()
bot.md_to_html = lambda c: "<html>" + c + "</html>"
content = "# Plan\nDo X.\n```python\nfoo()\nbar()\n```\nDone.\n--- a/x\n+++ b/x\n-old\n+new"
prose, blocks = bot._split_md_code(content)
bot.send_long(996288865, "cap\n" + bot.md_to_html(prose))
for i, blk in enumerate(blocks):
    ext = "diff" if blk.strip().startswith(("---", "+++", "diff", "Index:", "@@")) else "txt"
    bot._send_code_attachment(996288865, blk, "block%d" % (i + 1), "plan_block%d.%s" % (i + 1, ext))
print("\n--- plan forward ---")
for s in sent:
    print(s)

print("\nALL E2E TESTS PASSED")
