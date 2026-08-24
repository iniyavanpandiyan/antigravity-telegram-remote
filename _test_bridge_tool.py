import json, sys
sys.path.insert(0, ".")
import bot

captured = {"att": None, "long": []}

def fake_send_code_attachment(chat_id, text, caption, filename, reply_markup=None):
    captured["att"] = (chat_id, text, caption, filename)
    return {"ok": True}

def fake_send_long(chat_id, text, reply_markup=None):
    captured["long"].append((chat_id, text))

bot._send_code_attachment = fake_send_code_attachment
bot.send_long = fake_send_long

# st=5 write_to_file: AbsolutePath + CodeContent (whole payload is JSON)
payload = json.dumps({
    "AbsolutePath": "/home/u/proj/app.py",
    "CodeContent": "def hello():\n    return 42\n",
    "toolSummary": "created app.py",
}).encode("utf-8")

bot._bridge_tool_step(999, payload)

att = captured["att"]
assert att is not None, "no attachment sent"
chat_id, text, caption, filename = att
assert chat_id == 999
assert "✏️ edit" in caption, caption
assert filename.endswith(".txt"), filename
assert "app.py" in filename, filename
assert "def hello" in text, "content not forwarded"
assert not captured["long"], "should not fall back to send_long for write"

print("write_to_file ->", caption, "|", filename)
print("PASS")

# st=21 run_command
captured["att"] = None
captured["long"] = []
cpl = json.dumps({"CommandLine": "ls -la", "toolSummary": "list"}).encode()
bot._bridge_tool_step(999, cpl)
assert any("⚡ ran:" in t for _, t in captured["long"]), "cmd not forwarded"
print("cmd ->", captured["long"][0][1])
print("PASS")
