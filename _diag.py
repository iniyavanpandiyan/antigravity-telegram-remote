import os, sqlite3, json, sys
HOME=os.path.expanduser("~")
DB=os.path.join(HOME, ".antigravity-telegram-remote", "bridge.db")
if not os.path.exists(DB):
    # try cwd
    DB="bridge.db"
con=sqlite3.connect(DB)
print("=== sessions ===")
for row in con.execute("SELECT chat_id,name,conv_id,model,workspace,source,ide_id,title,bridge FROM sessions"):
    print(row)
print("\n=== active ===")
for row in con.execute("SELECT * FROM active"):
    print(row)
print("\n=== prefs ===")
for row in con.execute("SELECT * FROM prefs"):
    print(row)
con.close()
