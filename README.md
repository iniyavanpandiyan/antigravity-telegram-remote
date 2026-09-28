# Antigravity (agy) Telegram Bridge

A customized Telegram bridge that drives the official **`agy` CLI** (headless, `stream-json`)
so a Telegram chat gets a near-realtime agent experience. Built on top of the `agy` CLI's
streaming output rather than its interactive UI.

Dedicated bot: **@antigravityfiip_bot**, polled via `getUpdates` (handles both `message`
and `callback_query` events).

---

## Features

| Feature | Description |
|---|---|
| **Live answer streaming** | One message is edited in place as tokens arrive (`edit_message_text`), not a burst of messages. |
| **Tool-call / step notifications** | Each tool call, file edit, diff, and command is rendered as a structured bubble — output/diff is collapsible. |
| **Named + pinned sessions** | Multiple named sessions per chat, switchable via inline buttons. Pinned status message shows the active session + model + cwd@branch. |
| **Model picker** | Inline buttons, grouped. See [Models](#models). |
| **MCQ / option detection** | When the model emits question/option style text, options are extracted and rendered as tappable inline buttons. |
| **Per-session reasoning effort** | `low` / `medium` / `high`, stored per chat. |
| **Per-session workspace** | Each session can bind to a workspace dir; a `/workspace` browser + live `cwd @branch` label. |
| **IDE session import** | Import your laptop-IDE (editor) sessions into Telegram so both share one live history. |
| **git push from chat** | `/git-push`-style helper for the active session workspace (in-chat commit + push). |
| **MCQ buttons** | See above — tappable option buttons instead of typed replies. |

---

## How it works

The bridge invokes the `agy` CLI headless with stream-json output and parses its structured
event stream (`step_update`, `agent_response`, `reasoning`, tool events). It then maps those
events to Telegram messages:

1. A single **live answer** message is created once and edited as `text_delta`s arrive.
2. Tool/step events render as separate **bubbles** with collapsible output and diffs.
3. `reasoning` events are captured in memory (see [Thinking / CoT](#thinking--cot)) — not persisted.
4. On completion the durable bridge cursor advances past the fully-forwarded steps.

### IDE ↔ Telegram session sharing (the symlink)

The `agy` CLI and the IDE editor keep their conversation stores in **separate** directories:

- CLI: `~/.gemini/antigravity-cli/conversations`
- IDE: `~/.gemini/antigravity-ide/conversations`

To give both binaries the **same** live history, the bridge symlinks the CLI store dirs onto the
IDE store dirs. `agy` then reads/writes the *same physical* conversation `.db` the editor uses.
Cloud sync propagates both ways — so a conversation started on Telegram continues on the laptop
IDE and vice-versa, with no copy and no "source flip."

> Caveat: this requires the CLI and IDE to be on the same machine and the symlink to survive
> the CLI run. See [Troubleshooting](#troubleshooting).

---

## Setup

### Prerequisites

- Python 3.10+
- `agy` CLI on `PATH` (or at `~/.local/bin/agy`)
- A Telegram bot token (via [@BotFather](https://t.me/BotFather))
- `git`, `requests`

### Install

```bash
# 1. Clone / copy the bridge into place
mkdir -p ~/.antigravity-telegram-remote && cd ~/.antigravity-telegram-remote

# 2. Install the only third-party dependency (everything else is stdlib)
pip install requests

# 3. Create config.json from your bot token (see Configuration below)
```

### Configuration

`config.json`:

```json
{
  "telegramBotToken": "1234567:AAA...your-bot-token",
  "allowedUserIds": ["996288865"],
  "workspaceBaseDir": "/home/<you>/workspace",
  "defaultModel": "gemini-3.7-flash-high"
}
```

| Key | Purpose |
|---|---|
| `telegramBotToken` | Your Telegram bot token from BotFather. **Never commit this.** |
| `allowedUserIds` | Whitelist of Telegram user IDs allowed to use the bot. |
| `workspaceBaseDir` | Default workspace directory sessions point at. |
| `defaultModel` | The model selected for new sessions. |

> **Security:** the bridge hard-fails at startup if the token is missing, placeholder
> (`endswith(":***")`), or shorter than 20 chars.

### Run as a systemd user service

The bridge ships as `agy-telegram.service` (user-level). Enable it with:

```bash
systemctl --user enable --now agy-telegram.service
systemctl --user status agy-telegram.service       # check it's active
systemctl --user restart agy-telegram.service      # reload after edits
```

---

## Commands

These are registered via `setMyCommands` so they appear in the bot's `/` menu:

| Command | Description |
|---|---|
| `/start` | Show help / bot intro |
| `/help` | List commands & usage |
| `/new` | Start a new named session & switch to it |
| `/sessions` | Switch / delete sessions (inline buttons) |
| `/model` | Pick a model (inline buttons) |
| `/workspace` | Show or pick the session workspace dir |
| `/history` | Show your last 5 messages + the bot's replies |
| `/effort` | Set reasoning effort: low / medium / high |
| `/steer` | Guide the **running** turn without killing it (queued to run right after) |
| `/abort` | Kill the running turn (alias: `/stop`) |
| `/credits` | Show the **G1** credit balance; `/credits on\|off` spends credits once quota hits 0 |
| `/pin` | Pin the live status message |

`/quota` and `/usage` were dropped from the menu: they reported the same number twice.
The pinned message now carries a persistent quota dashboard instead — 5-hour **and**
weekly limits for both the Google (Gemini) group and the third-party (Claude/GPT) group,
plus the credit state — refreshed every 5 minutes in the background. `/quota` and
`/usage` still work as typed aliases if you have them in muscle memory.

### The pinned message

The pin is the always-on dashboard. It shows:

* **Run state** — `⏳ running <n>s`, `⚠️ stalled` (no output for 45s+),
  `✅ done · <n>s · <n> tools · <n> files edited`, `❌ failed`, `🛑 aborted`, or `🟢 idle`.
* **Quota** — 5h + weekly for Gemini and for Claude/GPT, with reset countdowns.
* **Credits** — balance of the **G1** pool (`agy /credits`) and whether it is
  enabled. Note this is *not* the balance the IDE shows under
  Settings → Models; that is a separate pool (`CreditsProto`) the CLI cannot read.
* Session model/workspace, context tokens and system load.

So you can tell whether a turn is alive, stuck, or finished without sending anything.

---

## Models

The model picker exposes all agy-supported models, grouped by family:

| Family | Variants |
|---|---|
| **Gemini 3.7 Flash** | High / Medium / Low |
| **Gemini 3.6 Flash** | High / Medium / Low |
| **Gemini 3.5 Flash** | High / Medium / Low |
| **Gemini 3.1 Pro** | High / Low |
| **Claude Sonnet 4.6** | Thinking |
| **Claude Opus 4.6** | Thinking |

> The reasoning-effort (`low`/`medium`/`high`) applies to Gemini variants. Claude thinking
> models are detected and handled separately.

---

## Storage

All bridge state lives in `antigravity.db` (SQLite, created on first run):

| Table | Purpose |
|---|---|
| `sessions` | Per-chat named sessions: conv_id, model, workspace, source (`local`/`ide`), ide_id, title |
| `chat_sessions` | Chat → category mapping |
| `workspace_bindings` | Chat → workspace path binding |
| `prefs` | Per-chat reasoning `effort` |
| `active` | Chat → active session name |
| `threads` | Message send-log (role, text, timestamp) |
| `pinned_msg` | Pinned status message ids (survives restarts) |
| `bridge_cursor` | Durable forward cursor per conversation (`conv_id → last_idx`) |
| `templates` | Reusable prompt templates |

The conversation steps themselves **do not** live here — they live in the agy conversations `.db`
(CLI or IDE store, shared via the symlink). The bridge only keeps its own bookkeeping.

---

## Thinking / CoT

The agy version used here exposes model reasoning as a **transient stream event only** — it is
never persisted to the DB. The bridge captures those deltas in memory (`reasoning_buf`).

**Unless disabled**, the bridge posts the captured reasoning once as a `💭 thinking
<tg-spoiler>…</tg-spoiler>` bubble immediately after the answer.

> **NOTE (2026-08-25):** this is currently **disabled by default** per user request — the bridge
> still captures reasoning (so it can be re-enabled trivially) but no longer posts the thinking
> bubble. The answer is sent with the CoT suppressed. To re-enable, flip the `if False and
> reasoning_buf:` guard back to `if reasoning_buf:`.

---

## Development

### Tests

```bash
python3 test_render.py
```

Expected: **140/140 green** (`test_render.py` covers the markdown/HTML rendering helpers —
`_md_send` / `_md_edit`, the `_html_safe_chunks` / table handling — plus the quota
dashboard, run-state line, `/steer` queue and the abort path). It `import`s `bot`
directly, so run it from the repo root.

### Lint / compile

```bash
python3 -m py_compile bot.py ide_preview.py
```

---

## Troubleshooting

### "Failed to get shell integration"
The headless `agy` needs the CLI's backend to be reachable. On the host where `agy` runs,
source=1 headless execution works (`run_command` returns output). If it fails, verify the
`agy` CLI backend/port used by the bridge matches what `agy` actually binds.

### Conversation doesn't appear on the laptop IDE
This is the symlink caveat. Telegram's `agy` must hit the **same physical** `.db` the IDE uses.
If you used the older "copy + flip source" approach, Telegram and the IDE would write to
different files (`~/.gemini/antigravity-cli/conversations/` vs `~/.gemini/antigravity-ide/conversations/`)
and never sync. The current design uses a **symlink `cli → ide`** so both binaries share one file.
If it still diverges, check that the symlink survives the CLI run (some versions replace a
symlink with a regular file at end-of-run).

### Callback button taps don't do anything
The Telegram gateway uses **long polling**; callback-query events are received but some clients
don't route them to the agent. The bridge handles `callback_query` itself via `getUpdates`, but
if a gateway sits in front, taps may not reach the agent — use the numbered-choice fallback
where available.

### Pinned status message lost after restart
The bridge restores pinned message ids from `pinned_msg` on startup. If a pin is still lost,
re-run `/pin` in that chat.

---

## Files

| File | Purpose |
|---|---|
| `bot.py` | The bridge (entry point, run by the systemd service). ~2,900 lines. |
| `ide_preview.py` | IDE-session preview helper (`ide_preview`), imported by `bot.py`. |
| `test_render.py` | Test suite for the render/forward/feature paths (140 cases). |
| `requirements.txt` | Python dependencies (`requests`). |
| `config.json` | Configuration — your bot token. **Git-ignored.** |
| `antigravity.db` | Bridge state (SQLite). **Git-ignored.** |
| `.gitignore` | Excludes secrets, DBs, caches from git. |

---

## License

Internal tooling. Use at your own risk.
