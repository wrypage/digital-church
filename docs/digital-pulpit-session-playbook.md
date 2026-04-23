# Digital Pulpit — Session Playbook
Version 1.0 — April 2026
Last updated: 2026-04-22

---

## Orienting Claude at the start of a session

Paste these two URLs into Claude.ai at the start of any session:

https://raw.githubusercontent.com/wrypage/digital-church/main/docs/digital-pulpit-state-of-the-project.md
https://raw.githubusercontent.com/wrypage/digital-church/main/docs/digital-pulpit-decisions.md

Claude will fetch both and be fully oriented without uploading anything.

---

## The rule

One session, one goal. Name it before touching anything.

---

## Start of session (Claude.ai)

1. Paste the orientation URLs above
2. State the session goal — one sentence
3. Check decisions.md Active Unresolved Issues — anything blocking?
4. Write the Claude Code brief

**Claude Code brief format:**
```
Session goal: [one sentence]
Read digital-pulpit-decisions.md before touching anything.
Do NOT touch: [list what's off-limits]
Success condition: [how will you know it's done?]
Context: [1-2 sentences of relevant state]
```

---

## Start of session (Claude Code)

```bash
cd ~/Library/Mobile\ Documents/com~apple~CloudDocs/digital-church
git pull
claude
```

Settings already configured — no flags needed.

---

## During session

- Make one change at a time — evaluate before moving on
- Never modify channels.csv without checking Primary Orientation column logic
- Never change digital_pulpit_config.json version without updating patch_notes
- Log anything that fails or surprises immediately
- The central question: does this still feel like climate listening, not drift policing?

---

## Signs the session is drifting

- Fixing something that wasn't the session goal
- Rebuilding something already working
- Adding complexity to avoid a hard decision
- The system is producing topical clustering instead of emphasis detection

---

## End of session

1. Come back to Claude.ai — tell Claude what happened
2. Claude helps write decisions.md entries
3. Update state-of-the-project.md — what changed, what was learned
4. Define the next session goal
5. Write session record to Supabase — ask Claude to insert via MCP:
   projects: ['digital-pulpit'], summary, decisions, next_steps
6. Commit and push:
```bash
git add -A && git commit -m "session: [goal]" && git push
```

---

## Session types

**Vacuum session** — running ingest pipeline
- Done when: new videos in DB with status=transcribed, verified in sqlite3

**Brain session** — running theological analysis
- Done when: brain_results rows exist, drift metrics computed

**Assembly session** — generating avatar scripts
- Done when: assembly_scripts row exists with real content, not fallback

**Tuning session** — adjusting scoring config
- Done when: digital_pulpit_config.json version bumped, patch_notes updated

**Documentation session** — updating project docs
- Done when: docs reflect current reality

---

## The question that ends every session

> Does the system know more about the evangelical preaching climate now than it did at the start?

If yes: succeeded, even if nothing shipped.
If no: figure out why before closing.
