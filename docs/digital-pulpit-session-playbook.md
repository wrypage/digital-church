# Digital Pulpit — Session Playbook
Version 1.0 — [Month Year]
Last updated: [YYYY-MM-DD]

This file defines how a working session should run.
Its purpose is to prevent drift, repeated mistakes, and lost context.

Read this at the start of every session.

---

## The rule

**One session, one goal.**

Before starting, name the goal in one sentence.
If you cannot name it, clarify it before touching anything.

---

## Start of session

1. **Read `state-of-the-project.md`** — where things stand right now
2. **Read the last 5 entries in `decisions.md`** — what was just tried or decided
3. **State the session goal** — one sentence, written out loud
4. **Identify the current phase** — are we still in the right phase?
5. **Check for open issues** — anything in decisions.md "Active unresolved issues" that
   blocks the session goal?

If the session is with Claude Code or a new AI session, paste:
- The session goal
- The relevant section of `state-of-the-project.md`
- The last 5 decisions.md entries
- Any relevant architecture.md section

**Never start a Claude Code session without reading decisions.md first.**

---

## During session

- **Make one change at a time.** Evaluate before moving on.
- **Test before declaring something done.** "Seems to work" is not done.
- **Log immediately.** If something fails or succeeds in an instructive way,
  add a decisions.md entry before moving on. Memory of why things were done
  decays within the same session.
- **Use review.md** when evaluating output quality — not intuition alone.
- **Name the failure mode** if something breaks. Not just "it didn't work" —
  what specifically failed and why?
- **Stop and clarify** if the session goal shifts. Don't silently pivot.

---

## Signs the session is drifting

- You are fixing something that wasn't the session goal
- You are rebuilding something that was already working
- You have lost track of what the original goal was
- The AI is generating confidently but you are not sure it's right
- You are adding complexity to avoid a hard decision

When you notice drift: stop, re-read the session goal, decide whether to
continue or declare the session done and start a new one with a clearer goal.

---

## End of session

1. **Update `state-of-the-project.md`** — what changed, what was learned
2. **Add decisions.md entries** — for everything tried, accepted, or rejected
3. **Note any new open issues** — in decisions.md "Active unresolved issues"
4. **Define the next session goal** — write it at the bottom of
   state-of-the-project.md so the next session starts with a clear target
5. **Commit / save** — push to repo if applicable

---

## Session types

Not all sessions are the same. Name the type before starting.

**Build session** — adding new functionality or pipeline stages
- Goal: one working, tested component
- Done when: it runs cleanly and the output is verified

**Debug session** — fixing something broken
- Goal: identify root cause, not just suppress symptoms
- Done when: the failure mode is understood and documented in decisions.md

**Evaluation session** — reviewing output quality
- Goal: honest assessment against review.md criteria
- Done when: review.md is filled out and next tuning actions are named

**Documentation session** — updating project docs
- Goal: close the gap between what the system does and what the docs say
- Done when: docs reflect current reality, not aspirational state

**Enrichment / ingest session** — running data pipelines
- Goal: data lands correctly in the right place
- Done when: verified in database, not just "no errors in terminal"

---

## The question that ends every session

> Does the project know more now than it did at the start of this session?

If yes: the session succeeded, even if nothing shipped.
If no: figure out why before closing.
