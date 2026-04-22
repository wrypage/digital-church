# Digital Pulpit — Principles
Version 1.0 — [Month Year]
Last updated: [YYYY-MM-DD]

This document records the non-negotiable philosophical commitments of this project.
These are not implementation rules — those live in architecture.md.
These are the beliefs that should survive any technical change.

If a decision feels right technically but wrong here, trust this document.
If a decision feels efficient but violates a principle, name the tension explicitly
before proceeding.

---

## What this project is trying to be

*[One paragraph. Not what it does — what it is trying to be.
What kind of thing is this, philosophically?
What would make it genuinely good rather than merely functional?]*

---

## Core principles

*[List 5-10 principles. Each one should be:
- Short enough to remember
- Specific enough to settle a real dispute
- Derived from hard-won experience, not aspiration

Bad principle: "Be accurate."
Good principle: "Never synthesize a claim the source material doesn't support.
                 Infer nothing. Name the gap instead."]*

### 1. [Principle name]

*[1-3 sentences. What is this principle? Why does it matter for this project?
What goes wrong when it's violated?]*

### 2. [Principle name]

### 3. [Principle name]

### 4. [Principle name]

### 5. [Principle name]

---

## Principles shared across all projects

*[These are the cross-project principles that apply regardless of what is being built.
Populate this section once and carry it into every new project.]*

### Observation before interpretation

Report what is present before naming what it means.
A system that interprets before it observes will hallucinate meaning into absence.

### No invented facts

If the source material doesn't contain it, don't generate it.
Name the gap instead of filling it.
"I don't have evidence for this" is a complete and honest answer.

### Voice must be earned

Don't simulate a voice by imitating its surface features.
Understand what the voice is doing and why, then produce that effect genuinely.
A voice that sounds right but isn't rooted is worse than a generic voice —
it's a forgery.

### Pipelines must tell the truth about themselves

Silent failures are the most dangerous failures.
A system that reports success when it skipped work is lying.
Logs, summaries, and status outputs must reflect what actually happened.

### Slow is almost always right

Speed is not a virtue in systems built for meaning.
Slow, thorough, and honest beats fast, approximate, and confident.
This applies to pipelines, to output generation, and to sessions.

### Document the failure, not just the fix

When something breaks, the failure mode is the valuable thing — not just the solution.
Record what failed, why it failed, and what assumption was wrong.
This is the only way to prevent the same failure in a different form.

---

## Principles in tension

*[Sometimes two principles pull in different directions.
Name the known tensions explicitly so they can be navigated consciously.]*

| Principle A | Principle B | How to navigate |
|------------|------------|----------------|
| | | |
| | | |

---

## What violating these principles looks like

*[For each major failure mode, name which principle it violates.
This helps catch drift before it becomes a problem.]*

| Failure mode | Principle violated |
|-------------|-------------------|
| Output sounds confident but is based on thin data | No invented facts |
| System reports "done" but work was skipped | Pipelines must tell the truth |
| Voice drifts into generic AI register | Voice must be earned |
| | |
