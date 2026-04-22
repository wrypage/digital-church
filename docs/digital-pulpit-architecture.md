# Digital Pulpit — Architecture
Version [X.X] — [Month Year]
Last updated: [YYYY-MM-DD]

This document defines how the project is built.
For rejected technical approaches, read decisions.md before changing anything.

---

## What this system does

*[One paragraph. Input → process → output. What goes in, what happens, what comes out.
Be concrete. Name the components.]*

---

## Stack

*[List every tool, library, service, API, and language in use.
Include version numbers where they matter.
Note which are confirmed working vs. provisional.]*

| Layer | Tool / Service | Notes |
|-------|---------------|-------|
| Language | | |
| Framework | | |
| Database | | |
| Embeddings | | |
| AI / LLM | | |
| External APIs | | |
| Deployment | | |

---

## Pipeline / Data flow

*[How does data move through the system?
Use a simple diagram or numbered steps.
Name every transformation point.]*

```
[Input] → [Step 1] → [Step 2] → [Step 3] → [Output]
```

*[Describe each step in 1-2 sentences.]*

---

## File / folder structure

*[What lives where? What is each file responsible for?
List the key files — not every file, just the ones that matter.]*

```
project/
  [key file]     — [what it does]
  [key file]     — [what it does]
  [folder]/
    [key file]   — [what it does]
```

---

## Schema

*[If there's a database: tables, key fields, relationships.
If there's a file format: structure and required fields.
If there's an API: key endpoints and payloads.]*

---

## Key architectural rules

*[The non-negotiable constraints that protect the system.
Example from Here & Now: "Every external provider must be normalized into internal
app types before reaching UI components."
List 3-7 rules. These are the things Claude Code must not violate.]*

1.
2.
3.

---

## Environment and credentials

*[What environment variables are required?
Where does the .env file live?
What keys are needed and where to get them?
Do not store actual credentials here — reference them.]*

```
REQUIRED_KEY=          # Description, where to get it
ANOTHER_KEY=           # Description
```

---

## Known fragile points

*[What parts of this system are most likely to break?
What external dependencies are unreliable?
What assumptions could become false?]*

---

## Development workflow

*[How do you run this locally?
What are the commands?
What should be checked before starting a session?]*

```bash
# Start
[command]

# Test
[command]

# Deploy / publish
[command]
```

---

## Current known costs

Last updated: [YYYY-MM-DD]

This section tracks the real-world cost of running the system.
Update whenever pricing, models, or pipeline structure changes.
Ignorance of cost is a design flaw, not a detail to handle later.

### Cost per run (typical)

| Component | Model / Service | Unit | Estimated cost |
|-----------|----------------|------|---------------|
| Summarization | | per item | $ |
| Embeddings | | per 1K chunks | $ |
| Analysis / enrichment | | per item | $ |
| Storage | | per month | $ |

**Typical full run:** $X for Y items
**At scale (1,000 items):** $X estimated

### Cost drivers

| Driver | Why it matters |
|--------|---------------|
| Token volume | Largest variable cost |
| Items processed per run | Linear scaling factor |
| Chunk size | Affects total embedding count |
| Model choice | Major multiplier |
| Reprocessing / re-runs | Duplicate cost — avoid |

### Known optimization levers

| Lever | Effect | Tradeoff |
|-------|--------|----------|
| Summary-first processing | Reduces token usage | May lose nuance |
| Smaller model for routine tasks | Lower cost | Quality tradeoff |
| Batch processing | Fewer API calls | More complexity |
| Caching / deduplication | Avoids reprocessing | Storage overhead |

### What we don't yet know

*[List the cost unknowns that matter most for this project.
These should be measured and moved into the table above.]*

- [ ]
- [ ]
