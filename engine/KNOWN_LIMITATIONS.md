# Known Limitations - Digital Pulpit Brain Engine

## Theological Axis Scoring

### Negation Context Not Detected

**Status:** Known limitation (as of 2026-02-25)
**Affected Component:** `engine/brain.py` - keyword-based axis scoring
**Severity:** Moderate - affects sermons with strong refutation language

#### Description

The keyword-based scoring system cannot detect negation or theological context. A sermon that **refutes** a concept scores identically to one that **affirms** it.

#### Examples

- **"No condemnation"** scores as fear language (keyword: "condemnation")
- **"There is therefore now NO condemnation"** (Romans 8:1) scores as condemning
- **"Not by works"** scores as effort-focused (keyword: "works")
- **"Instead of condemnation"** scores as fear-focused

#### Impact

**Sermons affected:**
- Romans 8:1 expositions ("no condemnation in Christ Jesus")
- John 3:17 expositions ("God did not send His Son to condemn")
- Grace-focused refutations of legalism
- Hope-focused refutations of despair

**Observed case:**
- Video ID: `588ea171ce4164a1` ("Tracking in the Dirt")
- Hope vs Fear score: **-0.560** (fear-leaning)
- Actual content: **Anti-condemnation** sermon preaching freedom in Christ
- Reason: 5 instances of "condemnation" keyword, all in negation context

#### Mitigation Strategies

1. **Monitor high-frequency refutation texts** for corpus-wide bias:
   - Romans 8:1 ("no condemnation")
   - John 3:17 ("not to condemn")
   - Ephesians 2:8-9 ("not by works")

2. **Manual review** of sermons with:
   - Strong negative scores + grace-heavy keywords
   - High fear scores + known gospel-centered preachers
   - Unexpected axis reversals after calibration

3. **Context-aware NLP** (future enhancement):
   - Dependency parsing to detect negation patterns
   - Sentiment analysis on surrounding sentences
   - Theological phrase recognition ("no condemnation", "not by works")

#### Calibration Settings (v5.4)

Current settings work well for corpus-wide averages but cannot resolve individual sermon context:

```python
AXIS_INERTIA_K = 2.0              # Prevents ±1.0 saturation
AXIS_MIN_ACTIVATION = 1.0         # Filters weak signals
```

**Distribution after calibration:**
- 0% at perfect ±1.000 scores (down from 52.3%)
- Hope average: 0.463 (well-centered)
- 10.6% negative hope sermons (includes false negatives from negation)

#### Recommendation

Accept this limitation for now. The keyword approach provides valuable corpus-wide signal despite individual sermon edge cases. A context-aware NLP upgrade would require significant architectural changes and may not be worth the engineering cost.

**For pastoral use:** Trust the axis scores for aggregates and trends, but manually verify any surprising individual sermon scores.

---

**Last Updated:** 2026-02-25
**Related Files:** `engine/brain.py`, `data/digital_pulpit_config.json`
**Calibration Version:** Config v5.4, Summary Generator v2.1
