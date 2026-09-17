# Test fixtures

Copied unchanged from wikiflow's `tests/fixtures/` (2026-09-17), so unit tests never touch the
network. Wikimedia pageviews data is released under CC0.

## `projectviews-20260915-120000`

A real, unmodified `projectviews` file: 26 KB, 1,946 rows, one per project. The label is the END
of the hour (wikiflow ADR 0001), so it covers **11:00-11:59 UTC on 2026-09-15**. It holds the full
real domain-code inventory, including the awkward cases:

- `.m` meaning **mobile** (`en.m`, `en.m.d`) and meaning **wikimedia.org** (`commons.m`)
- both at once: `commons.m.m`
- prefix-less wikis where the mobile marker comes first: `m.wd`, `m.w`, `m.s`
- a literal `""` domain code with 5,383 views
- hyphenated language codes (`bat-smg`, `zh-yue`)

On 2026-09-17 the M0-S4 spike reconciled a full Spark scan of the matching pageviews hour against
this file's live original: 1,946 domains, 0 mismatches, 18,917,973 views.

## `pageviews-sample-20260914-120000.gz`

A trimmed slice of the real dump for 2026-09-14 11:00 UTC (label 12:00): 62 KB, 4,379 lines. It
keeps **all 379 tracked-company rows** of that hour plus 4,000 sampled filler rows, in source order.

| Expected from extraction | |
|---|---|
| lines | 4,379 |
| company rows | 379 |
| company views | 5,138 |
| distinct domain codes | 320 |
| total views | 15,255 |
