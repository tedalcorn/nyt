# Author Overrides — Notes and Rationale

`author_overrides.json` is a flat key→value lookup used by `build_data.py` to
normalize author names before computing statistics. Key = wrong/variant form as
it appears in the NYT API. Value = canonical form to use instead.

## Entry categories

- **"St." compound last names** — API drops the second word of compound last
  names like "St. Fleur". Only add entries here when the correct full name is
  confirmed.

- **Middle initial sometimes dropped / capitalization varies** — e.g. "Michael
  J. de la Merced" vs "Michael De La Merced".

- **Trailing "Photographs" suffix** — byline parsed as "Name; Photographs by
  …" produces "Name Photographs" as an author token.

- **Podcast show names indexed as bylines** — merge to the host's personal
  byline. Includes Unicode quote variants (the Ezra Klein Show appears with
  both curly-open + straight-close and both-curly quote forms in the API data).

- **"X Nyt" suffix** — Metro Briefing and wire-style bylines (2001–2006)
  produce "Firstname Lastname Nyt" tokens.

- **Accent mark variants** — API inconsistently strips/preserves diacritics.

- **Manual corrections** — both short and full forms had >10 articles, so the
  conservative auto-dedup threshold didn't fire. Verified via beats, section,
  year range, and where available Wikipedia/LinkedIn.

- **Middle-initial variants confirmed same person** — count >10 so auto-dedup
  skips them; manually verified.

- **Bare-s prefix artifacts** — mis-split possessive in byline string produces
  "s Firstname Lastname".

## Negative assertions — names we explicitly decided NOT to merge

> **Robert Frank / Robert H. Frank** — different people.
> Robert H. Frank = Cornell economist/columnist; Robert Frank =
> wealth/lifestyle reporter. Do not add an override merging these.

## Per-entry notes

| Key (wrong form) | Note |
|---|---|
| `'The Ezra Klein Show'` (curly-quoted) | Two Unicode variants; both map to "Ezra Klein" |
| `Stuart Elliot` | Typo variant of the advertising columnist |
| `Andrew Kramer` | Business/oil early career → Russia/World correspondent |
| `Robert Worth` | Metro desk start → Middle East correspondent (Wikipedia) |
| `Elizabeth Harris` | Metro → Business → Culture → Books (Wikipedia) |
| `Pedro Rosado` | Same audio/video producer (MuckRack) |
| `Natalia Osipova` | Same NYT video journalist (LinkedIn) |
| `Timothy Williams` | Maps to itself — keep-as-is marker; "Timothy R. Williams" is the rare variant and a different person |
| `Deborah B. Solomon` | Shorter form "Deborah Solomon" is canonical |
| `Alan S. Blinder` | Shorter form "Alan Blinder" is canonical |

## How to add a new override

1. Add the entry to `data/author_overrides.json`.
2. If the entry is a deliberate non-merge decision, add a note to the
   **Negative assertions** section above.
3. After adding, run `python scripts/build_data.py` and verify the author's
   article count looks correct.

## Name change merges (marriage/rename)
- `Emily Baumgaertner Nunn` → `Emily Baumgaertner`: Reporter changed name; NYT created second bio page at /by/emily-baumgaertner-nunn. Use original name (bio page /by/emily-baumgaertner) as canonical. Pattern may apply to other reporters who married and changed names.
