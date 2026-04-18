"""
Scrape NYT author bio pages to help classify staff vs. freelancers.

For each author with >= MIN_ARTICLES, constructs the likely nytimes.com/by/[slug] URL,
checks whether it exists, and if so scrapes the page text for staff indicators.

Results saved to data/author_bios.json (resumable — skips already-checked authors).
Run:  python3 scrape_bios.py
Stop: Ctrl-C at any time; progress is saved after each author.
"""

import json, re, time, unicodedata, os, sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

MIN_ARTICLES = 5          # only check authors with this many articles total
DELAY = 1.5               # seconds between requests (be polite)
RESULTS_FILE = 'data/author_bios.json'

# ── Staff indicator phrases found on NYT bio pages ───────────────────────────
TIMES_REF = r'(?:(?:the )?new york times?|n\.y\.t\.|nyt|the times?|times)'
_VERB = r'(?:is|was|has been|have been)'
# Expanded role list
_ROLE = (r'(?:reporter|correspondent|columnist|editor|photographer|writer|producer|'
         r'critic|contributor|bureau chief|investigative reporter|senior writer|'
         r'staff writer|culture reporter|science reporter|political reporter|'
         r'obituar[yi]|senior editor|deputy editor|opinion writer|op-ed|'
         r'deputy bureau chief|culture desk|metro reporter|'
         r'video journalist|visual journalist|photojournalist|features writer|'
         r'news assistant|news editor|managing editor|executive editor|'
         r'graphics editor|data reporter|audio producer|podcast host|'
         r'international correspondent|foreign correspondent|'
         r'journalist|publisher|art director|newsletter editor|newsletter host)')
STAFF_PATTERNS = [
    # Third-person: "is/was/has been a [role] ... Times"
    r'\b' + _VERB + r' an? (?:former |veteran |longtime |long-?time |senior )?' + _ROLE + r'.{0,120}' + TIMES_REF,
    # Third-person: "is a Times reporter"
    r'\b' + _VERB + r' an? (?:former |veteran |longtime )?' + TIMES_REF + r'[\s]+' + _ROLE,
    # First-person: "I am a reporter/correspondent ... Times"
    r"\bI(?:[\u2019']m| am) an? (?:former |veteran |longtime |senior )?" + _ROLE + r'.{0,250}' + TIMES_REF,
    # First-person: "I am a Times reporter" or "I'm a New York Times [modifier] reporter"
    r"\bI(?:[\u2019']m| am) an? (?:former |)?" + TIMES_REF + r'[\s\w]* ' + _ROLE,
    # First-person: "I am a [role] for/at/in The Times"
    r"\bI(?:[\u2019']m| am) an? (?:former |veteran |longtime |senior )?" + _ROLE + r' (?:for|at|in) ' + TIMES_REF,
    # First-person: "I am the [role]..." or "I am deputy editor..." (definite article or no article)
    r"\bI(?:[\u2019']m| am) (?:the |an? )?(?:[\w-]+ ){0,4}" + _ROLE + r'.{0,250}' + TIMES_REF,
    # First-person: any verb + Times ref (broad catch-all for unusual phrasing)
    r"\bI (?:cover|covered|write|wrote|report|reported|oversee|manage|lead|led|run|work|conduct|host|produce|edit|create).{0,200} (?:at|for|of) " + TIMES_REF,
    # "My job ... TIMES_REF" (Suzanne Daley: "My job in the newsroom is to oversee The New York Times International Edition")
    r"\bMy job .{0,60}" + TIMES_REF,
    # "I lead/am a member of/am on The New York Times's [team]"
    r"\bI (?:lead|am) .{0,60}" + TIMES_REF,
    # Times ref followed by a role word (catches "New York Times bureau chief in Beirut")
    TIMES_REF + r'[^.!?]{0,60}\b(?:reporter|journalist|editor|bureau chief|chief|correspondent|columnist|critic|host|producer|researcher|fellow|designer|photographer|team)',
    # Third-person with Times ref followed by a role (catches "Kayla Guo covered ... for The NYT as a fellow")
    r'(?:covered|writes|worked|reported|was).{0,80}' + TIMES_REF + r'.{0,80}\b(?:reporter|journalist|fellow|class|program|correspondent|investigative)',
    # First-person: "I am part of The Times"
    r"\bI(?:[\u2019']m| am) part of " + TIMES_REF,
    # First-person: "I work as ... at/for The Times"
    r"\bI work(?:ed)? as .{0,100} (?:at|for) " + TIMES_REF,
    # First-person career: "I've been a Times reporter", "I joined The Times"
    r"\bI(?:[\u2019']ve| have) been .{0,80}" + TIMES_REF,
    r"\bI joined " + TIMES_REF,
    # "[role] for The Times" (any person)
    _ROLE + r' for ' + TIMES_REF,
    # "bureau chief of The Times"
    r'bureau chief of ' + TIMES_REF,
    # "has been writing/working for The Times"
    r'has been (?:writing|working|reporting) for ' + TIMES_REF,
    # "joined The Times" / "has worked at The Times"
    r'joined ' + TIMES_REF,
    r'has worked at ' + TIMES_REF,
    # "covering X for The Times"
    r'covering .{3,80} for ' + TIMES_REF,
    # "[X] at The Times"
    _ROLE + r' (?:at|on) ' + TIMES_REF,
    # "staff reporter/writer"
    r'staff (?:reporter|writer|correspondent|photographer|editor)',
    # "writes the X column"
    r'writes (?:the |a )?.{2,40}column',
]
STAFF_RE = re.compile('|'.join(STAFF_PATTERNS), re.IGNORECASE)

# ── Freelance indicator phrases ───────────────────────────────────────────────
FREELANCE_PATTERNS = [
    r'\bfreelance (?:contributor|journalist|writer|reporter|photographer|videographer)\b',
    r'\bfreelancer\b',
    r'\bcontributing writer\b',
    r'\bregular (?:freelance |)contributor\b',
    r'\bindependent journalist\b',
]
FREELANCE_RE = re.compile('|'.join(FREELANCE_PATTERNS), re.IGNORECASE)

# Sentence-level past-tense freelance detector — used to veto a freelance match
_PAST_FREELANCE_RE = re.compile(
    r'\b(?:was|were|had been|formerly|previously)\b.{0,50}freelance', re.IGNORECASE
)

def is_freelance(text):
    """Return True only if text has a present-tense freelance signal with no staff override."""
    m = FREELANCE_RE.search(text)
    if not m:
        return False, None
    # Veto if the match appears within a past-tense context
    start = max(0, m.start() - 60)
    context = text[start:m.end()]
    if _PAST_FREELANCE_RE.search(context):
        return False, None
    return True, m.group(0)[:120]

# ── Photographer indicator phrases ────────────────────────────────────────────
PHOTO_PATTERNS = [
    # Self-identification as photographer (first or third person)
    r"\bI(?:'m| am) a(?:n)? (?:staff |freelance |photojournalist|)photographer\b",
    r'\bI (?:shoot|photograph) for\b',
    r'\bstaff photographer\b',
    r'\bphotojournalist\b',
    r'\bI am a visual journalist\b',
    r'\bphotographer (?:for|at|with) (?:the )?(?:new york times?|times?)\b',
    r'\bphotographer and (?:writer|editor|reporter)\b',
]
PHOTO_RE = re.compile('|'.join(PHOTO_PATTERNS), re.IGNORECASE)

# ── Manual staff overrides (NYT mention beyond 800-char bio_text truncation) ──
# These authors' bios confirm NYT employment but the mention falls after the stored excerpt.
MANUAL_STAFF_OVERRIDES = {
    'Audra D. S. Burch',   # "national reporter... race and identity" — NYT appears later in bio
    'Carlos Lozada',       # Opinion columnist — NYT appears later in bio
    'Jesse Drucker',       # "investigative reporter for the Business section" — NYT later in bio
    'John Branch',         # long-time NYT staff writer — NYT appears later in bio
    'Lisa Miller',         # "domestic correspondent for the Well section" — NYT later in bio
    'Melissa Kirsch',      # hosts The Good List + The Morning newsletters — NYT appears after truncation
}

# ── Manual URL overrides (name changes, unusual slugs, etc.) ─────────────────
MANUAL_URL_OVERRIDES = {
    # Jodi Wilgoren married Gary Ruderman in 2004 and now bylines as Jodi Rudoren
    'Jodi Wilgoren': 'https://www.nytimes.com/by/jodi-rudoren',
}

# ── URL slug construction ─────────────────────────────────────────────────────
def name_to_slug(name: str) -> str:
    """Convert 'Michael S. Schmidt' → 'michael-s-schmidt',
       'C.J. Chivers' → 'c-j-chivers'."""
    # Normalize unicode (é → e, etc.)
    name = unicodedata.normalize('NFKD', name)
    name = name.encode('ascii', 'ignore').decode('ascii')
    # Remove suffixes and honorifics
    name = re.sub(r'\b(Jr\.?|Sr\.?|II+|III+|IV|Esq\.?)\b', '', name, flags=re.IGNORECASE)
    # Convert initials: a period between/after single letters becomes a hyphen
    # "C.J." → "C-J", "A.O." → "A-O"
    name = re.sub(r'(?<=\b[A-Za-z])\.(?=[A-Za-z]\b)', '-', name)
    # Remove remaining periods
    name = name.replace('.', '')
    # Convert Irish/Scottish O' prefix: NYT is inconsistent (o-reilly vs oreilly),
    # so we try o- form as default; caller can retry without hyphen if this 404s.
    name = re.sub(r"\bO[\u2019']([A-Z])", r'O-\1', name)
    # Mid-word apostrophes (e.g. Dell'Antonia → DellAntonia): remove without space
    name = re.sub(r"(?<=[a-zA-Z])'(?=[a-zA-Z])", '', name)
    # Replace remaining non-alphanumeric (except hyphen) with space
    name = re.sub(r"[^a-zA-Z0-9 \-]", ' ', name)
    # Collapse spaces, strip
    name = re.sub(r'\s+', ' ', name).strip()
    # Lowercase and hyphenate spaces
    slug = name.lower().replace(' ', '-')
    # Remove double hyphens
    slug = re.sub(r'-+', '-', slug).strip('-')
    return slug

def build_url(name: str) -> str:
    if name in MANUAL_URL_OVERRIDES:
        return MANUAL_URL_OVERRIDES[name]
    return f"https://www.nytimes.com/by/{name_to_slug(name)}"

# ── HTTP session ──────────────────────────────────────────────────────────────
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
})
retry = Retry(total=2, backoff_factor=1, status_forcelist=[429, 500, 502, 503])
session.mount('https://', HTTPAdapter(max_retries=retry))

def extract_body_text(html: str) -> str:
    """Extract bio body text from window.__preloadedData TextInline nodes."""
    scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
    preload = next((s for s in scripts if '__preloadedData' in s), '')
    if not preload:
        return ''
    nodes = re.findall(
        r'"__typename"\s*:\s*"TextInline"\s*,\s*"text"\s*:\s*"((?:[^"\\]|\\.)*)"',
        preload
    )
    parts = []
    for n in nodes:
        decoded = (n.replace('\\u002F', '/').replace('\\u0026', '&')
                    .replace('\\u00a0', ' ').replace('\\"', '"')
                    .replace('\\n', ' ').replace('\\t', ' '))
        parts.append(decoded)
    return ' '.join(parts)

def check_bio(name: str) -> dict:
    """Check whether the bio page exists and scrape body + meta text."""
    url = build_url(name)
    result = {'name': name, 'url': url, 'slug': name_to_slug(name),
              'exists': False, 'is_staff': False, 'is_freelance': False,
              'is_photographer': False,
              'staff_phrase': None, 'freelance_phrase': None, 'bio_text': None}
    try:
        resp = session.get(url, timeout=12, allow_redirects=True)
        if resp.status_code == 200:
            result['exists'] = True
            html = resp.text

            # Primary: body text from __preloadedData TextInline nodes
            body_text = extract_body_text(html)

            # Fallback: meta description (catches staff bios that also go in meta)
            desc_m = re.search(r'<meta[^>]+name="description"[^>]+content="([^"]{10,})"', html)
            meta_text = desc_m.group(1).strip() if desc_m else ''

            # Combined text for detection (body first — it's richer)
            full_text = body_text + ' ' + meta_text

            is_generic = (not body_text and
                          bool(re.match(r'Recent and archived work by', meta_text, re.I)))
            result['generic_page'] = is_generic
            result['has_custom_bio'] = bool(body_text)
            result['bio_text'] = (body_text[:800] if body_text
                                  else meta_text[:400])

            # Freelance detection — past-tense mentions ("was a freelance writer") don't count
            _is_free, _free_phrase = is_freelance(full_text)
            if _is_free:
                result['is_freelance'] = True
                result['freelance_phrase'] = _free_phrase

            # Photographer detection (independent of staff/freelance)
            m = PHOTO_RE.search(full_text)
            if m:
                result['is_photographer'] = True

            # Staff detection (skip if already confirmed freelance)
            if not result['is_freelance']:
                m = STAFF_RE.search(full_text)
                if m:
                    result['is_staff'] = True
                    result['staff_phrase'] = m.group(0)[:120]

        elif resp.status_code == 404:
            result['exists'] = False
        else:
            result['http_status'] = resp.status_code
    except Exception as e:
        result['error'] = str(e)[:100]
    return result

# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    authors = json.load(open('data/authors.json'))
    candidates = [a for a in authors if a['article_count'] >= MIN_ARTICLES]
    candidates.sort(key=lambda a: -a['article_count'])
    print(f'Candidates (>= {MIN_ARTICLES} articles): {len(candidates):,}')

    # Load existing results
    if os.path.exists(RESULTS_FILE):
        results = json.load(open(RESULTS_FILE))
    else:
        results = {}

    # "done" means: either we've confirmed it doesn't exist (exists=False),
    # or it exists AND we've already scraped bio text (bio_text is not None).
    # Pre-seeded entries have exists=True but bio_text=None — they need a scrape pass.
    already_done = {
        name for name, r in results.items()
        if r.get('exists') is False          # confirmed 404 — no point retrying
        or r.get('bio_text') is not None     # already have bio text
        or r.get('error')                    # network error — skip for now
    }
    todo = [a for a in candidates if a['name'] not in already_done]
    needs_bio_scrape = sum(1 for a in candidates
                          if a['name'] in results
                          and results[a['name']].get('exists') is True
                          and results[a['name']].get('bio_text') is None)
    print(f'Already fully checked: {len(already_done):,}  |  Need bio scrape: {needs_bio_scrape:,}  |  New to check: {len(todo)-needs_bio_scrape:,}')
    print('Press Ctrl-C at any time — progress is saved.\n')

    for i, a in enumerate(todo):
        name = a['name']
        try:
            r = check_bio(name)
            status = '✓ EXISTS' if r['exists'] else '  404   '
            if r.get('is_staff'):        tag = ' [STAFF]  '
            elif r.get('has_custom_bio'): tag = ' [custom] '
            elif r.get('generic_page'):  tag = ' [generic]'
            else:                        tag = '          '
            print(f'[{len(already_done)+i+1:5d}/{len(candidates):,}] {status}{tag}  {name}  → {r["url"]}')
            if r.get('bio_text'):
                print(f'         "{r["bio_text"][:80]}"')
            # If re-scraping a pre-seeded entry, use the pre-seeded URL
            # (it was verified valid; our generated URL might differ for edge cases)
            if name in results and results[name].get('source','').startswith('old_data'):
                r['url'] = results[name]['url']
                r['source'] = results[name]['source']
                if results[name].get('slug_note'):
                    r['slug_note'] = results[name]['slug_note']
            results[name] = r
            # Save every 10 authors
            if (i + 1) % 10 == 0:
                json.dump(results, open(RESULTS_FILE, 'w'), indent=1)
        except KeyboardInterrupt:
            print('\n\nInterrupted — saving progress...')
            json.dump(results, open(RESULTS_FILE, 'w'), indent=1)
            sys.exit(0)
        time.sleep(DELAY)

    json.dump(results, open(RESULTS_FILE, 'w'), indent=1)
    print(f'\nDone. Results in {RESULTS_FILE}')

    # Quick summary
    res = list(results.values())
    n_exist       = sum(1 for r in res if r.get('exists'))
    n_staff       = sum(1 for r in res if r.get('is_staff'))
    n_custom      = sum(1 for r in res if r.get('has_custom_bio') and not r.get('is_staff'))
    n_generic     = sum(1 for r in res if r.get('generic_page'))
    n_no_page     = sum(1 for r in res if not r.get('exists') and not r.get('error'))
    print(f'\nSummary:')
    print(f'  Bio page exists:            {n_exist:,}')
    print(f'  Confirmed staff (regex):    {n_staff:,}')
    print(f'  Custom bio (likely staff):  {n_custom:,}')
    print(f'  Generic page (contributor): {n_generic:,}')
    print(f'  No page found (404):        {n_no_page:,}')
