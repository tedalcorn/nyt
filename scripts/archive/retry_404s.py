"""
Retry 404 entries in author_bios.json with alternative URL slugs.

Key lessons from manual lookups:
  - NYT KEEPS Jr/Sr in slugs (donald-g-mcneil-jr, james-c-mckinley-jr)
  - O' names: NYT is inconsistent — try both o-[rest] and o[rest]
  - Middle-initial drop rarely helps for early-2000s reporters

Variants tried per name:
  1. Current slug (fixes stale pre-fix URLs)
  2. Add -jr suffix (catches Jr. reporters whose bylines omit Jr.)
  3. O' names: swap between o-[rest] and o[rest] forms
  4. Drop middle initial (lower-priority fallback)

Run:  python3 retry_404s.py
"""
import json, re, time, sys
from scrape_bios import name_to_slug, RESULTS_FILE, session, extract_body_text
from scrape_bios import STAFF_RE, FREELANCE_RE, PHOTO_RE

DELAY = 1.2

def slug_variants(name: str, existing_slug: str) -> list:
    base = name_to_slug(name)
    variants = []

    # Always try current slug function output (may differ from stale stored URL)
    if base != existing_slug:
        variants.append(base)

    # Jr variant: try appending -jr (NYT keeps Jr in slug but bylines often omit it)
    if not base.endswith('-jr') and not base.endswith('-sr'):
        variants.append(base + '-jr')

    # O' names: try swapping between o-X and oX forms
    # Only operate on the O' portion specifically
    if re.search(r"\bO'", name):
        # If current slug has o- after a word boundary, try without hyphen
        v1 = re.sub(r'(^|-)o-([a-z])', lambda m: m.group(1) + 'o' + m.group(2), base)
        # If current slug has o immediately followed by letter, try with hyphen
        # Only for the Irish/Scottish O prefix: word-boundary 'o' + consonant (not common words)
        v2 = re.sub(r'(^|-)o([bcdfghjklmnpqrstvwxyz])', lambda m: m.group(1) + 'o-' + m.group(2), base)
        for v in (v1, v2):
            if v != base and v not in variants:
                variants.append(v)
        # Also try -jr variants of O' slugs
        for v in (v1, v2):
            jrv = v + '-jr'
            if jrv not in variants:
                variants.append(jrv)

    # Middle-initial drop (lower priority — rarely helps but worth trying)
    parts = name.split()
    if len(parts) >= 3:
        for i, p in enumerate(parts):
            clean = p.rstrip('.')
            if len(clean) == 1 and i not in (0, len(parts) - 1):
                reduced = parts[:i] + parts[i + 1:]
                v = name_to_slug(' '.join(reduced))
                if v not in variants and v != existing_slug:
                    variants.append(v)
                # Also try without-initial + jr
                jrv = v + '-jr'
                if jrv not in variants:
                    variants.append(jrv)

    return variants


def _check_url(name: str, url: str) -> dict:
    result = {'name': name, 'url': url, 'exists': False,
              'is_staff': False, 'is_freelance': False, 'is_photographer': False,
              'staff_phrase': None, 'freelance_phrase': None, 'bio_text': None}
    try:
        resp = session.get(url, timeout=12, allow_redirects=True)
        if resp.status_code == 200:
            result['exists'] = True
            html = resp.text
            body_text = extract_body_text(html)
            desc_m = re.search(r'<meta[^>]+name="description"[^>]+content="([^"]{10,})"', html)
            meta_text = desc_m.group(1).strip() if desc_m else ''
            full_text = body_text + ' ' + meta_text
            is_generic = not body_text and bool(re.match(r'Recent and archived work by', meta_text, re.I))
            result['generic_page'] = is_generic
            result['has_custom_bio'] = bool(body_text)
            result['bio_text'] = body_text[:800] if body_text else meta_text[:400]
            m = FREELANCE_RE.search(full_text)
            if m:
                result['is_freelance'] = True
                result['freelance_phrase'] = m.group(0)[:120]
            if not result['is_freelance']:
                m = STAFF_RE.search(full_text)
                if m:
                    result['is_staff'] = True
                    result['staff_phrase'] = m.group(0)[:120]
            m = PHOTO_RE.search(full_text)
            if m:
                result['is_photographer'] = True
        elif resp.status_code != 404:
            result['http_status'] = resp.status_code
    except Exception as e:
        result['error'] = str(e)[:100]
    return result


if __name__ == '__main__':
    results = json.load(open(RESULTS_FILE))
    authors = {a['name']: a for a in json.load(open('data/authors.json'))}

    no_page = [
        (name, r) for name, r in results.items()
        if not r.get('exists') and not r.get('error')
    ]
    no_page.sort(key=lambda x: -(authors.get(x[0], {}).get('article_count', 0)))

    print(f"404 entries to retry: {len(no_page):,}")
    print("Press Ctrl-C at any time — progress is saved.\n")

    fixed = 0
    try:
        for i, (name, r) in enumerate(no_page):
            existing_url = r.get('url', '')
            existing_slug = existing_url.split('/')[-1]
            variants = slug_variants(name, existing_slug)
            if not variants:
                continue

            ac = authors.get(name, {}).get('article_count', 0)
            found = None
            tried = set()
            for slug in variants:
                url = f"https://www.nytimes.com/by/{slug}"
                if url in tried:
                    continue
                tried.add(url)
                res = _check_url(name, url)
                time.sleep(DELAY)
                if res.get('exists'):
                    found = res
                    break

            if found:
                results[name] = found
                fixed += 1
                tag = '[STAFF]' if found.get('is_staff') else '[custom]' if found.get('has_custom_bio') else '[generic]'
                print(f"✓ {name:35s} ({ac:4})  {tag}  → {found['url'].split('/')[-1]}")

            if (i + 1) % 20 == 0:
                json.dump(results, open(RESULTS_FILE, 'w'), indent=1)
                print(f"  [saved — {i+1}/{len(no_page)} checked, {fixed} fixed]")

    except KeyboardInterrupt:
        print('\nInterrupted — saving...')

    json.dump(results, open(RESULTS_FILE, 'w'), indent=1)
    print(f"\nDone. Fixed {fixed} previously-404 entries.")
