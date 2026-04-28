"""
Surgical merge: fold the byline `'‘The Ezra Klein Show''` (curly-open + straight-close)
into `'Ezra Klein'` everywhere — both in `data/articles_YYYY.json` (the `a`
field) and `data/authors.json` (re-aggregated record + removed show entry).

build_data.py's AUTHOR_OVERRIDES now also has this mapping, so future full
builds will not re-introduce the duplicate. Idempotent.
"""
import json
import os
from collections import Counter, defaultdict
from datetime import date as date_cls
from glob import glob

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
SHOW_NAME = "‘The Ezra Klein Show'"
EZRA = 'Ezra Klein'

# Mirror of build_data.py:is_podcast_article (URL is path-only here)
_PODCAST_KICKERS = {
    'the daily', 'the ezra klein show', 'still processing', 'the run-up',
    'dear sugars', 'cannonball with wesley morris', 'the new washington',
    "tell me something i don't know", "tell me something i don’t know",
    'the modern love podcast', 'modern love podcast', 'modern love',
    'the book review podcast', 'book review podcast', 'the argument',
    'matter of opinion', 'first person', 'sway', 'hard fork',
    'popcast', 'the popcast', "the 'hard fork' podcast",
}
_PODCAST_SLUG_PATTERNS = ('ezra-klein-podcast-', 'argument-podcast-', 'matter-of-opinion-')
_INSTITUTIONAL = {
    'The New York Times', 'The Associated Press', 'The Editorial Board',
    'Reuters', 'Bloomberg', 'Agence France-Presse',
}


def is_podcast(section, url, kicker):
    if section == 'Podcasts':
        return True
    u = (url or '').lower()
    if '/podcasts/' in u:
        return True
    if u.startswith('/audio/') or 'nytimes.com/audio/' in u:
        return True
    if kicker and kicker.strip().lower() in _PODCAST_KICKERS:
        return True
    if any(p in u for p in _PODCAST_SLUG_PATTERNS):
        return True
    return False


def main():
    # ── Step 1: rewrite article files ──────────────────────────────────────────
    article_files = sorted(glob(os.path.join(DATA_DIR, 'articles_*.json')))
    print(f'Patching {len(article_files)} article files…', flush=True)
    total_replaced = 0
    for fpath in article_files:
        with open(fpath) as f:
            arts = json.load(f)
        n = 0
        for a in arts:
            bys = a.get('a') or []
            if SHOW_NAME in bys:
                # Show byline is solo in every observed case, but be defensive:
                # replace and dedupe while preserving order.
                new_bys, seen = [], set()
                for b in bys:
                    repl = EZRA if b == SHOW_NAME else b
                    if repl not in seen:
                        new_bys.append(repl); seen.add(repl)
                a['a'] = new_bys
                n += 1
        if n:
            with open(fpath, 'w') as f:
                json.dump(arts, f, separators=(',', ':'))
            print(f'  {os.path.basename(fpath)}: replaced {n}', flush=True)
            total_replaced += n
    print(f'Total article-byline replacements: {total_replaced}', flush=True)

    # ── Step 2: re-aggregate Ezra Klein from patched articles ──────────────────
    print('Re-aggregating Ezra Klein record from articles…', flush=True)
    article_count = 0
    total_words = 0
    annual_words   = defaultdict(int)
    annual_pod_words = defaultdict(int)
    annual_pod_counts = defaultdict(int)
    annual_sections = defaultdict(Counter)
    monthly_counts = defaultdict(int)
    monthly_shared_counts = defaultdict(int)
    sections_ctr = Counter()
    coauthors = Counter()
    zero_word_articles = 0
    solo_text_articles = 0
    shared_byline_count = 0
    wc_hist = [0] * 21
    first_date, last_date = None, None
    years_set = set()

    for fpath in article_files:
        with open(fpath) as f:
            arts = json.load(f)
        for a in arts:
            bys = a.get('a') or []
            if EZRA not in bys:
                continue
            article_count += 1
            human = [b for b in bys if b not in _INSTITUTIONAL]
            n_authors = len(human) or 1
            wc = a.get('w') or 0
            per_author = wc // n_authors if n_authors else 0
            total_words += per_author
            d = a.get('d')                         # 'YYYY-MM-DD'
            ym = a.get('m') or (d[:7] if d else '')
            year = int(d[:4]) if d else None
            sec = a.get('s') or ''
            if year is not None:
                annual_words[year] += per_author
                annual_sections[year][sec] += 1
                years_set.add(year)
            sections_ctr[sec] += 1
            monthly_counts[ym] += 1
            is_shared = len([b for b in human if b != EZRA]) > 0
            if is_shared:
                shared_byline_count += 1
                monthly_shared_counts[ym] += 1
                for co in human:
                    if co != EZRA:
                        coauthors[co] += 1
            if wc == 0:
                zero_word_articles += 1
            else:
                wc_hist[min(wc // 200, 20)] += 1
            if not is_shared and wc > 200:
                solo_text_articles += 1
            if is_podcast(sec, a.get('u'), a.get('k')):
                annual_pod_counts[year] += 1
                annual_pod_words[year] += per_author
            if first_date is None or (d and d < first_date):
                first_date = d
            if last_date is None or (d and d > last_date):
                last_date = d

    years = sorted(years_set)
    print(f'  Aggregated: {article_count} articles, {total_words:,} words, span {first_date}..{last_date}', flush=True)

    # Normalize partial first/last years (mirror build_data.py)
    annual_words_norm = {}
    annual_pod_words_norm = {}
    if years and first_date and last_date:
        fd = date_cls.fromisoformat(first_date)
        ld = date_cls.fromisoformat(last_date)
        for y in years:
            raw = annual_words[y]
            if len(years) == 1:
                span_days = max((ld - fd).days + 1, 1)
                norm = round(raw * 365 / span_days)
            elif y == years[0]:
                ye = date_cls(y, 12, 31)
                active = max((ye - fd).days + 1, 1)
                norm = round(raw * 365 / active)
            elif y == years[-1]:
                ys = date_cls(y, 1, 1)
                active = max((ld - ys).days + 1, 1)
                norm = round(raw * 365 / active)
            else:
                norm = raw
            annual_words_norm[y] = norm
            raw_pod = annual_pod_words.get(y, 0)
            if raw > 0 and raw_pod > 0:
                annual_pod_words_norm[y] = round(norm * raw_pod / raw)

    # avg_words_per_year (mirrors build_data.py)
    avg_words_per_year = 0
    if first_date and last_date and total_words:
        fd = date_cls.fromisoformat(first_date)
        ld = date_cls.fromisoformat(last_date)
        span_days = max((ld - fd).days, 1)
        if span_days >= 90:
            span_years = span_days / 365.25
            avg_words_per_year = round(total_words / span_years)

    sections_ranked = sections_ctr.most_common()
    primary_section = sections_ranked[0][0] if sections_ranked else ''
    secondary_section = sections_ranked[1][0] if len(sections_ranked) > 1 else ''
    annual_primary = {y: ctr.most_common(1)[0][0] for y, ctr in annual_sections.items() if ctr}
    all_sections = sorted(set(annual_primary.values()))
    if primary_section and primary_section not in all_sections:
        all_sections.insert(0, primary_section)

    # ── Step 3: patch authors.json ─────────────────────────────────────────────
    authors_path = os.path.join(DATA_DIR, 'authors.json')
    print(f'Loading {authors_path}…', flush=True)
    with open(authors_path) as f:
        authors = json.load(f)

    ezra_idx = next((i for i, a in enumerate(authors) if a['name'] == EZRA), None)
    show_idx = next((i for i, a in enumerate(authors) if a['name'] == SHOW_NAME), None)
    if ezra_idx is None:
        raise RuntimeError(f'No record found for {EZRA!r}')

    # Carry forward beats from both records (these come from build_beats which
    # we do not re-run; union and dedupe to preserve coverage)
    existing_beats = list(authors[ezra_idx].get('beats') or [])
    if show_idx is not None:
        for b in (authors[show_idx].get('beats') or []):
            if b not in existing_beats:
                existing_beats.append(b)

    # Re-derive likely_multimedia (mirror build_data.py logic)
    shared_rate = shared_byline_count / article_count if article_count else 0
    zero_rate = zero_word_articles / article_count if article_count else 0
    avg_words = round(total_words / article_count) if article_count else 0
    has_reporting_history = solo_text_articles >= 20
    is_photo_video = article_count >= 5 and shared_rate >= 0.75 and zero_rate >= 0.35
    is_pure_zero = article_count >= 20 and zero_rate >= 0.95
    is_low_word_shared = article_count >= 5 and shared_rate >= 0.90 and avg_words < 100
    is_pod_author = article_count >= 5 and primary_section == 'Podcasts'
    is_structural = article_count >= 5 and shared_rate >= 0.90 and primary_section in ('Crosswords & Games', 'Briefing')
    likely_multimedia = (is_photo_video or is_pure_zero or is_low_word_shared or is_pod_author or is_structural) and not has_reporting_history

    new_record = {
        'name': EZRA,
        'article_count': article_count,
        'total_words': total_words,
        'avg_words': avg_words,
        'avg_words_per_year': avg_words_per_year,
        'primary_section': primary_section,
        'secondary_section': secondary_section,
        'all_sections': all_sections,
        'year_range': f'{years[0]}-{years[-1]}' if years else '',
        'first_year': years[0] if years else None,
        'last_year': years[-1] if years else None,
        'first_date': first_date,
        'last_date': last_date,
        'annual_words_norm': {str(y): v for y, v in annual_words_norm.items()},
        'annual_words': {str(y): v for y, v in annual_words.items()},
        'monthly_counts': dict(monthly_counts),
        'annual_blog_counts': {},
        'annual_blog_words_norm': {},
        'annual_podcast_counts': {str(y): v for y, v in annual_pod_counts.items() if v > 0},
        'annual_podcast_words_norm': {str(y): v for y, v in annual_pod_words_norm.items()},
        'shared_byline_count': shared_byline_count,
        'monthly_shared_counts': dict(monthly_shared_counts),
        'coauthors': dict(coauthors.most_common(10)),
        'likely_multimedia': likely_multimedia,
        'solo_text_articles': solo_text_articles,
        'wc_hist': wc_hist,
        'beats': existing_beats,
    }

    # Replace Ezra's record; remove show record
    authors[ezra_idx] = new_record
    if show_idx is not None:
        # If show_idx > ezra_idx the index is still valid post-replace; if < then drop changes
        # the indexed offset. Both cases are safe with del at original index.
        del authors[show_idx if show_idx > ezra_idx else show_idx]

    tmp = authors_path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(authors, f, separators=(',', ':'))
    os.replace(tmp, authors_path)
    print(f'Wrote {authors_path}: {len(authors):,} authors ({os.path.getsize(authors_path):,} bytes)', flush=True)

    print('\nMerged Ezra Klein record:')
    print(f'  article_count: {new_record["article_count"]}')
    print(f'  total_words:   {new_record["total_words"]:,}')
    print(f'  year_range:    {new_record["year_range"]}')
    print(f'  podcast yrs:   {new_record["annual_podcast_counts"]}')
    print(f'  pod words yrs: {new_record["annual_podcast_words_norm"]}')


if __name__ == '__main__':
    main()
