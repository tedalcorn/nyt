"""
Surgical patch: add `annual_podcast_counts` and `annual_podcast_words_norm`
to each entry in `data/authors.json`, without rerunning build_data.py.

Reads the per-year article files (`data/articles_YYYY.json`), classifies each
record as a podcast via the same rule used by build_data.py:is_podcast_article
(section_name == 'Podcasts', /podcasts/ in URL, /audio/ path prefix, known
podcast kicker, or Opinion-section podcast slug). Tallies per-author per-year
podcast counts and raw words, then scales the words by each author's existing
`annual_words_norm` / `annual_words` ratio so the partial-year normalization
matches what build_data.py would have produced.

Idempotent: safe to re-run.
"""
import json
import os
import sys
from collections import defaultdict
from glob import glob

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

_PODCAST_KICKERS = {
    'the daily', 'the ezra klein show', 'still processing', 'the run-up',
    'dear sugars', 'cannonball with wesley morris', 'the new washington',
    "tell me something i don't know", "tell me something i don’t know",
    'the modern love podcast', 'modern love podcast', 'modern love',
    'the book review podcast', 'book review podcast', 'the argument',
    'matter of opinion', 'first person', 'sway', 'hard fork',
    'popcast', 'the popcast', "the 'hard fork' podcast",
}
_PODCAST_SLUG_PATTERNS = (
    'ezra-klein-podcast-', 'argument-podcast-', 'matter-of-opinion-',
)
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
    authors_path = os.path.join(DATA_DIR, 'authors.json')
    print(f'Loading {authors_path}…', flush=True)
    with open(authors_path) as f:
        authors = json.load(f)
    print(f'  {len(authors):,} authors', flush=True)

    # Walk all article files, accumulating per-author per-year podcast tallies
    pod_counts = defaultdict(lambda: defaultdict(int))   # name -> year(int) -> count
    pod_words  = defaultdict(lambda: defaultdict(int))   # name -> year(int) -> raw words

    article_files = sorted(glob(os.path.join(DATA_DIR, 'articles_*.json')))
    print(f'Scanning {len(article_files)} article files…', flush=True)
    n_pod_articles = 0
    for fpath in article_files:
        year = int(os.path.basename(fpath).split('_')[1][:4])
        with open(fpath) as f:
            arts = json.load(f)
        for a in arts:
            if not is_podcast(a.get('s'), a.get('u'), a.get('k')):
                continue
            n_pod_articles += 1
            authors_list = a.get('a') or []
            human = [x for x in authors_list if x not in _INSTITUTIONAL]
            n = len(human) or 1
            per_author_words = (a.get('w') or 0) // n if n else 0
            for name in authors_list:
                pod_counts[name][year] += 1
                pod_words[name][year]  += per_author_words
        print(f'  {os.path.basename(fpath)}: {len(arts):,} records', flush=True)
    print(f'Total podcast articles flagged: {n_pod_articles:,}', flush=True)
    print(f'Authors with at least one podcast: {len(pod_counts):,}', flush=True)

    # Patch each author record
    n_updated = 0
    for a in authors:
        name = a['name']
        # Use string years to match the existing annual_words_norm key style
        counts = {str(y): c for y, c in pod_counts.get(name, {}).items() if c > 0}
        if not counts:
            # Ensure stale fields are cleared if a previous run added them
            if a.get('annual_podcast_counts') or a.get('annual_podcast_words_norm'):
                a['annual_podcast_counts'] = {}
                a['annual_podcast_words_norm'] = {}
                n_updated += 1
            continue

        # Scale podcast raw words by the same factor used to normalize annual_words
        # (annual_words_norm[y] / annual_words[y]) so partial-year scaling matches
        annual_words = a.get('annual_words', {}) or {}
        annual_norm  = a.get('annual_words_norm', {}) or {}
        words_norm = {}
        raw_words = pod_words.get(name, {})
        for y_str, c in counts.items():
            # annual_words keys are int in some pipelines, str in others - try both
            raw_total = annual_words.get(y_str) or annual_words.get(int(y_str)) or 0
            norm_total = annual_norm.get(y_str) or annual_norm.get(int(y_str)) or 0
            raw_pod = raw_words.get(int(y_str), 0)
            if raw_total > 0 and norm_total > 0 and raw_pod > 0:
                words_norm[y_str] = round(norm_total * raw_pod / raw_total)

        a['annual_podcast_counts'] = counts
        a['annual_podcast_words_norm'] = words_norm
        n_updated += 1

    print(f'Patched {n_updated:,} author records', flush=True)

    # Compact JSON write (matches build_data.py style)
    tmp_path = authors_path + '.tmp'
    with open(tmp_path, 'w') as f:
        json.dump(authors, f, separators=(',', ':'))
    os.replace(tmp_path, authors_path)
    print(f'Wrote {authors_path} ({os.path.getsize(authors_path):,} bytes)', flush=True)

    # Spot-check Ezra Klein
    for a in authors:
        if a['name'] == 'Ezra Klein':
            print('\nSpot-check — Ezra Klein:')
            print('  annual_podcast_counts:', a.get('annual_podcast_counts'))
            print('  annual_podcast_words_norm:', a.get('annual_podcast_words_norm'))
            break


if __name__ == '__main__':
    main()
