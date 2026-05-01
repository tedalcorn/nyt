"""
Surgical patch: add `podcast`, `podcast_words`, and `blog_words` fields to
each entry in `dashboard.articles_per_month`, and adjust `nonblog` so the
three count buckets (blog / podcast / nonblog) are mutually exclusive and
sum to `count`.

Detection mirrors build_data.py: `is_blog_url` keys on the host, which is
preserved in articles_*.json for the blog subdomains (dealbook.nytimes.com,
*.blogs.nytimes.com); `is_podcast` keys on section/URL/kicker. Idempotent.
"""
import json
import os
from collections import Counter
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
_PODCAST_SLUG_PATTERNS = ('ezra-klein-podcast-', 'argument-podcast-', 'matter-of-opinion-')


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


def is_blog_url(url):
    if not url:
        return False
    try:
        domain = url.split('/')[2]
    except IndexError:
        return False
    return domain.endswith('.blogs.nytimes.com') or domain == 'dealbook.nytimes.com'


def main():
    # Tally podcast and blog counts + words per month
    monthly_podcast = Counter()
    monthly_podcast_words = Counter()
    monthly_blog_words = Counter()
    files = sorted(glob(os.path.join(DATA_DIR, 'articles_*.json')))
    print(f'Scanning {len(files)} article files…', flush=True)
    total_pod = 0
    for fp in files:
        with open(fp) as f:
            arts = json.load(f)
        for a in arts:
            ym = a.get('m') or (a.get('d', '')[:7])
            if not ym:
                continue
            wc = a.get('w') or 0
            url = a.get('u')
            if is_blog_url(url):
                monthly_blog_words[ym] += wc
                continue   # blog takes precedence over podcast
            if is_podcast(a.get('s'), url, a.get('k')):
                monthly_podcast[ym] += 1
                monthly_podcast_words[ym] += wc
                total_pod += 1
    print(f'Total podcast articles: {total_pod:,} across {len(monthly_podcast)} months', flush=True)

    # Patch dashboard.articles_per_month
    dashboard_path = os.path.join(DATA_DIR, 'dashboard.json')
    with open(dashboard_path) as f:
        dashboard = json.load(f)

    apm = dashboard.get('articles_per_month') or []
    n_patched = 0
    for entry in apm:
        m = entry['month']
        pod = monthly_podcast.get(m, 0)
        # Derive nonblog from total - blog - podcast (idempotent on rerun)
        new_nonblog = max(0, entry.get('count', 0) - entry.get('blog', 0) - pod)
        entry['podcast'] = pod
        entry['podcast_words'] = monthly_podcast_words.get(m, 0)
        entry['blog_words'] = monthly_blog_words.get(m, 0)
        entry['nonblog'] = new_nonblog
        n_patched += 1

    print(f'Patched {n_patched} monthly entries', flush=True)

    tmp = dashboard_path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(dashboard, f, separators=(',', ':'))
    os.replace(tmp, dashboard_path)
    print(f'Wrote {dashboard_path} ({os.path.getsize(dashboard_path):,} bytes)', flush=True)

    # Spot-check
    sample = [e for e in apm if e['month'] in ('2016-08', '2020-04', '2024-06', '2025-12')]
    for s in sample:
        std_words = s['words'] - s.get('blog_words', 0) - s.get('podcast_words', 0)
        print(f"  {s['month']}: count={s['count']}  blog={s.get('blog',0)}  podcast={s.get('podcast',0)}  nonblog={s['nonblog']}  | words: total={s['words']:,}  blog={s.get('blog_words',0):,}  pod={s.get('podcast_words',0):,}  std={std_words:,}")


if __name__ == '__main__':
    main()
