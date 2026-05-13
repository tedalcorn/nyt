"""Generate the final methodology slide for the state-by-state Twitter thread.

Matches the visual style of the state cards (16:9, cream, serif).
Output: outputs/top-keyword/2026-05-12-us-state-tweets/-National/methodology.png (also .svg, .pdf)
"""

import os
import matplotlib.pyplot as plt

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CREAM = '#f4efe6'
INK = '#2a2a2a'
MUTED = '#7a7368'
TITLE_BLUE = '#326891'


def main():
    fig = plt.figure(figsize=(16, 9), dpi=100, facecolor=CREAM)

    # Title (mirrors the state-card title style)
    fig.text(0.5, 0.86, 'About This Analysis',
             fontsize=42, weight='bold', ha='center', family='serif', color=INK)

    # Body paragraphs — left-aligned in a center column
    BODY_X = 0.16
    BODY_W = 0.68
    body_lines = [
        ('For each state, the keywords The New York Times most',
         22, 'normal', INK),
        ('disproportionately attaches to it — versus the rate that',
         22, 'normal', INK),
        ('keyword appears nationwide.',
         22, 'normal', INK),
        ('', 10, 'normal', INK),
        ('Source: ~224,000 NYT articles tagged to a U.S. state between',
         17, 'normal', INK),
        ('2000 and 2026, from the U.S. and New York sections. The',
         17, 'normal', INK),
        ('national-average comparison draws on every NYT article in',
         17, 'normal', INK),
        ('the same window (2.16 million).',
         17, 'normal', INK),
        ('', 10, 'normal', INK),
        ('Excluded: one-time events (named storms, dated incidents,',
         17, 'normal', INK),
        ('individual party conventions), generic state-level topics,',
         17, 'normal', INK),
        ('and single-venue tags.',
         17, 'normal', INK),
    ]
    y = 0.74
    for text, fs, weight, color in body_lines:
        if text:
            fig.text(BODY_X, y, text, fontsize=fs, weight=weight,
                     family='serif', color=color, ha='left')
        y -= 0.040

    # URL plug
    fig.text(0.5, 0.16, 'See the full project at Below The Fold',
             fontsize=18, family='serif', color=MUTED, ha='center')
    fig.text(0.5, 0.085, 'tedalcorn.github.io/nyt',
             fontsize=30, weight='bold', family='serif',
             color=TITLE_BLUE, ha='center')

    # Footer credit
    fig.text(0.5, 0.035,
             'By Ted Alcorn  ·  Data from NYT Archive API',
             fontsize=12, ha='center', family='serif', color=MUTED)

    out_dir = os.path.join(PROJECT_DIR, 'outputs', 'top-keyword',
                           '2026-05-12-us-state-tweets', '-National')
    os.makedirs(out_dir, exist_ok=True)
    out_png = os.path.join(out_dir, 'methodology.png')
    plt.savefig(out_png, dpi=100, facecolor=CREAM)
    plt.savefig(out_png.replace('.png', '.svg'), facecolor=CREAM)
    plt.savefig(out_png.replace('.png', '.pdf'), facecolor=CREAM)
    plt.close()
    print(f'Saved {out_png} (+ .svg, .pdf)')


if __name__ == '__main__':
    main()
