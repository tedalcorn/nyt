#!/usr/bin/env python3
"""Build packed per-year tracker files (v2) from the existing tracker_{year}.json.

Old format: list of {h, m, ph?, fp?} objects, one per article (2.1M total).
New format (data/v2/tracker_{year}.json): one object per year —
    {
      "year":   2024,
      "n":      48693,            # line count == old record count (asserted)
      "months": ["2024-01", ...], # distinct months present, sorted
      "mi":     [0, 0, 1, ...],   # per-line index into months
      "h":      "headline\nheadline\n...",   # n lines, web headlines
                                  # (lines containing literal newlines are
                                  #  sanitized here and carried in "exc")
      "exc":    {"91466": "TV's, DVD's: All Yours,\nbut First Do the Math"},
                                  # line -> ORIGINAL web headline, for the ~0.1%
                                  # of headlines that embed \n or \r. The client
                                  # must skip these lines in the packed scan and
                                  # regex-test the original strings individually,
                                  # reproducing the old per-record behavior exactly.
      "phd":    {"0": "Print Hed", ...},     # line -> print headline VERBATIM
                                             #   (may contain newlines), ONLY when
                                             #   present and != web headline.
                                             #   Effective print headline is
                                             #   phd[i] ?? original h (matches the
                                             #   old client's `ph || h`).
      "fp":     [12, 87, ...]     # line indices of front-page (A1) articles
    }

The headline strings are byte-identical to the old files — only the packaging
changes. Invariants asserted per year by full re-expansion (every record, not
a sample): line count, months, fp flags, original web headline, and effective
print headline all round-trip exactly.
"""
import json
import re
import sys
from pathlib import Path

SITE = Path(__file__).resolve().parent.parent
DATA = SITE / 'data'
OUT = DATA / 'v2'
OUT.mkdir(exist_ok=True)

NEWLINES = re.compile(r'[\r\n]')


def pack_year(year: int) -> dict:
    src = DATA / f'tracker_{year}.json'
    recs = json.loads(src.read_text())

    months = sorted({r['m'] for r in recs})
    midx = {m: i for i, m in enumerate(months)}

    h_lines, mi, exc, phd, fp = [], [], {}, {}, []
    for i, r in enumerate(recs):
        h = r['h']
        ph = r.get('ph', '')
        if NEWLINES.search(h):
            exc[str(i)] = h
            h = NEWLINES.sub(' ', h)
        h_lines.append(h)
        mi.append(midx[r['m']])
        if ph and ph != r['h']:
            phd[str(i)] = ph  # verbatim, newlines and all
        if r.get('fp'):
            fp.append(i)

    packed = {
        'year': year,
        'n': len(recs),
        'months': months,
        'mi': mi,
        'h': '\n'.join(h_lines),
        'exc': exc,
        'phd': phd,
        'fp': fp,
    }

    # Full round-trip verification against the source records.
    re_lines = packed['h'].split('\n')
    assert len(re_lines) == len(recs) == packed['n'], f'{year}: line count mismatch'
    fpset = set(packed['fp'])
    for i, r in enumerate(recs):
        original_h = packed['exc'].get(str(i), re_lines[i])
        assert original_h == r['h'], f'{year} line {i}: headline mismatch'
        if str(i) not in packed['exc']:
            assert '\n' not in re_lines[i] and '\r' not in re_lines[i]
        assert packed['months'][packed['mi'][i]] == r['m'], f'{year} line {i}: month mismatch'
        assert (i in fpset) == bool(r.get('fp')), f'{year} line {i}: fp mismatch'
        old_effective_print = r.get('ph') or r['h']
        new_effective_print = packed['phd'].get(str(i), original_h)
        assert new_effective_print == old_effective_print, f'{year} line {i}: print headline mismatch'
    return packed


def main():
    years = sorted(int(p.stem.split('_')[1]) for p in DATA.glob('tracker_*.json'))
    total_old = total_new = total_exc = 0
    for y in years:
        packed = pack_year(y)
        out = OUT / f'tracker_{y}.json'
        out.write_text(json.dumps(packed, separators=(',', ':'), ensure_ascii=False))
        old_sz = (DATA / f'tracker_{y}.json').stat().st_size
        new_sz = out.stat().st_size
        total_old += old_sz
        total_new += new_sz
        total_exc += len(packed['exc'])
        print(f'{y}: {packed["n"]:>6} lines  {old_sz/1e6:6.1f}MB -> {new_sz/1e6:5.1f}MB  '
              f'(months={len(packed["months"])}, fp={len(packed["fp"])}, '
              f'phd={len(packed["phd"])}, exc={len(packed["exc"])})',
              flush=True)
    print(f'\nTOTAL: {total_old/1e6:.0f}MB -> {total_new/1e6:.0f}MB '
          f'({total_new/total_old*100:.0f}%) across {len(years)} years, '
          f'{total_exc} newline-exception headlines')


if __name__ == '__main__':
    sys.exit(main())
