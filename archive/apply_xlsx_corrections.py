"""Apply manual corrections from `Corrected parsed names.xlsx` to the obit dataset.

The xlsx has these flag conventions in column A (notes):
  - "Not an obituary - this is a list..." → drop the entry (REMOVE)
  - "Broken link..." → drop the entry (REMOVE)
  - "Y - link to two entries for X (age, gen) and Y (age, gen)" → split into 2 obits

Other corrections come from the data columns:
  - Column E (Gender) = M/F/X (X = non-binary) when non-empty/non-"·"
  - Column C (Name), F (Role), or D (Age) prefixed with "Correction:" / "CORRECTION:"
    → strip the prefix and apply

Outputs:
  1. Patches `data/obituaries.json` in place (so changes show immediately)
  2. Prints Python code blocks that can be merged into `build_obituaries.py`
     under OBIT_REMOVE / OBIT_OVERRIDES / OBIT_SPLITS for durability across
     full rebuilds.
"""
import json
import re
import unicodedata
import openpyxl


XLSX_PATH = '/Users/tedalcorn/Desktop/Corrected parsed names.xlsx'
OBITS_PATH = 'data/obituaries.json'


def fuzzy_keys(name, date):
    if not name:
        return set()
    name = name.strip()
    base = name.lower()
    keys = {(date, base)}
    norm = unicodedata.normalize('NFKD', base).encode('ascii', 'ignore').decode()
    keys.add((date, norm))
    for h in ('dr.', 'mr.', 'mrs.', 'ms.', 'prof.', 'col.', 'gen.', 'sister',
              'father', 'rev.', 'rabbi', 'lady', 'lord', 'sir', 'cardinal'):
        if base.startswith(h + ' '):
            keys.add((date, base[len(h)+1:]))
            keys.add((date, norm[len(h)+1:]))
    keys.add((date, base.replace("'", '').replace('\u2019', '')))
    keys.add((date, norm.replace("'", '').replace('\u2019', '')))
    m = re.match(r"^(?:iran|china|india|britain|israel|russia|japan|france|"
                 r"germany|africa|america)['\u2019]?s\s+(.+)$", base)
    if m:
        keys.add((date, m.group(1)))
    return keys


def build_index(obits):
    idx = {}
    for o in obits:
        nm = (o.get('name') or '').lower().strip()
        d = o.get('date') or ''
        if not nm:
            continue
        idx.setdefault((d, nm), o)
        norm = unicodedata.normalize('NFKD', nm).encode('ascii', 'ignore').decode()
        idx.setdefault((d, norm), o)
        idx.setdefault((d, nm.replace("'", '').replace('\u2019', '')), o)
        # Also key by fragment (last part of name) for "Crash Kills William Marrié"
        toks = nm.split()
        if len(toks) > 2:
            tail = ' '.join(toks[-2:])
            idx.setdefault((d, tail), o)
    return idx


def parse_split_note(note):
    """Parse 'Y - link to two entries for X (info) and Y (info)' format.

    Returns (entries, default_gender_to_all) where entries is a list of
    {name, age, gender} dicts. default_gender_to_all is set when the note
    ends with "both male"/"both female" — apply to any entries missing gender.
    """
    rest_m = re.search(r'(?:for|both)\s+(.+)$', note, re.I)
    if not rest_m:
        return [], None
    rest = rest_m.group(1).rstrip('.').strip()
    # Pull off trailing "both male"/"both female" — applies gender to all
    default_g = None
    g_suffix = re.search(r',?\s*both\s+(male|female)s?\.?\s*$', rest, re.I)
    if g_suffix:
        default_g = 'M' if g_suffix.group(1).lower() == 'male' else 'F'
        rest = rest[:g_suffix.start()].rstrip(', ').rstrip()
    # Split on " and " (case-insensitive)
    parts = re.split(r'\s+and\s+', rest, flags=re.I)
    if len(parts) != 2:
        return [], default_g
    out = []
    for p in parts:
        p = p.rstrip('.').strip().rstrip(',').strip()
        pm = re.match(r'^([A-Za-z][\w\s.\-\u00C0-\u017F\u2018\u2019\']+?)\s*\(([^)]+)\)\s*$', p)
        info = ''
        nm = ''
        if pm:
            nm, info = pm.group(1).strip(), pm.group(2).strip()
        else:
            cm = re.match(r'^([A-Za-z][\w\s.\-\u00C0-\u017F\u2018\u2019\']+?),\s*(.+)$', p)
            if cm:
                nm, info = cm.group(1).strip(), cm.group(2).strip()
            else:
                nm = p
        age = None
        gender = None
        am = re.search(r'\b(\d{1,3})\b', info)
        if am:
            age = int(am.group(1))
        if re.search(r'\bm(?:ale)?\b', info, re.I):
            gender = 'M'
        elif re.search(r'\bf(?:emale)?\b', info, re.I):
            gender = 'F'
        out.append({'name': nm, 'age': age, 'gender': gender})
    return out, default_g


def main():
    print('Loading xlsx + obituaries.json...')
    wb = openpyxl.load_workbook(XLSX_PATH)
    ws = wb['Sheet1']
    obits = json.load(open(OBITS_PATH))
    idx = build_index(obits)

    remove_urls = set()
    overrides = {}        # url -> {field: value}
    splits = {}           # url -> [{name, age, gender, profession}, ...]
    bulk_apply = []       # list of (date, [name1, name2], override_dict) — apply same override to N existing entries
    unmatched = []

    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        if i == 1:
            continue
        note, date, name, age, gen, role, *_ = row
        if not name and not date:
            continue
        d = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date or '')

        # Strip any "Correction:" prefix from name for matching, but track if it was there
        raw_name = (name or '').strip()
        name_corrected = raw_name.lower().startswith('correction:')
        match_name = raw_name.split(':', 1)[1].strip() if name_corrected else raw_name

        # Try to match
        cands = fuzzy_keys(match_name, d)
        found = None
        for k in cands:
            if k in idx:
                found = idx[k]
                break
        # Last-resort: same-day URL slug contains last-name token
        if not found:
            ln = match_name.split()[-1].lower() if match_name else ''
            ln_norm = unicodedata.normalize('NFKD', ln).encode('ascii', 'ignore').decode()
            for o in obits:
                if o.get('date') != d:
                    continue
                u = (o.get('url') or '').lower()
                if ln_norm and (ln_norm in u or ln in u):
                    found = o
                    break
        if not found:
            unmatched.append((i, d, raw_name))
            continue
        url = found.get('url')

        note_str = (str(note).strip() if note else '')
        note_lower = note_str.lower()

        # Removal
        if note_lower.startswith('not an obituary') or note_lower.startswith('broken link'):
            remove_urls.add(url)
            continue

        # "Y - need to be assigned to entries for X and Y, same age and gender"
        # → SPLIT the single combined obit into 2 entries; share age+gender
        # from the xlsx columns. (Note typo "Clean Boyd" should be Cleon.)
        if note_lower.startswith('y') and ('need' in note_lower) and ('assigned to' in note_lower):
            m = re.search(r'(?:for(?:\s+both)?)\s+(.+?)(?:,|$)', note_str, re.I)
            if m:
                rest = m.group(1).rstrip('.').strip()
                names = [p.strip() for p in re.split(r'\s+and\s+', rest, flags=re.I) if p.strip()]
                # Fix common spelling/typo: "Clean Boyd" in note → "Cleon Boyd"
                names = [n.replace('Clean Boyd', 'Cleon Boyd') for n in names]
                gen_str = (str(gen).strip() if gen else '')
                shared_gender = gen_str if gen_str in ('M','F','X') else None
                shared_age = age if isinstance(age, int) else None
                entries = []
                for nm in names:
                    e = {'name': nm}
                    if shared_age is not None: e['age'] = shared_age
                    if shared_gender is not None: e['gender'] = shared_gender
                    entries.append(e)
                splits[url] = entries
            else:
                print(f'  WARN: row {i}: could not parse assignment note: {note_str[:80]}')
            continue

        # Multi-subject split (single existing record → N records)
        if note_lower.startswith('y'):
            entries, default_g = parse_split_note(note_str)
            if entries:
                if default_g:
                    for e in entries:
                        if not e.get('gender'):
                            e['gender'] = default_g
                splits[url] = entries
            else:
                print(f'  WARN: row {i}: could not parse split note: {note_str[:80]}')
            continue

        # Apply M/F/X corrections + name/role/age corrections
        ov = {}
        gen_str = (str(gen).strip() if gen else '')
        if gen_str in ('M', 'F'):
            ov['gender'] = gen_str
            ov['gender_src'] = 'manual'
        elif gen_str == 'X':
            ov['gender'] = 'X'
            ov['gender_src'] = 'manual'

        if name_corrected:
            ov['name'] = match_name

        role_raw = (str(role).strip() if role else '')
        if role_raw.lower().startswith('correction:'):
            ov['profession'] = role_raw.split(':', 1)[1].strip()

        # Age correction lives in column D as "Correction: 141" string
        age_raw = age
        if isinstance(age_raw, str) and age_raw.lower().startswith('correction:'):
            try:
                ov['age'] = int(age_raw.split(':', 1)[1].strip())
            except ValueError:
                pass

        if ov:
            overrides[url] = ov

    # ---- Apply to obituaries.json ----
    print(f'\nApplying to obituaries.json:')
    print(f'  Remove: {len(remove_urls)}')
    print(f'  Overrides: {len(overrides)}')
    print(f'  Splits: {len(splits)}')
    print(f'  Unmatched xlsx rows: {len(unmatched)}')
    for u in unmatched:
        print(f'    {u}')

    # Drop removals
    new_obits = [o for o in obits if o.get('url') not in remove_urls]
    # Apply overrides
    for o in new_obits:
        url = o.get('url')
        if url in overrides:
            for k, v in overrides[url].items():
                o[k] = v
            if 'name' in overrides[url]:
                o['display_name'] = overrides[url]['name']
    # Apply bulk_apply (same override to multiple same-day entries by name match)
    for (date, names, ov) in bulk_apply:
        ln_set = {n.split()[-1].lower() for n in names if n}
        targets = [o for o in new_obits
                   if o.get('date') == date
                   and (o.get('name') or '').split()
                   and (o.get('name') or '').split()[-1].lower() in ln_set]
        for o in targets:
            for k, v in ov.items():
                o[k] = v
    # Apply splits — replace one record with N records
    if splits:
        out = []
        for o in new_obits:
            url = o.get('url')
            if url in splits:
                for s in splits[url]:
                    rec = dict(o)  # copy parent
                    rec['name'] = s['name']
                    rec['display_name'] = s['name']
                    if s.get('age') is not None:
                        rec['age'] = s['age']
                    if s.get('gender') is not None:
                        rec['gender'] = s['gender']
                        rec['gender_src'] = 'manual'
                    out.append(rec)
            else:
                out.append(o)
        new_obits = out

    print(f'  Total before: {len(obits)}, after: {len(new_obits)}')
    with open(OBITS_PATH, 'w') as f:
        json.dump(new_obits, f, separators=(',', ':'), ensure_ascii=False)
    print(f'  wrote {OBITS_PATH}')

    # ---- Emit build_obituaries.py code blocks for durability ----
    print('\n' + '='*70)
    print('Add these to build_obituaries.py to make changes survive a full rebuild:')
    print('='*70)

    print('\n# REMOVE set (drop these URLs from obits list — they are not obituaries):')
    print('OBIT_REMOVE = {')
    for u in sorted(remove_urls):
        print(f'    {u!r},')
    print('}')

    print('\n# OBIT_OVERRIDES additions:')
    for u in sorted(overrides):
        ov = overrides[u]
        ov_str = ', '.join(f'{k!r}: {v!r}' for k, v in ov.items())
        print(f'    {u!r}: {{{ov_str}}},')

    print('\n# OBIT_SPLITS additions:')
    for u in sorted(splits):
        print(f'    {u!r}: [')
        for s in splits[u]:
            parts = [f"'name': {s['name']!r}"]
            if s.get('age') is not None:
                parts.append(f"'age': {s['age']}")
            if s.get('gender') is not None:
                parts.append(f"'gender': {s['gender']!r}, 'gender_src': 'manual'")
            print(f'        {{{", ".join(parts)}}},')
        print('    ],')

    print('\n# OBIT_BULK_APPLY (date, [last names], override) — apply to all matching entries:')
    for (d, ns, ov) in bulk_apply:
        print(f'    ({d!r}, {ns!r}, {ov!r}),')


if __name__ == '__main__':
    main()
