"""Overlay hand-drawn doodle PNGs onto the master state map.

Reads outputs/doodles/placements.json (the placement registry) and
composites the listed transparent-PNG doodles onto state-map.png.
Output goes to outputs/<output>.png (default: state-map-doodled.png).

Re-runnable: every overlay re-applies cleanly to the latest base map.
Edit placements.json to nudge positions; do not edit this script.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile

try:
    from PIL import Image
except ImportError:
    Image = None

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOC_DIR = os.path.join(PROJECT_DIR, 'outputs')
DOODLE_DIR = os.path.join(DOC_DIR, 'doodles')
PLACEMENTS = os.path.join(DOODLE_DIR, 'placements.json')


def main():
    with open(PLACEMENTS) as f:
        cfg = json.load(f)

    base_path = os.path.join(DOC_DIR, cfg.get('base_map', 'state-map.png'))
    out_path = os.path.join(DOC_DIR, cfg.get('output', 'state-map-doodled.png'))

    if not os.path.exists(base_path):
        sys.exit(f'base map not found: {base_path}')

    if Image is None:
        main_imagemagick(cfg, base_path, out_path)
        return

    base = Image.open(base_path).convert('RGBA')
    W, H = base.size
    print(f'base map: {W}x{H}')

    placed = 0
    skipped = 0
    missing = []

    for d in cfg['doodles']:
        if not d.get('enabled', True):
            skipped += 1
            continue

        path = os.path.join(DOODLE_DIR, d['file'])
        if not os.path.exists(path):
            missing.append(d['file'])
            continue

        doodle = Image.open(path).convert('RGBA')

        target_w = int(round(d['width'] * W))
        scale = target_w / doodle.width
        target_h = int(round(doodle.height * scale))
        doodle = doodle.resize((target_w, target_h), Image.LANCZOS)

        rot = float(d.get('rotation', 0) or 0)
        if rot:
            doodle = doodle.rotate(rot, resample=Image.BICUBIC, expand=True)

        opacity = float(d.get('opacity', 1.0))
        if opacity < 1.0:
            r, g, b, a = doodle.split()
            a = a.point(lambda v: int(v * opacity))
            doodle = Image.merge('RGBA', (r, g, b, a))
        doodle_w, doodle_h = doodle.size

        # Figure-coord (x, y) is the doodle CENTER. Note y is from BOTTOM
        # in the JSON (matplotlib convention), so flip for PIL pixel y.
        cx_px = int(round(d['x'] * W))
        cy_px = int(round((1.0 - d['y']) * H))
        x_px = cx_px - doodle_w // 2
        y_px = cy_px - doodle_h // 2

        base.alpha_composite(doodle, dest=(x_px, y_px))
        placed += 1
        print(f'  · {d["file"]:24s} → {d["state"]:18s} '
              f'({d["x"]:.3f}, {d["y"]:.3f}) w={d["width"]:.3f}')

    base.save(out_path, optimize=True)
    print()
    print(f'placed   {placed}')
    print(f'skipped  {skipped} (enabled=false)')
    if missing:
        print(f'missing  {len(missing)} (PNG not in {DOODLE_DIR}):')
        for m in missing:
            print(f'           {m}')
    print(f'wrote    {out_path}')


def _magick_identify(path, fmt):
    return subprocess.check_output(
        ['magick', 'identify', '-format', fmt, path],
        text=True,
    ).strip()


def main_imagemagick(cfg, base_path, out_path):
    if shutil.which('magick') is None:
        sys.exit('Pillow is not installed and ImageMagick `magick` was not found.')

    W = int(_magick_identify(base_path, '%w'))
    H = int(_magick_identify(base_path, '%h'))
    print(f'base map: {W}x{H}')

    placed = 0
    skipped = 0
    missing = []

    with tempfile.TemporaryDirectory(prefix='nyt-doodles.') as tmp:
        work = os.path.join(tmp, 'work.png')
        shutil.copyfile(base_path, work)

        for d in cfg['doodles']:
            if not d.get('enabled', True):
                skipped += 1
                continue

            path = os.path.join(DOODLE_DIR, d['file'])
            if not os.path.exists(path):
                missing.append(d['file'])
                continue

            target_w = int(round(d['width'] * W))
            prepared = os.path.join(tmp, d['file'])
            subprocess.run(
                [
                    'magick',
                    path,
                    '-resize',
                    f'{target_w}x',
                    '-background',
                    'none',
                    '-virtual-pixel',
                    'transparent',
                    '-rotate',
                    str(float(d.get('rotation', 0) or 0)),
                    prepared,
                ],
                check=True,
            )

            opacity = float(d.get('opacity', 1.0))
            if opacity < 1.0:
                toned = os.path.join(tmp, f'toned-{d["file"]}')
                subprocess.run(
                    [
                        'magick',
                        prepared,
                        '-channel',
                        'A',
                        '-evaluate',
                        'multiply',
                        str(opacity),
                        '+channel',
                        toned,
                    ],
                    check=True,
                )
                prepared = toned

            doodle_w = int(_magick_identify(prepared, '%w'))
            doodle_h = int(_magick_identify(prepared, '%h'))
            x_px = int(round(d['x'] * W - doodle_w / 2))
            y_px = int(round((1.0 - d['y']) * H - doodle_h / 2))

            next_work = os.path.join(tmp, 'next.png')
            subprocess.run(
                [
                    'magick',
                    work,
                    prepared,
                    '-geometry',
                    f'+{x_px}+{y_px}',
                    '-compose',
                    'over',
                    '-composite',
                    next_work,
                ],
                check=True,
            )
            shutil.move(next_work, work)
            placed += 1
            print(f'  · {d["file"]:24s} → {d["state"]:18s} '
                  f'({d["x"]:.3f}, {d["y"]:.3f}) w={d["width"]:.3f}')

        shutil.copyfile(work, out_path)

    print()
    print(f'placed   {placed}')
    print(f'skipped  {skipped} (enabled=false)')
    if missing:
        print(f'missing  {len(missing)} (PNG not in {DOODLE_DIR}):')
        for m in missing:
            print(f'           {m}')
    print(f'wrote    {out_path}')


if __name__ == '__main__':
    main()
