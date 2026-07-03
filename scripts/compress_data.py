#!/usr/bin/env python3
"""Compress the large per-year data files for deployment.

The site serves ~940 MB of per-year JSON (data/articles_*.json and
data/v2/tracker_*.json). That pushed the GitHub Pages *published* tree past
its hard 1 GB limit, so the deploy step (syncing_files) began failing. We
gzip those files here; only the .gz copies are committed and deployed. The
plain .json files stay on disk (gitignored) because later build steps
(corrections, unique_reporters, validate) still read them.

The browser fetches the .gz and decompresses it via DecompressionStream —
see _fetchGzJson() in index.html.

Runs as the final step of update.py, after every step that reads the plain
.json. gzip is deterministic here (mtime=0) so a year whose data did not
change produces byte-identical output and does not churn the nightly commit.
"""
import gzip
import glob
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)

# The two big per-year families. Small data/*.json (dashboard, authors, etc.)
# stay uncompressed and are fetched normally.
PATTERNS = ["data/articles_*.json", "data/v2/tracker_*.json"]


def main():
    sources = []
    for pat in PATTERNS:
        sources.extend(sorted(glob.glob(pat)))
    if not sources:
        print("compress_data: no source files found — nothing to do")
        return

    written = unchanged = 0
    total_in = total_out = 0
    keep = set()

    for i, src in enumerate(sources, 1):
        with open(src, "rb") as f:
            raw = f.read()
        packed = gzip.compress(raw, compresslevel=9, mtime=0)
        dst = src + ".gz"
        keep.add(os.path.abspath(dst))

        old = None
        if os.path.exists(dst):
            with open(dst, "rb") as f:
                old = f.read()
        if old == packed:
            unchanged += 1
        else:
            with open(dst, "wb") as f:
                f.write(packed)
            written += 1

        total_in += len(raw)
        total_out += len(packed)
        print(
            f"  [{i}/{len(sources)}] {src}  "
            f"{len(raw)/1048576:.0f} MB -> {len(packed)/1048576:.0f} MB",
            flush=True,
        )

    # Drop stale .gz whose source .json no longer exists (e.g. a year rolls off).
    removed = 0
    for pat in PATTERNS:
        for gz in glob.glob(pat + ".gz"):
            if os.path.abspath(gz) not in keep:
                os.remove(gz)
                removed += 1

    print(
        f"\ncompress_data: {written} written, {unchanged} unchanged, "
        f"{removed} stale removed"
    )
    print(
        f"  {total_in/1048576:.0f} MB -> {total_out/1048576:.0f} MB deployed "
        f"({total_in/total_out:.1f}x smaller)"
    )


if __name__ == "__main__":
    main()
