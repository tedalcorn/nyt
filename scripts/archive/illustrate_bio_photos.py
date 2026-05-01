#!/usr/bin/env python3
"""Create uniform monochrome circular illustrations from scraped bio photos."""

from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image, ImageChops, ImageDraw, ImageEnhance, ImageFilter, ImageFont, ImageOps

try:
    import numpy as np
    import cv2
except ImportError:  # pragma: no cover - depends on the local Python environment.
    np = None
    cv2 = None


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "data" / "bio_photos"
DEFAULT_DEST = DEFAULT_SOURCE / "Illustrations"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


def image_files(source: Path) -> list[Path]:
    return sorted(
        path
        for path in source.iterdir()
        if path.is_file() and path.suffix.lower() in IMAGE_EXTENSIONS
    )


def fit_square(image: Image.Image, size: int) -> Image.Image:
    image = ImageOps.exif_transpose(image).convert("RGB")
    width, height = image.size
    scale = size / min(width, height)
    resized = image.resize(
        (round(width * scale), round(height * scale)),
        Image.Resampling.LANCZOS,
    )
    left = (resized.width - size) // 2
    top = (resized.height - size) // 2
    return resized.crop((left, top, left + size, top + size))


def posterize_tones(gray: np.ndarray) -> np.ndarray:
    # Five ink-like tones keep the tiny 150px originals from looking like noisy photos.
    tone_values = np.array([32, 78, 126, 180, 232], dtype=np.uint8)
    bins = np.array([50, 96, 144, 196], dtype=np.uint8)
    return tone_values[np.digitize(gray, bins)]


def posterize_tones_pil(gray: Image.Image) -> Image.Image:
    def map_tone(pixel: int) -> int:
        if pixel < 50:
            return 32
        if pixel < 96:
            return 78
        if pixel < 144:
            return 126
        if pixel < 196:
            return 180
        return 232

    return gray.point(map_tone)


def illustrate(source_path: Path, output_size: int, circle_scale: float) -> Image.Image:
    source = Image.open(source_path)
    square = fit_square(source, output_size)
    if cv2 is not None and np is not None:
        rgb = np.array(square)
        lab = cv2.cvtColor(rgb, cv2.COLOR_RGB2LAB)
        l_chan, a_chan, b_chan = cv2.split(lab)
        clahe = cv2.createCLAHE(clipLimit=1.8, tileGridSize=(8, 8))
        l_chan = clahe.apply(l_chan)
        balanced = cv2.cvtColor(cv2.merge((l_chan, a_chan, b_chan)), cv2.COLOR_LAB2RGB)

        gray = cv2.cvtColor(balanced, cv2.COLOR_RGB2GRAY)
        gray = cv2.bilateralFilter(gray, 9, 36, 36)
        gray = cv2.normalize(gray, None, 18, 238, cv2.NORM_MINMAX)

        tones = posterize_tones(gray)
        tones = cv2.medianBlur(tones, 3)

        edges = cv2.Canny(gray, 46, 118)
        edges = cv2.dilate(edges, np.ones((2, 2), np.uint8), iterations=1)
        edges = cv2.GaussianBlur(edges, (3, 3), 0)

        ink = tones.copy()
        ink[edges > 34] = np.minimum(ink[edges > 34], 28)

        # Lighten the treatment slightly so the set reads consistently on white.
        ink = cv2.addWeighted(ink, 0.86, np.full_like(ink, 255), 0.14, 0)
        portrait = Image.fromarray(ink, mode="L").convert("RGB")
    else:
        gray_image = ImageOps.grayscale(square)
        gray_image = ImageOps.autocontrast(gray_image, cutoff=1)
        gray_image = ImageEnhance.Contrast(gray_image).enhance(1.14)
        gray_image = gray_image.filter(ImageFilter.SMOOTH_MORE)
        tones = posterize_tones_pil(gray_image).filter(ImageFilter.MedianFilter(3))
        edge_image = gray_image.filter(ImageFilter.FIND_EDGES)
        edge_image = ImageOps.autocontrast(edge_image)
        edge_image = edge_image.filter(ImageFilter.MaxFilter(3)).filter(ImageFilter.GaussianBlur(0.45))
        edge_mask = edge_image.point(lambda pixel: 255 if pixel > 34 else 0)
        dark_lines = Image.new("L", tones.size, 28)
        ink = Image.composite(dark_lines, tones, edge_mask)
        portrait = ImageChops.blend(ink, Image.new("L", ink.size, 255), 0.14).convert("RGB")

    canvas = Image.new("RGB", (output_size, output_size), "white")
    margin = round(output_size * (1.0 - circle_scale) / 2.0)
    diameter = output_size - margin * 2

    mask = Image.new("L", (output_size, output_size), 0)
    draw = ImageDraw.Draw(mask)
    draw.ellipse((margin, margin, margin + diameter, margin + diameter), fill=255)
    canvas.paste(portrait, (0, 0), mask)

    border = ImageDraw.Draw(canvas)
    border.ellipse(
        (margin, margin, margin + diameter, margin + diameter),
        outline=(215, 215, 215),
        width=max(1, output_size // 160),
    )
    return canvas


def output_path_for(source: Path, dest: Path, extension: str) -> Path:
    return dest / f"{source.stem}_illustrated{extension}"


def save_image(image: Image.Image, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() in {".jpg", ".jpeg"}:
        image.save(path, quality=94, subsampling=0, optimize=True)
    else:
        image.save(path, optimize=True)


def build_contact_sheet(pairs: list[tuple[Path, Path]], path: Path, thumb: int = 150) -> None:
    if not pairs:
        return

    pad = 18
    label_h = 30
    cols = 4
    cell_w = thumb * 2 + pad * 3
    cell_h = thumb + label_h + pad * 2
    rows = (len(pairs) + cols - 1) // cols
    sheet = Image.new("RGB", (cols * cell_w, rows * cell_h), "white")
    draw = ImageDraw.Draw(sheet)
    try:
        font = ImageFont.truetype("Arial.ttf", 11)
    except OSError:
        font = ImageFont.load_default()

    for idx, (source, illustrated) in enumerate(pairs):
        col = idx % cols
        row = idx // cols
        x = col * cell_w + pad
        y = row * cell_h + pad

        original = fit_square(Image.open(source), thumb)
        output = Image.open(illustrated).convert("RGB").resize((thumb, thumb), Image.Resampling.LANCZOS)
        sheet.paste(original, (x, y))
        sheet.paste(output, (x + thumb + pad, y))
        label = source.stem[:38]
        draw.text((x, y + thumb + 7), label, fill=(40, 40, 40), font=font)

    path.parent.mkdir(parents=True, exist_ok=True)
    sheet.save(path, quality=92, optimize=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--size", type=int, default=512)
    parser.add_argument("--circle-scale", type=float, default=0.90)
    parser.add_argument("--extension", choices=(".jpg", ".png"), default=".jpg")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--contact-sheet", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    files = image_files(args.source)
    batch = files[args.offset :]
    if args.limit is not None:
        batch = batch[: args.limit]

    written: list[tuple[Path, Path]] = []
    skipped = 0
    for source in batch:
        out = output_path_for(source, args.dest, args.extension)
        if out.exists() and not args.overwrite:
            skipped += 1
            continue
        image = illustrate(source, args.size, args.circle_scale)
        save_image(image, out)
        written.append((source, out))
        print(out)

    if args.contact_sheet:
        build_contact_sheet(written, args.contact_sheet)
        print(args.contact_sheet)

    print(f"written={len(written)} skipped={skipped} total_selected={len(batch)}")


if __name__ == "__main__":
    main()
