# Archived scripts

Scripts in this folder are no longer part of the active build pipeline. They're kept for reference: useful when debugging historical data or as templates for similar one-off tasks.

## The patch family (archived 2026-05-15)

All four of these were "surgical patches" that modified `data/` files outside the normal `build_data.py` flow. Their logic has been folded into `build_data.py` so every nightly rebuild applies the same transformations automatically.

| Patch | Superseded by |
|-------|---------------|
| `patch_features.py` | `scripts/build_data.py` standing-features block (`standing_feature_specs` near the features_data dict). Builds 7 features: letters_to_editor, on_the_market, metropolitan_diary, boldface_names, names_of_the_dead, coronavirus_briefing, arts_briefly. |
| `patch_lottery_feature.py` | `scripts/build_data.py:425-433` (section reassignment + is_lottery_numbers flag) and the `lottery_numbers` feature block in `features_data`. |
| `patch_medians.py` | `scripts/build_data.py:1911-1953` (exact annual median computation from per-article word counts, with the same blog/podcast/live/brief filters). |
| `patch_rnc_dnc_year.py` | `scripts/build_data.py:2543-2557` (`_rewrite_conv_year`) applied to every subject tag at `:2722`. Future convention years get the suffix automatically. |

If you're tempted to resurrect one of these, first check whether `build_data.py` already handles the case — it almost certainly does.

## Other archived scripts

Earlier one-shot scrapes, prototypes, and superseded versions of current scripts. Filenames are mostly self-explanatory. None are imported by anything in the active pipeline.
