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
| `regenerate_obit_interactive_fixes.py` | `scripts/build_obituaries.py:1370` (`NON_OBIT_URLS` filter during ingestion) and `:1574-1584` (`OBIT_OVERRIDES` applied to all records). Was previously called by `update.py` after `build_obituaries.py` for a fast-iteration loop during the 2026-04-25 interactive obit audit. **Had a bug**: when an override defined both `name` and `display_name`, it forced `display_name = ov['name']`, ignoring `ov['display_name']`. This silently stripped honorifics ("Pope John Paul II" → "John Paul II") and middle initials ("Geraldine A. Ferraro" → "Geraldine Ferraro") on every nightly run. Removed from `update.py` and archived 2026-05-15. `build_obituaries.py` already does the right thing in one pass. |
| `patch_beats.py` | `scripts/build_data.py:_normalize_subject_kw` (`:2540`) + `_is_generic_subject` (`:955`) + `build_beats` (`:963`) — fully re-implemented inside `build_data.py`. The patch existed as a fast-iteration alternative to a full rebuild when `tag_config.json` changed: it could re-normalize subjects and rebuild beats in ~30s instead of ~8min. But it was a partial reimplementation maintained in parallel (drift risk), and the daily noon build runs `build_data.py` anyway, so the longest the patch saves you is < 24 hours. New workflow after editing `tag_config.json`: run `python update.py --no-corr --no-validate` (~8 min) or just wait for the noon build. Archived 2026-05-15. |

If you're tempted to resurrect one of these, first check whether `build_data.py` / `build_obituaries.py` already handles the case — it almost certainly does.

## Other archived scripts

Earlier one-shot scrapes, prototypes, and superseded versions of current scripts. Filenames are mostly self-explanatory. None are imported by anything in the active pipeline.
