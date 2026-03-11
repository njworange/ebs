# EBS

EBS TV VOD downloader skeleton for FlaskFarm.

## Goal

This repository is a fresh starting point for a TV-wide EBS plugin, separate from the AniKids-only `ebs_downloader` plugin.

## Current Scope

- Plugin bootstrap and menu wiring
- Basic settings/download page skeleton
- Auto collection/list page skeleton
- `EbsTvClient` placeholder for TV-wide discovery and replay parsing
- DB-backed queue model scaffold for future download automation

## Suggested Phase 1

1. Implement `EbsTvClient.collect_daily_vods()` using the EBS TV program/VOD source.
2. Implement `EbsTvClient.analyze_program_url()` for `tv/show?...` replay pages.
3. Implement `EbsTvClient.get_episode_play_info()` for quality/playback extraction.
4. Reuse the existing queue/download pipeline from `mod_auto.py` once playback parsing is stable.

## Architecture

- `setup.py`
  - FlaskFarm plugin bootstrap and menu registration
- `clients/ebs_tv.py`
  - EBS TV-specific discovery and replay parsing entry points
- `client.py`
  - Compatibility wrapper that re-exports `EbsTvClient`
- `mod_basic.py`
  - Manual analysis/download workflow
- `mod_auto.py`
  - Scheduler/list command scaffold that delegates to client and queue services
- `models.py`
  - Neutral DB model using remote identifiers instead of AniKids naming
- `queue_service.py`
  - Reusable queue/dedupe service for later download worker expansion
- `templates/`
  - Minimal UI for settings, download, and auto list pages

## Important Design Choice

This plugin should stay separate from `ebs_downloader`.

- `ebs_downloader` is AniKids-specific.
- `ebs` should target EBS TV replay pages and the daily VOD feed.
- Shared download primitives can be reused later, but discovery/parsing should remain separate.
