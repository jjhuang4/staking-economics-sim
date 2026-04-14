# Shared

This folder is the exchange area used by both Docker Compose services.

## Purpose

- Provide a simple host-mounted location for files produced by `simulator` and `cadlabs`.
- Avoid direct service-to-service RPC for basic data exchange.
- Preserve generated outputs outside transient containers.

## Subfolders

- `output/` is for plots, summaries, and exported artifacts.
- `data/` is for CSV, JSON, snapshots, and intermediate datasets.

## Notes

- Keep filenames distinct when both services write similar artifacts.
- If needed, create subfolders like `output/simulator/` and `output/cadlabs/` to avoid ambiguity.
