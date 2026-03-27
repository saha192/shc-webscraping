# Soil Health Card Scraper

This repository contains a Python script to download and organize Soil Health Card (SHC) geospatial data from the SHC endpoint, including:

- state metadata
- district metadata
- layer metadata
- KML files
- parsed feature JSON files

## What the script does

The main script:

1. fetches the list of states
2. fetches districts for each state
3. fetches layer metadata for each district
4. downloads KML files for each SHC layer
5. parses the KML files into merged `features.json`

## Notes

- State and district filters should be passed in uppercase to match the internal matching logic.
- Existing JSON/KML files are skipped if they are already present.
- The script assumes the SHC endpoint and GraphQL queries remain available in their current form.
- If your utilities return unexpected output types, update the helper functions accordingly.

## How to use?

1. Run download_shc_kml.py
2. Run csv_data_extractor.py
