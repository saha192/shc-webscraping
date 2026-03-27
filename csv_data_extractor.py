import json
import re
from pathlib import Path

import pandas as pd


# ============================================================
# PATHS
# ============================================================
INPUT_ROOT = Path(r"H:\shc_data\KML_FILES\states")
OUTPUT_ROOT = Path(r"H:\shc_data\SHC_data")
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)


# ============================================================
# HELPERS
# ============================================================
def first_non_empty(*values):
    """Return the first non-empty value."""
    for v in values:
        if v is not None and str(v).strip() != "":
            return v
    return ""


def safe_filename(name: str) -> str:
    """Make a safe Windows filename."""
    return re.sub(r'[<>:"/\\|?*]', "_", name).strip()


def normalize_key(key: str) -> str:
    """
    Standardize property names so old/new JSON schemas align.
    Example: Fe/fe -> Fe, village/VILLAGE -> village
    """
    k = str(key).strip()

    alias_map = {
        "fe": "Fe",
        "FE": "Fe",
        "zn": "Zn",
        "ZN": "Zn",
        "cu": "Cu",
        "CU": "Cu",
        "mn": "Mn",
        "MN": "Mn",
        "ph": "pH",
        "PH": "pH",
        "ec": "EC",
        "oc": "OC",
        "n": "N",
        "p": "P",
        "k": "K",
        "b": "B",
        "s": "S",
        "village": "village",
        "VILLAGE": "village",
        "district": "district",
        "DISTRICT": "district",
        "state": "state",
        "STATE": "state",
        "tehsil": "tehsil",
        "TEHSIL": "tehsil",
        "surveyno": "surveyNo",
        "surveyNo": "surveyNo",
        "computedid": "computedID",
        "computedID": "computedID",
        "date": "date",
        "Soil_Depth": "Soil_Depth",
        "Slope": "Slope",
        "Texture": "Texture",
        "LCC": "LCC",
        "LIC": "LIC",
        "Erosion": "Erosion",
        "HSG": "HSG",
        "DISTRICT_L": "DISTRICT_L",
        "STATE_LGD": "STATE_LGD",
        "Category": "Category",
    }

    return alias_map.get(k, k)


def extract_record(feature: dict, state_name: str, district_name: str) -> dict:
    """Flatten one feature record into a row dictionary."""
    props = feature.get("properties", {})
    if not isinstance(props, dict):
        props = {}

    # Normalize property keys
    norm_props = {}
    for k, v in props.items():
        norm_props[normalize_key(k)] = v

    row = {
        "state": first_non_empty(norm_props.get("state"), state_name),
        "district": first_non_empty(norm_props.get("district"), district_name),
        "village": first_non_empty(norm_props.get("village")),
        "tehsil": first_non_empty(norm_props.get("tehsil")),
        "latitude": feature.get("latitude", ""),
        "longitude": feature.get("longitude", ""),
        "date": first_non_empty(norm_props.get("date")),
        "period": feature.get("period", ""),
    }

    # Add all remaining properties too
    for k, v in norm_props.items():
        if k not in row:
            row[k] = v

    return row


# ============================================================
# MAIN EXTRACTION
# ============================================================
all_state_dirs = [p for p in INPUT_ROOT.iterdir() if p.is_dir()]

for state_dir in sorted(all_state_dirs):
    state_name = state_dir.name
    rows = []

    # Search recursively for all features.json inside the state folder
    json_files = list(state_dir.rglob("features.json"))

    if not json_files:
        print(f"[WARNING] No features.json found in: {state_name}")
        continue

    for json_file in json_files:
        district_name = json_file.parent.name

        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"[ERROR] Could not read {json_file}: {e}")
            continue

        if not isinstance(data, list):
            print(f"[WARNING] JSON is not a list in file: {json_file}")
            continue

        for feature in data:
            if not isinstance(feature, dict):
                continue
            row = extract_record(feature, state_name=state_name, district_name=district_name)
            rows.append(row)

    if not rows:
        print(f"[WARNING] No valid records found for state: {state_name}")
        continue

    df = pd.DataFrame(rows)

    # Convert lat/lon to numeric
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Try converting common soil numeric columns
    numeric_cols = [
        "B", "Cu", "Fe", "Mn", "Zn", "S", "OC", "pH", "EC",
        "N", "P", "K"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Preferred column order
    preferred_cols = [
        "state", "district", "village", "tehsil",
        "latitude", "longitude", "date", "period",
        "N", "P", "K", "OC", "pH", "EC", "S",
        "Zn", "Fe", "Mn", "Cu", "B",
        "Category",
        "Soil_Depth", "Slope", "Texture", "LCC", "LIC", "Erosion", "HSG",
        "surveyNo", "computedID", "DISTRICT_L", "STATE_LGD"
    ]

    existing_preferred = [c for c in preferred_cols if c in df.columns]
    remaining_cols = [c for c in df.columns if c not in existing_preferred]
    df = df[existing_preferred + remaining_cols]

    out_csv = OUTPUT_ROOT / f"{safe_filename(state_name)}.csv"
    df.to_csv(out_csv, index=False, encoding="utf-8-sig")

    print(f"[DONE] {state_name}: {len(df)} records saved to {out_csv}")

print("\nAll state CSV files created successfully.")
