import json
import os
import re

# ---------------- 📘 Load Field Map ----------------
def load_field_map_from_json(file_path="./fieldMap.json"):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ JSON field map not found at: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        field_map = json.load(f)

    normalized = {}

    def normalize_key(key: str) -> str:
        """
        Normalize field map keys so that:
        - extra underscores are collapsed
        - single `_c` or malformed ones become `__c`
        - everything is lowercase
        """
        key = key.strip()
        # collapse multiple underscores like fee__creation__c → fee_creation__c
        key = re.sub(r'_+', '_', key)
        # ensure it ends with __c
        if key.endswith('_c'):
            key = re.sub(r'_c$', '__c', key)
        return key

    for key, val in field_map.items():
        norm_key = normalize_key(key)
        normalized[norm_key] = val

    print("✅ Loaded Field Map objects:", list(normalized.keys()))
    print("🧾 Field counts per object:", {k: len(v) for k, v in normalized.items()})
    return normalized


# ---------------- 🔎 Filter Fields ----------------
def filter_fields_by_list(data, allowed_fields, strict=True):
    if not isinstance(allowed_fields, list):
        allowed_fields = []
    if not isinstance(data, list):
        return []

    def normalize(s):
        if not s:
            return ""
        return (
            str(s)
            .replace(" ", "")
            .replace("(", "")
            .replace(")", "")
            .replace("-", "")
            .replace("_", "")
            .lower()
        )

    filtered_list = []
    for record in data:
        if not isinstance(record, dict):
            continue
        filtered = {}
        record_lookup = {normalize(k): k for k in record.keys()}

        for field in allowed_fields:
            orig = record_lookup.get(normalize(field))
            filtered[field] = record.get(orig) if orig else None

        if record.get("fivestarId"):
            filtered["fivestarId"] = record["fivestarId"]
        if record.get("Id"):
            filtered["Id"] = record["Id"]

        if not strict and not filtered:
            filtered_list.append(record)
        else:
            filtered_list.append(filtered)
    return filtered_list
