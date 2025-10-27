import json
import pandas as pd
import re
 
# ----------------------------
# CONFIG
# ----------------------------
FIELD_MAP_PATH = "./filtered_fieldMap_FIVC.json"
EXCEL_PATH = "./Salesforce Prod objects with sample UAT records.xlsx"
OUTPUT_PATH = "./filtered_fieldMap_FIVC.json"
 
# ----------------------------
# HELPERS
# ----------------------------
def clean_field_name(field_name: str) -> str:
    """Clean relationship and formatting junk."""
    if not isinstance(field_name, str):
        return ""
    # Remove tabs, carriage returns, spaces
    field_name = field_name.replace("\t", "").replace("\r", "").strip()
    # Split at newline or '(' to remove references
    base = re.split(r"[\n(]", field_name)[0].strip()
    # Remove non-alphanumeric ending chars
    base = re.sub(r"[^A-Za-z0-9_]+$", "", base)
    return base
 
def normalize_field(field: str) -> str:
    """Normalize for lowercase comparison."""
    return re.sub(r"[^a-z0-9_]+", "", field.lower())
 
def normalize_name(name: str) -> str:
    return name.lower().replace("__c", "").strip()
 
# ----------------------------
# STEP 1: Load data
# ----------------------------
with open(FIELD_MAP_PATH, "r", encoding="utf-8") as f:
    field_map = json.load(f)
print(f"✅ Loaded fieldMap with {len(field_map)} objects")
 
# ----------------------------
# STEP 2: Load Excel sheet names
# ----------------------------
xls = pd.ExcelFile(EXCEL_PATH)
sheet_names = xls.sheet_names
print(f"📄 Found {len(sheet_names)} sheets in Excel")
 
sheet_lookup = {normalize_name(s): s for s in sheet_names}
 
# ----------------------------
# STEP 3: Clean fields (only check changed ones)
# ----------------------------
filtered_field_map = {}
changed_fields_summary = {}
 
for obj_name, fields in field_map.items():
    obj_norm = normalize_name(obj_name)
    sheet_name = sheet_lookup.get(obj_norm)
    if not sheet_name:
        possible = [s for n, s in sheet_lookup.items() if obj_norm in n or n in obj_norm]
        if possible:
            sheet_name = possible[0]
        else:
            print(f"⚠️ No matching sheet found for {obj_name}")
            filtered_field_map[obj_name] = fields  # keep as is
            continue
 
    try:
        df = pd.read_excel(xls, sheet_name=sheet_name, nrows=1)
        excel_fields = [str(c).strip() for c in df.columns if pd.notna(c)]
        excel_lookup = {normalize_field(c): c for c in excel_fields}
    except Exception as e:
        print(f"⚠️ Error reading {sheet_name}: {e}")
        filtered_field_map[obj_name] = fields
        continue
 
    cleaned_fields = []
    changed_fields = []
    seen = set()
 
    for f in fields:
        cleaned = clean_field_name(f)
        if cleaned != f:  # ✅ changed/cleaned field only
            changed_fields.append((f, cleaned))
        if cleaned and cleaned not in seen:
            cleaned_fields.append(cleaned)
            seen.add(cleaned)
 
    # ✅ Validate only changed fields
    valid_fields = []
    for f in cleaned_fields:
        norm = normalize_field(f)
        if f in [c for _, c in changed_fields]:  # if it was changed, verify in Excel
            if norm in excel_lookup:
                valid_fields.append(excel_lookup[norm])
            else:
                print(f"⚠️ {f} not found in Excel ({obj_name}) – skipped")
        else:
            valid_fields.append(f)  # unchanged, keep directly
 
    filtered_field_map[obj_name] = valid_fields
    changed_fields_summary[obj_name] = len(changed_fields)
 
# ----------------------------
# STEP 4: Save result
# ----------------------------
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(filtered_field_map, f, indent=2, ensure_ascii=False)
 
print(f"\n✅ Filtered field map saved to {OUTPUT_PATH}")
print(f"🔍 Objects with cleaned fields: { {k: v for k, v in changed_fields_summary.items() if v > 0} }")
 
 