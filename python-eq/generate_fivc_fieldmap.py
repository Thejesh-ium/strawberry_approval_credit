import pandas as pd
import json

# Path to your Excel file
EXCEL_PATH = "FIVC_Screen_Analysis.xlsx"
SHEET_NAME = "In Scope objects and fields"

# Objects relevant to FIV-C stage (from your hierarchy)

FIVC_OBJECTS = [
    "Application__c",
    "Capability__c",
    "Character__c",
    "Property_Owners__c",
    "CommonObject__c",
    "Deferral_Document__c",
    "Geo_Location__c",
    "Loan_Applicant__c",
    "Bureau_Highmark__c",
    "Loan_Details__c",
    "Property__c",
    "Verification__c",
    "Revisit__c",
]

# Read the Excel sheet
df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)

# Normalize column names
df.columns = [c.strip() for c in df.columns]

# Expect columns:  "Salesforce Object"  |  "Field API Name"
object_col = "Salesforce Object"
field_col = "Field API Name"

# Build the mapping
field_map = {}
for obj in FIVC_OBJECTS:
    subset = df[df[object_col].str.strip().eq(obj)]
    field_list = subset[field_col].dropna().astype(str).tolist()
    field_map[obj] = field_list

# Save to JSON
with open("filtered_fieldMap_FIVC.json", "w", encoding="utf-8") as f:
    json.dump(field_map, f, indent=2)

print(f"✅ Created filtered_fieldMap_FIVC.json with {len(field_map)} objects")
