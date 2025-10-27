import os
import requests
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any

from field_filter import load_field_map_from_json, filter_fields_by_list

load_dotenv()

# ------------------ 🔧 ENV VARS ------------------
ORG_ID = os.getenv("ORG_ID")
APP_FORM_ID = os.getenv("APP_FORM_ID")
LOGIN_ID = os.getenv("LOGIN_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
LOGIN_URL = os.getenv("LOGIN_URL")
GATEWAY = os.getenv("GATEWAY")


# ------------------ 📘 FORM → OBJECT MAP ------------------
FORM_TO_OBJECT = {
    os.getenv("FORM_APPLICATION"): "Application__c",
    os.getenv("FORM_PROPERTY"): "Property__c",
    os.getenv("FORM_LOAN_APPLICANT"): "Loan_Applicant__c",
    os.getenv("FORM_CAPABILITY"): "Capability__c",
    os.getenv("FORM_LOAN_DETAILS"): "Loan_Details__c",
    os.getenv("FORM_CONTENT_VERSION"): "ContentVersion",
    os.getenv("FORM_CASHFLOW"): "Cashflow__c",
    os.getenv("FORM_CHARACTER"): "Character__c",
    os.getenv("FORM_PROPERTY_OWNERS"): "Property_Owners__c",
    os.getenv("FORM_BUREAU_HIGHMARK"): "Bureau_Highmark__c",
    os.getenv("FORM_COMMON_OBJECT"): "Commonobject__c",
    os.getenv("FORM_DEDUPE_DETAIL"): "Dedupe_Detail__c",
    os.getenv("FORM_DEFERRAL_DOCUMENT"): "Deferral_Document__c",
    os.getenv("FORM_FEE_CREATION"): "Fee_Creation__c",
    os.getenv("FORM_LOAN_LIEN_LINKING"): "Loan_Lien_Linking__c",
    os.getenv("FORM_RECEIPT"): "Receipt__c",
    os.getenv("FORM_SANCTION_CONDITION"): "Sanction_Condition__c",
    os.getenv("FORM_TOPUP"): "Topup__c",
    os.getenv("FORM_TR_DEVIATION"): "Tr_Deviation__c",
}


# ------------------ 🔐 SESSION ------------------
def get_session_id():
    res = requests.post(
        LOGIN_URL,
        headers={
            "fs-api-key": CLIENT_ID,
            "fs-organization-id": ORG_ID,
            "fs-user-id": LOGIN_ID,
        },
    )
    res.raise_for_status()
    data = res.json()
    session_id = data.get("data", {}).get("sessionId")
    print("✅ Session ID:", session_id)
    return session_id


# ------------------ 🌐 FETCH HELPERS ------------------
def fetch_json(url, session_id):
    headers = {
        "x-session-id": session_id,
        "fs-api-key": CLIENT_ID,
        "fs-organization-id": ORG_ID,
        "fs-user-id": LOGIN_ID,
    }
    res = requests.get(url, headers=headers)
    try:
        return res.json()
    except Exception:
        print(f"⚠️ Invalid JSON from: {url}")
        return []


def fetch_by_parent_field(form_id, parent_field, parent_id, session_id):
    url = f"{GATEWAY}/incomming/configdata/{ORG_ID}/{form_id}?{parent_field}={parent_id}"
    print(f"📡 Fetching {FORM_TO_OBJECT.get(form_id)} via {parent_field}={parent_id}")
    data = fetch_json(url, session_id)
    return data if isinstance(data, list) else []


def to_api_name(name: str):
    """Convert e.g. loan_applicant__c → Loan_Applicant__c"""
    if not name:
        return name
    parts = name.split("__c")
    return parts[0].capitalize() + "__c"


# ------------------ 🔁 RECURSIVE HIERARCHY ------------------
def fetch_hierarchy_by_tree(object_name, parent_id, session_id, parent_object, tree_node, field_map):
    """
    Recursively fetches child records for a given object and parent.
    Uses the parent_id to filter results (e.g., Loan_Applicant__c = <fivestarId>).
    """
    form_id = next((fid for fid, obj in FORM_TO_OBJECT.items() if obj == object_name), None)
    if not form_id:
        print(f"⚠️ No form found for object: {object_name}")
        return []

    # Join field inferred from parent
    join_field = parent_object if parent_object else "Application__c"

    # Fetch children filtered by parent_id
    data = fetch_by_parent_field(form_id, join_field, parent_id, session_id)
    if not data:
        print(f"⚠️ No records found for {object_name} via {join_field}={parent_id}")
        return []

    # Filter only relevant fields
    normalized_key = next((k for k in field_map.keys() if k.lower() == object_name.lower()), None)
    allowed_fields = field_map.get(normalized_key, []) if normalized_key else []
    filtered = filter_fields_by_list(data, allowed_fields, strict=True)

    flattened_records = []

    for rec in filtered:
        rec_id = rec.get("fivestarId") or rec.get("Id")
        flat_rec = {**rec}

        # Recursive call for children
        for child_obj, child_tree in (tree_node or {}).items():
            print(f"➡ Fetching child {child_obj} under {object_name} ({rec_id})")
            child_records = fetch_hierarchy_by_tree(
                child_obj,
                rec_id,
                session_id,
                object_name,
                child_tree,
                field_map,
            )

            flat_rec[to_api_name(child_obj)] = child_records if child_records else []

        flattened_records.append(flat_rec)

    # Ensure child structure even if no records exist
    if not flattened_records and tree_node:
        empty_stub = {}
        for child_obj in (tree_node or {}).keys():
            empty_stub[to_api_name(child_obj)] = []
        flattened_records.append(empty_stub)

    return flattened_records


# ------------------ 🧠 REQUEST MODEL ------------------
class HierarchyRequest(BaseModel):
    application_name: str
    relation_map: Dict[str, Any]
    field_map: Dict[str, Any]


# ------------------ 🚀 FASTAPI APP ------------------
app = FastAPI()


@app.post("/generate_hierarchy")
def generate_hierarchy(req: HierarchyRequest):
    session_id = get_session_id()

    # Anchor: Application__c
    anchor = list(req.relation_map.keys())[0]
    print(f"🔗 Starting from anchor: {anchor}")

    # Fetch base application record
    app_url = f"{GATEWAY}/incomming/configdata/{ORG_ID}/{APP_FORM_ID}?Name={req.application_name}"
    app_data = fetch_json(app_url, session_id)
    if not app_data:
        return {"error": f"Application {req.application_name} not found."}

    app_record = app_data[0]
    app_id = app_record.get("fivestarId")

    # Filter base application fields
    normalized_app_key = next((k for k in req.field_map if k.lower() == anchor.lower()), None)
    allowed_app_fields = req.field_map.get(normalized_app_key, [])
    app_fields_arr = filter_fields_by_list([app_record], allowed_app_fields, strict=True)
    app_fields = app_fields_arr[0] if app_fields_arr else {}

    # Build result tree
    result = {anchor: {**app_fields}}

    # Iterate over child objects
    for child_obj, child_tree in req.relation_map[anchor].items():
        child_key = to_api_name(child_obj)
        child_records = fetch_hierarchy_by_tree(
            child_obj,
            app_id,
            session_id,
            anchor,
            child_tree,
            req.field_map,
        )
        result[anchor][child_key] = child_records if child_records else []

    return result


@app.get("/")
def root():
    return {"message": "POST /generate_hierarchy with relation_map, field_map, and application_name"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
