import os
import requests
from dotenv import load_dotenv
from field_filter import load_field_map_from_json, filter_fields_by_list
import strawberry
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from strawberry.scalars import JSON

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
    os.getenv("FORM_FORM_REVISIT") : "Revisit__c",
    os.getenv("FORM_GEO_LOCATION") : "Form_Geo_Location__c",
    os.getenv("FORM_VERIFICATION") : "Verification__c"
}

# ------------------ 🔗 RELATION MAP ------------------
RELATION_MAP = {
    "Application__c": {
        "Capability__c": {"ContentVersion": {}},
        "Cashflow__c": {"ContentVersion": {}},
        "Character__c": {"Property_Owners__c": {}},
        "CommonObject__c": {},
        "Deferral_Document__c": {},
        "Fee_Creation__c": {},
        "Loan_Applicant__c": {
            "Bureau_Highmark__c": {"Loan_Details__c": {}},
            "Capability__c":{},
            "Cashflow__c":{},
            "Character__c":{},
            "ContentVersion":{},
            "Dedupe_Details__c": {},
            "Fee_Creation__c": {},
            "Loan_Details__c": {},
            "Property__c": {
                "CommonObject__c": {},
                "Deferral_Document__c": {},
                "Fee_Creation__c": {},
                "Property_Owners__c": {},
                "Tr_Deviation__c": {}
            },
            "Property_Owners__c" : {},
            "Reciept__c" : {},
            "Tr_Deviation__c" : {}
        },
        "Loan_Lien_Linking__c": {},
        "Property__c": {},
        "Reciept__c": {},
        "Sanction_Condition__c": {},
        "Topup__c": {},
        "Tr_Deviation__c": {}
    }
}


# ------------------ 📘 Load Field Map ------------------
field_map = load_field_map_from_json("./filtered_fieldMap.json")

# ------------------ 🔐 Session ------------------
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


# ------------------ 🌐 Fetch JSON ------------------
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


# ------------------ 🔍 Fetch by Parent ------------------
def fetch_by_parent_field(form_id, parent_field, parent_id, session_id):
    url = f"{GATEWAY}/incomming/configdata/{ORG_ID}/{form_id}?{parent_field}={parent_id}"
    print(f"📡 Fetching {FORM_TO_OBJECT.get(form_id)} via {parent_field}={parent_id}")
    data = fetch_json(url, session_id)
    return data if isinstance(data, list) else []


# ------------------ 🔠 Utility ------------------
def to_api_name(name: str):
    if not name:
        return name
    parts = name.split("__c")
    return parts[0].capitalize() + "__c"


# ------------------ 🔁 Recursive Hierarchy ------------------
def fetch_hierarchy_by_tree(object_name, parent_id, session_id, parent_object, tree_node):
    form_id = next((fid for fid, obj in FORM_TO_OBJECT.items() if obj == object_name), None)
    if not form_id:
        return []

    # 🔁 Join field inferred directly from parent name
    join_field = parent_object if parent_object else "Application__c"

    data = fetch_by_parent_field(form_id, join_field, parent_id, session_id)

    # Even if data is empty, we'll still traverse children (for consistent key structure)
    normalized_key = next((k for k in field_map.keys() if k.lower() == object_name.lower()), None)
    allowed_fields = field_map.get(normalized_key, []) if normalized_key else []
    filtered = filter_fields_by_list(data, allowed_fields, strict=True) if data else []

    flattened_records = []

    # If there are records, process them normally
    for rec in filtered:
        rec_id = rec.get("fivestarId") or rec.get("Id")
        flat_rec = {**rec}

        # Recursively fetch children for each record
        for child_obj, child_tree in (tree_node or {}).items():
            child_key = to_api_name(child_obj)
            child_records = fetch_hierarchy_by_tree(child_obj, rec_id, session_id, object_name, child_tree)
            # ✅ Always include the key, even if no data
            flat_rec[child_key] = child_records if child_records else []

        flattened_records.append(flat_rec)

    # If no records exist at this level, we still return an empty list (no recursion)
    if not flattened_records and tree_node:
        # Return a stub structure showing empty children for completeness
        empty_stub = {}
        for child_obj in (tree_node or {}).keys():
            child_key = to_api_name(child_obj)
            empty_stub[child_key] = []
        flattened_records.append(empty_stub)

    return flattened_records


# ------------------ 🧠 GraphQL ------------------
@strawberry.type
class Query:
    # type: ignore[reportInvalidTypeForm]
    @strawberry.field
    def get_application_hierarchy(self, application_name: str) -> JSON:
        """Fetch full application hierarchy by name"""
        session_id = get_session_id()
        app_url = f"{GATEWAY}/incomming/configdata/{ORG_ID}/{APP_FORM_ID}?Name={application_name}"
        app_data = fetch_json(app_url, session_id)
        if not app_data:
            return {}

        app_record = app_data[0]
        app_id = app_record.get("fivestarId")

        normalized_app_key = next((k for k in field_map if k == "Application__c"), None)
        allowed_app_fields = field_map.get(normalized_app_key, [])
        app_fields_arr = filter_fields_by_list([app_record], allowed_app_fields, strict=True)
        app_fields = app_fields_arr[0] if app_fields_arr else {}

        top_key = to_api_name("Application__c")
        app_result = {top_key: {**app_fields}}

        for child_obj, child_tree in RELATION_MAP["Application__c"].items():
            child_key = to_api_name(child_obj)
            child_records = fetch_hierarchy_by_tree(child_obj, app_id, session_id, "Application__c", child_tree)
            # ✅ Always include child keys, even when no data
            app_result[top_key][child_key] = child_records if child_records else []


        return app_result



# ------------------ 🚀 FastAPI + Strawberry ------------------
schema = strawberry.Schema(Query)
graphql_app = GraphQLRouter(schema)
app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")

@app.get("/")
def root():
    return {"message": "Go to /graphql for GraphQL Playground"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
