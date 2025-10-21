from flask import Flask, request, jsonify
from clickhouse_connect import get_client
from datetime import datetime, date
import os
import json
import logging

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

# ----------------------------
# 1️⃣ Connect to ClickHouse
# ----------------------------
client = get_client(
    host=os.getenv("CH_HOST", "localhost"),
    username=os.getenv("CH_USER", "admin"),
    password=os.getenv("CH_PASS", "rishu123"),  # remove hardcoded default in production
    port=int(os.getenv("CH_PORT", "8123")),
    database=os.getenv("CH_DB", "vehicle_fastag")
)

# ----------------------------
# 2️⃣ Helper Functions
# ----------------------------
def _parse_date(value):
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.date() if isinstance(value, datetime) else value
    try:
        return datetime.fromisoformat(value).date()
    except Exception:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except Exception:
            raise ValueError("Invalid date format, expected YYYY-MM-DD or ISO date")

def _parse_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(value)
    except Exception:
        try:
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except Exception:
            raise ValueError("Invalid datetime format, expected YYYY-MM-DD HH:MM:SS or ISO datetime")

def create_table_if_not_exists():
    """
    Create fastag_details table if missing.
    Issue_Date is Nullable so inserts won't fail if client omits it.
    """
    create_sql = """
    CREATE TABLE IF NOT EXISTS fastag_details (
        TagId String,
        VRN String,
        Tag_Status String,
        Vehicle_Class String,
        Action String,
        Issue_Date Nullable(Date),
        Issuer_Bank String,
        Last_Update Nullable(DateTime),
        created_on DateTime,
        updated_on DateTime,
        is_current UInt8,
        is_changed UInt8,
        dwid Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY TagId
    """
    client.command(create_sql)

# ----------------------------
# 3️⃣ API Endpoint
# ----------------------------
@app.route('/add_fastag', methods=['POST'])
def add_fastag():
    create_table_if_not_exists()

    # Accept JSON even if Content-Type missing
    data = request.get_json(silent=True)
    if data is None:
        raw = request.get_data(as_text=True)
        if raw:
            try:
                data = json.loads(raw)
            except Exception:
                return jsonify({"error": "Invalid or missing JSON payload"}), 400
        else:
            return jsonify({"error": "Invalid or missing JSON payload"}), 400

    # Required fields
    if not data.get("TagId") or not data.get("VRN"):
        return jsonify({"error": "Missing required fields: TagId and VRN"}), 400

    columns = [
        "TagId", "VRN", "Tag_Status", "Vehicle_Class", "Action",
        "Issue_Date", "Issuer_Bank", "Last_Update",
        "created_on", "updated_on", "is_current", "is_changed", "dwid"
    ]

    # store timestamps without seconds (HH:MM:00)
    now = datetime.now().replace(second=0, microsecond=0)

    # parse IssueDate (optional) and LastUpdate (optional)
    try:
        issue_date = _parse_date(data.get("IssueDate"))
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400

    try:
        last_update = _parse_datetime(data.get("LastUpdate"))
    except ValueError:
        last_update = None

    row = [
        data.get("TagId"),
        data.get("VRN"),
        data.get("TagStatus"),
        data.get("VehicleClass"),
        data.get("Action"),
        issue_date,
        data.get("IssuerBank"),
        last_update,
        now,
        now,
        1,
        0,
        None
    ]

    try:
        client.insert("fastag_details", [row], column_names=columns)
        return jsonify({"message": "Data inserted successfully!"}), 201
    except Exception:
        logging.exception("Insert failed")
        return jsonify({"error": "Internal server error"}), 500

# ----------------------------
# 4️⃣ Run Flask Server
# ----------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
