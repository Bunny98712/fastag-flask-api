from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime, date
from clickhouse_connect import get_client
import os
import logging

logging.basicConfig(level=logging.INFO)
app = FastAPI(title="Vehicle Data API", version="1.0.0")

# ----------------------------
# ClickHouse Client
# ----------------------------
client = get_client(
    host=os.getenv("CH_HOST", "localhost"),
    username=os.getenv("CH_USER", "admin"),
    password=os.getenv("CH_PASS", "rishu123"),
    port=int(os.getenv("CH_PORT", "8123")),
    database=os.getenv("CH_DB", "vehicle_fastag")
)

# ----------------------------
# Helper Functions
# ----------------------------
def parse_date(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).date()
        except:
            continue
    try:
        return datetime.fromisoformat(value).date()
    except:
        return None

def parse_datetime(value):
    if not value:
        return None
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(value)
    except:
        return None

def safe_int(value):
    try:
        return int(value) if value not in (None, "") else None
    except:
        return None

def safe_float(value):
    try:
        return float(value) if value not in (None, "") else None
    except:
        return None

def bool_to_uint8(value):
    if value in (True, "true", "1", 1):
        return 1
    return 0

# ----------------------------
# Models
# ----------------------------
class FastagData(BaseModel):
    TagId: str
    VRN: str
    TagStatus: Optional[str] = None
    VehicleClass: Optional[str] = None
    Action: Optional[str] = None
    IssueDate: Optional[str] = None
    IssuerBank: Optional[str] = None
    LastUpdate: Optional[str] = None

class VehicleRCData(BaseModel):
    rc_number: str
    registration_date: Optional[str] = None
    owner_name: Optional[str] = None
    father_name: Optional[str] = None
    present_address: Optional[str] = None
    permanent_address: Optional[str] = None
    mobile_number: Optional[str] = None
    vehicle_category: Optional[str] = None
    vehicle_chasi_number: Optional[str] = None
    vehicle_engine_number: Optional[str] = None
    maker_description: Optional[str] = None
    maker_model: Optional[str] = None
    body_type: Optional[str] = None
    fuel_type: Optional[str] = None
    color: Optional[str] = None
    norms_type: Optional[str] = None
    fit_up_to: Optional[str] = None
    financer: Optional[str] = None
    financed: Optional[str] = None
    insurance_company: Optional[str] = None
    insurance_policy_number: Optional[str] = None
    insurance_upto: Optional[str] = None
    manufacturing_date: Optional[str] = None
    manufacturing_date_formatted: Optional[str] = None
    registered_at: Optional[str] = None
    latest_by: Optional[str] = None
    less_info: Optional[bool] = None
    tax_upto: Optional[str] = None
    tax_paid_upto: Optional[str] = None
    cubic_capacity: Optional[str] = None
    vehicle_gross_weight: Optional[str] = None
    no_cylinders: Optional[str] = None
    seat_capacity: Optional[str] = None
    sleeper_capacity: Optional[str] = None
    standing_capacity: Optional[str] = None
    wheelbase: Optional[str] = None
    unladen_weight: Optional[str] = None
    vehicle_category_description: Optional[str] = None
    pucc_number: Optional[str] = None
    pucc_upto: Optional[str] = None
    permit_number: Optional[str] = None
    permit_issue_date: Optional[str] = None
    permit_valid_from: Optional[str] = None
    permit_valid_upto: Optional[str] = None
    permit_type: Optional[str] = None
    national_permit_number: Optional[str] = None
    national_permit_upto: Optional[str] = None
    national_permit_issued_by: Optional[str] = None
    non_use_status: Optional[int] = None
    non_use_from: Optional[str] = None
    non_use_to: Optional[str] = None
    blacklist_status: Optional[str] = None
    noc_details: Optional[str] = None
    owner_number: Optional[str] = None
    rc_status: Optional[str] = None
    masked_name: Optional[bool] = None
    variant: Optional[str] = None
    permanent_Pincode: Optional[str] = None
    is_luxuryMover: Optional[str] = None
    make_Name: Optional[str] = None
    model_Name: Optional[str] = None
    variant_Name: Optional[str] = None
    statusAsOn: Optional[str] = None
    isCommercial: Optional[str] = None
    manufacture_Year: Optional[str] = None
    purchase_Date: Optional[str] = None
    rto_Code: Optional[str] = None
    rto_Name: Optional[str] = None
    regAuthority: Optional[str] = None
    rcStandardCap: Optional[str] = None
    blacklistDetails: Optional[str] = None
    dbResult: Optional[str] = None
    result: Optional[str] = None
    recommended_Vehicle: Optional[str] = None
    carVariant: Optional[str] = None
    cityofRegitration: Optional[str] = None
    cityofRegitrationId: Optional[str] = None
    manufactureMonth: Optional[str] = None
    expiryDuration: Optional[str] = None
    city: Optional[str] = None
    year: Optional[str] = None
    status: Optional[str] = None

# ----------------------------
# Table Creation
# ----------------------------
def create_fastag_table_if_not_exists():
    client.command("""
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
    """)

def create_rc_table_if_not_exists():
    client.command("""
        CREATE TABLE IF NOT EXISTS vehicle_rc_v10 (
            rc_number String,
            registration_date Nullable(Date),
            owner_name String,
            father_name String,
            present_address String,
            permanent_address String,
            mobile_number String,
            vehicle_category String,
            vehicle_chasi_number String,
            vehicle_engine_number String,
            maker_description String,
            maker_model String,
            body_type String,
            fuel_type String,
            color String,
            norms_type String,
            fit_up_to Nullable(Date),
            financer String,
            financed String,
            insurance_company String,
            insurance_policy_number String,
            insurance_upto Nullable(Date),
            manufacturing_date String,
            manufacturing_date_formatted String,
            registered_at String,
            latest_by Nullable(DateTime),
            less_info UInt8,
            tax_upto Nullable(Date),
            tax_paid_upto Nullable(Date),
            cubic_capacity Nullable(Float32),
            vehicle_gross_weight Nullable(Float32),
            no_cylinders Nullable(UInt8),
            seat_capacity Nullable(UInt8),
            sleeper_capacity String,
            standing_capacity String,
            wheelbase String,
            unladen_weight String,
            vehicle_category_description String,
            pucc_number String,
            pucc_upto Nullable(Date),
            permit_number String,
            permit_issue_date String,
            permit_valid_from String,
            permit_valid_upto String,
            permit_type String,
            national_permit_number String,
            national_permit_upto Nullable(Date),
            national_permit_issued_by String,
            non_use_status Nullable(UInt8),
            non_use_from Nullable(Date),
            non_use_to Nullable(Date),
            blacklist_status String,
            noc_details String,
            owner_number String,
            rc_status String,
            masked_name UInt8,
            variant Nullable(String),
            permanent_Pincode String,
            is_luxuryMover String,
            make_Name String,
            model_Name String,
            variant_Name String,
            statusAsOn String,
            isCommercial String,
            manufacture_Year String,
            purchase_Date String,
            rto_Code String,
            rto_Name String,
            regAuthority String,
            rcStandardCap String,
            blacklistDetails String,
            dbResult String,
            result String,
            recommended_Vehicle String,
            carVariant String,
            cityofRegitration String,
            cityofRegitrationId String,
            manufactureMonth String,
            expiryDuration String,
            city String,
            year String,
            status String,
            created_on DateTime,
            updated_on DateTime
        ) ENGINE = MergeTree()
        ORDER BY rc_number
    """)

# ----------------------------
# Endpoints
# ----------------------------
@app.get("/")
async def health():
    return {"status": "ok", "service": "Vehicle Data API", "endpoints": ["/add_fastag", "/add_vehicle_rc"]}

@app.post("/add_fastag")
async def add_fastag(data: FastagData):
    create_fastag_table_if_not_exists()
    now = datetime.now()
    # Duplicate check
    if client.query(f"SELECT count() FROM fastag_details WHERE TagId='{data.TagId}' AND VRN='{data.VRN}'").result_rows[0][0] > 0:
        raise HTTPException(status_code=409, detail="Duplicate FASTag entry")
    
    row = [
        data.TagId,
        data.VRN,
        data.TagStatus or "",
        data.VehicleClass or "",
        data.Action or "",
        parse_date(data.IssueDate),
        data.IssuerBank or "",
        parse_datetime(data.LastUpdate),
        now, now, 1, 0, None
    ]
    client.insert("fastag_details", [row], column_names=[
        "TagId","VRN","Tag_Status","Vehicle_Class","Action","Issue_Date",
        "Issuer_Bank","Last_Update","created_on","updated_on","is_current","is_changed","dwid"
    ])
    return {"message": "FASTag data inserted successfully", "TagId": data.TagId, "VRN": data.VRN}

@app.post("/add_vehicle_rc")
async def add_vehicle_rc(data: VehicleRCData):
    create_rc_table_if_not_exists()
    now = datetime.now()
    
    # Duplicate check
    if client.query(f"SELECT count() FROM vehicle_rc_v10 WHERE rc_number='{data.rc_number}'").result_rows[0][0] > 0:
        raise HTTPException(status_code=409, detail="Duplicate RC entry")

    # Prepare row
    row = []
    for field_name in list(VehicleRCData.__fields__.keys()):
        value = getattr(data, field_name)
        if field_name in ["registration_date","fit_up_to","insurance_upto","tax_upto","tax_paid_upto","pucc_upto","national_permit_upto","non_use_from","non_use_to"]:
            row.append(parse_date(value))
        elif field_name in ["latest_by"]:
            row.append(parse_datetime(value))
        elif field_name in ["cubic_capacity","vehicle_gross_weight"]:
            row.append(safe_float(value))
        elif field_name in ["no_cylinders","seat_capacity","non_use_status"]:
            row.append(safe_int(value))
        elif field_name in ["less_info","masked_name"]:
            row.append(bool_to_uint8(value))
        else:
            row.append(value or "")
    
    # Add timestamps
    row.extend([now, now])
    
    columns = list(VehicleRCData.__fields__.keys()) + ["created_on", "updated_on"]
    client.insert("vehicle_rc_v10", [row], column_names=columns)
    return {"message": "RC data inserted successfully", "rc_number": data.rc_number}


# ----------------------------
# 6. Run the API
# ----------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
