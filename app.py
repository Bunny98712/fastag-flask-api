from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
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
    
def parse_datetime(value: Optional[str]):
    if not value:
        return None
    v = value.strip()
    # Accept bare date by appending midnight if needed
    if len(v) == 10 and v.count("-") == 2:  # "YYYY-MM-DD"
        v = v + " 00:00:00"
    # Normalize trailing Z (UTC) to +00:00 only if present
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(v)
    except Exception:
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
    registration_date: str = None
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
    vehicle_gross_weight: Optional[int] = None
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

class ViolationDetails(BaseModel):
    offence: str
    penalty: Optional[str] = None

class ChallanRecord(BaseModel):
    forChallan: Optional[str] = None
    typeAccused: Optional[str] = None
    nameViolator: Optional[str] = None
    violatorFatherName: Optional[str] = None
    violatorContactNo: Optional[str] = None
    dlRcNumber: Optional[str] = None
    challanNo: Optional[str] = None
    State: Optional[str] = None
    dateChallan: Optional[str] = None
    detailsViolation: Optional[List[ViolationDetails]] = None
    investigateUnder: Optional[str] = None
    longLat: Optional[str] = None
    locationChallan: Optional[str] = None
    remarkChallan: Optional[str] = None
    typeBook: Optional[str] = None
    bookNo: Optional[str] = None
    formNo: Optional[str] = None
    witness1: Optional[str] = None
    witness2: Optional[str] = None
    witness3: Optional[str] = None
    imagesChallan: Optional[str] = None
    imageVehicle: Optional[str] = None
    imageCCTV1: Optional[str] = None
    imageCCTV2: Optional[str] = None
    numberDL: Optional[str] = None
    detailsDL: Optional[str] = None
    suspendISDL: Optional[str] = None
    accNameDL: Optional[str] = None
    accAddressDL: Optional[str] = None
    accFatherNameDL: Optional[str] = None
    accAgeDL: Optional[str] = None
    accGenderDL: Optional[str] = None
    validityDL: Optional[str] = None
    issueDateDL: Optional[str] = None
    issuedByDL: Optional[str] = None
    amountChallan: Optional[int] = None
    status: Optional[str] = None
    sourcePayment: Optional[str] = None
    datePayment: Optional[str] = None
    IDTransaction: Optional[str] = None
    noReceipt: Optional[str] = None
    noReceiptOffline: Optional[str] = None
    receiptOffline: Optional[str] = None
    noMobile: Optional[str] = None
    byPayment: Optional[str] = None
    acfIS: Optional[str] = None
    amountACF: Optional[int] = None
    noReceiptACF: Optional[str] = None
    nameRTO: Optional[str] = None
    impoundDocument: Optional[str] = None
    impoundVehicle: Optional[str] = None
    classVehicle: Optional[str] = None
    typeVehicle: Optional[str] = None
    uptoVehicle: Optional[str] = None
    uptoPermit: Optional[str] = None
    rcNo: Optional[str] = None
    noChassis: Optional[str] = None
    noEngine: Optional[str] = None
    noVehOwner: Optional[str] = None
    nameOwner: Optional[str] = None
    nameFatherOwner: Optional[str] = None
    addressOwner: Optional[str] = None
    idCourt: Optional[str] = None
    statusCourt: Optional[str] = None
    idCourtRelated: Optional[str] = None
    imgOrderRelease: Optional[str] = None
    dateRelease: Optional[str] = None
    noReceiptCourt: Optional[str] = None
    byAction: Optional[str] = None
    noDispatch: Optional[str] = None
    nameCourt: Optional[str] = None
    chargesUser: Optional[str] = None
    challan_search_source: Optional[str] = None
    court_status_desc: Optional[str] = None

class VehicleRCBlackList(BaseModel):
    regNo:  str
    stateCode:  Optional[str] = None
    regDate:  Optional[str] = None
    vehicleClass:  Optional[str] = None
    classCode:  Optional[str] = None
    model:  Optional[str] = None
    fuelType:  Optional[str] = None
    owner:  Optional[str] = None
    rcExpiryDate: Optional[str] = None
    vehicleTaxUpto: Optional[str] = None
    emissionNorms: Optional[str] = None
    normsCode: Optional[str] = None
    insurance_companyName: Optional[str] = None
    insurance_validUpto: Optional[str] = None
    financier_name: Optional[str] = None
    financedFrom: Optional[str] = None
    registrationAuthority: Optional[str] = None
    puccUpto: Optional[str] = None
    blacklistStatus: str
    nocDetails: Optional[str] = None
    status: Optional[str] = None
    statusAsOn: str


class VehicleChallanAllState(BaseModel):
    number: int
    challanNumber: Optional[str] = None
    offenseDetails: Optional[str] = None
    challanPlace: Optional[str] = None
    payment_url: Optional[str] = None
    image_url: Optional[str] = None
    challanDate: Optional[str] = None
    state: Optional[str] = None
    rto: Optional[str] = None
    accusedName: Optional[str] = None
    accused_father_name: Optional[str] = None
    amount: int
    challanStatus: Optional[str] = None
    court_status: Optional[str] = None  


class RcChassis(BaseModel):
    vehicle_num: str  


class Mahindraservice(BaseModel):
    chassis_no: Optional[str] = None
    location_code: Optional[str] = None
    location_name: Optional[str] = None
    mileage: Optional[str] = None
    net_bill_amt: Optional[str] = None
    online_payment_flag: Optional[str] = None
    out_standing_amt: Optional[str] = None
    paid_amt: Optional[str] = None
    dealer_code: Optional[str] = None
    dealer_name: Optional[str] = None
    repair_order_bill_date: Optional[str] = None
    repair_order_bill_no: Optional[str] = None
    svc_date: Optional[str] = None
    repair_order_no: Optional[str] = None
    register_no: Optional[str] = None
    service_assistant_no: Optional[str] = None
    service_assistant_name: Optional[str] = None
    work_type: Optional[str] = None
    status: Optional[str] = None
    service_cate: Optional[str] = None

class VehicleServiceHistory(BaseModel):
    vehicleNumber: str
    serviceHistoryDetails: List[Mahindraservice]





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

def create_vehicle_challan_table_if_not_exists():
    client.command("""
        CREATE TABLE IF NOT EXISTS vehicle_challan (
            forChallan Nullable(String),
            typeAccused Nullable(String),
            nameViolator Nullable(String),
            violatorFatherName Nullable(String),
            violatorContactNo Nullable(String),
            dlRcNumber String,
            challanNo String,
            State String,
            dateChallan Nullable(DateTime),
            detailsViolation Nested(
                offence String,
                penalty Nullable(String)
            ),
            investigateUnder Nullable(String),
            longLat Nullable(String),
            locationChallan Nullable(String),
            remarkChallan Nullable(String),
            typeBook Nullable(String),
            bookNo Nullable(String),
            formNo Nullable(String),
            witness1 Nullable(String),
            witness2 Nullable(String),
            witness3 Nullable(String),
            imagesChallan Nullable(String),
            imageVehicle Nullable(String),
            imageCCTV1 Nullable(String),
            imageCCTV2 Nullable(String),
            numberDL Nullable(String),
            detailsDL Nullable(String),
            suspendISDL Nullable(String),
            accNameDL Nullable(String),
            accAddressDL Nullable(String),
            accFatherNameDL Nullable(String),
            accAgeDL Nullable(String),
            accGenderDL Nullable(String),
            validityDL Nullable(String),
            issueDateDL Nullable(String),
            issuedByDL Nullable(String),
            amountChallan UInt32,
            status String,
            sourcePayment Nullable(String),
            datePayment Nullable(String),
            IDTransaction Nullable(String),
            noReceipt Nullable(String),
            noReceiptOffline Nullable(String),
            receiptOffline Nullable(String),
            noMobile Nullable(String),
            byPayment Nullable(String),
            acfIS Nullable(String),
            amountACF Nullable(UInt32),
            noReceiptACF Nullable(String),
            nameRTO Nullable(String),
            impoundDocument Nullable(String),
            impoundVehicle Nullable(String),
            classVehicle Nullable(String),
            typeVehicle Nullable(String),
            uptoVehicle Nullable(String),
            uptoPermit Nullable(String),
            rcNo String,
            noChassis Nullable(String),
            noEngine Nullable(String),
            noVehOwner Nullable(String),
            nameOwner Nullable(String),
            nameFatherOwner Nullable(String),
            addressOwner Nullable(String),
            idCourt Nullable(String),
            statusCourt Nullable(String),
            idCourtRelated Nullable(String),
            imgOrderRelease Nullable(String),
            dateRelease Nullable(String),
            noReceiptCourt Nullable(String),
            byAction Nullable(String),
            noDispatch Nullable(String),
            nameCourt Nullable(String),
            chargesUser Nullable(String),
            challan_search_source Nullable(String),
            court_status_desc Nullable(String)
        ) ENGINE = MergeTree
        ORDER BY challanNo;
    """)
def create_vehicle_rc_black_list_table_if_not_exists():
    client.command("""
        CREATE TABLE IF NOT EXISTS vehicle_rc_black_list (
            regNo String,
            stateCode String,
            regDate Date,
            vehicleClass String,
            classCode String,
            model String,
            fuelType String,
            owner String,
            rcExpiryDate Date,
            vehicleTaxUpto String,
            emissionNorms String,
            normsCode String,
            insurance_companyName String,
            insurance_validUpto Date,
            financier_name String,
            financedFrom String,
            registrationAuthority String,
            puccUpto String,
            blacklistStatus String,
            nocDetails String,
            status String,
            statusAsOn Date
        ) ENGINE = MergeTree()
        ORDER BY (regNo)
    """) 
 


def create_vehicle_challan_all_state_table_if_not_exists():
    client.command("""
        CREATE TABLE IF NOT EXISTS vehicle_challan_all_state (
            number Int32,
            challanNumber String,
            offenseDetails String,
            challanPlace String,
            payment_url Nullable(String),
            image_url Nullable(String),
            challanDate Date,
            state String,
            rto String,
            accusedName String,
            accused_father_name Nullable(String),
            amount Int32,
            challanStatus String,
            court_status Nullable(String)
        ) ENGINE = MergeTree()
        ORDER BY (number)
    """)

def create_rc_chassis_table_if_not_exists():
    client.command("""
        CREATE TABLE IF NOT EXISTS rc_chassis (
            vehicle_num String
        ) ENGINE = MergeTree()
        ORDER BY (vehicle_num)
    """)

def create_vehicle_service_history_table_if_not_exists():
    client.command("""
        CREATE TABLE IF NOT EXISTS vehicle_service_history (
            vehicleNumber String,
            register_no Nullable(String),
            repair_order_no String DEFAULT '',
            repair_order_bill_no Nullable(String),
            chassis_no Nullable(String),
            location_code Nullable(String),
            location_name Nullable(String),
            dealer_code Nullable(String),
            dealer_name Nullable(String),
            svc_date Date DEFAULT toDate(0),
            repair_order_bill_date Nullable(Date),
            mileage Nullable(UInt32),
            net_bill_amt Nullable(Float64),
            out_standing_amt Nullable(Float64),
            paid_amt Nullable(Float64),
            online_payment_flag Nullable(String),
            service_assistant_no Nullable(String),
            service_assistant_name Nullable(String),
            work_type Nullable(String),
            status Nullable(String),
            service_cate Nullable(String),
            created_on DateTime DEFAULT now(),
            updated_on DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (vehicleNumber, svc_date, repair_order_no)
        SETTINGS index_granularity = 8192
    """)







# ----------------------------
# Endpoints
# ----------------------------
@app.get("/")
async def health():
    return {"status": "ok", "service": "Vehicle Data API", "endpoints": ["/add_fastag", "/add_vehicle_rc", "/add_challan_record",
    "/add_vehicle_rc_black_list" ,"/add_vehicle_challan_all_state", "/add_rc_chassis", "/add_mahindra_service"]}


  ##### Vehicle Fastag Detailed V1 API ######

@app.post("/add_fastag")
async def add_fastag(data: FastagData):
    create_fastag_table_if_not_exists()
    now = datetime.now()
    # Duplicate check
#    # if client.query(f"SELECT count() FROM fastag_details WHERE TagId='{data.TagId}' AND VRN='{data.VRN}'").result_rows[0][0] > 0:
#         raise HTTPException(status_code=409, detail="Duplicate FASTag entry")

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

##### Vehicle RC V10 (Additional Details) ######

@app.post("/add_vehicle_rc")
async def add_vehicle_rc(data: VehicleRCData):
    create_rc_table_if_not_exists()
    now = datetime.now()
    # Duplicate check
    # if client.query(f"SELECT count() FROM vehicle_rc_v10 WHERE rc_number='{data.rc_number}'").result_rows[0][0] > 0:
    #     raise HTTPException(status_code=409, detail="Duplicate RC entry")

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
    row.extend([now, now])
    columns = list(VehicleRCData.__fields__.keys()) + ["created_on", "updated_on"]
    client.insert("vehicle_rc_v10", [row], column_names=columns)
    return {"message": "RC data inserted successfully", "rc_number": data.rc_number}


##### Vehicle Challan Detailed API ######


@app.post("/add_challan_record")
async def add_challan_record(data: ChallanRecord):
    create_vehicle_challan_table_if_not_exists()

    # Ensure nested arrays are not None
    dv = data.detailsViolation or []
    detailsViolation_offence = [v.offence or "" for v in dv]
    detailsViolation_penalty = [v.penalty if v.penalty is not None else "" for v in dv]

    # Parse non-nullable DateTime; fail fast if missing/invalid
    dt = parse_datetime(data.dateChallan)
    if dt is None:
     raise HTTPException(
        status_code=422,
        detail="dateChallan must be 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS' (optionally with Z or +HH:MM)"
    )


   

    row = [
        data.forChallan or None,                
        data.typeAccused or None,                
        data.nameViolator or "",                
        data.violatorFatherName or None,         
        data.violatorContactNo or None,          
        data.dlRcNumber or "",                   
        data.challanNo or "",                    
        data.State or "",                        
        dt,                                      
        detailsViolation_offence,               
        detailsViolation_penalty,                
        data.investigateUnder or None,
        data.longLat or None,
        data.locationChallan or None,
        data.remarkChallan or None,
        data.typeBook or None,
        data.bookNo or None,
        data.formNo or None,
        data.witness1 or None,
        data.witness2 or None,
        data.witness3 or None,
        data.imagesChallan or None,
        data.imageVehicle or None,
        data.imageCCTV1 or None,
        data.imageCCTV2 or None,
        data.numberDL or None,
        data.detailsDL or None,
        data.suspendISDL or None,
        data.accNameDL or None,
        data.accAddressDL or None,
        data.accFatherNameDL or None,
        data.accAgeDL or None,
        data.accGenderDL or None,
        data.validityDL or None,
        data.issueDateDL or None,
        data.issuedByDL or None,
        int(data.amountChallan or 0),            
        data.status or "",                      
        data.sourcePayment or None,
        data.datePayment or None,
        data.IDTransaction or None,
        data.noReceipt or None,
        data.noReceiptOffline or None,
        data.receiptOffline or None,
        data.noMobile or None,
        data.byPayment or None,
        data.acfIS or None,
        int(data.amountACF or 0),                
        data.noReceiptACF or None,
        data.nameRTO or None,
        data.impoundDocument or None,
        data.impoundVehicle or None,
        data.classVehicle or None,
        data.typeVehicle or None,
        data.uptoVehicle or None,
        data.uptoPermit or None,
        data.rcNo or "",                         
        data.noChassis or None,
        data.noEngine or None,
        data.noVehOwner or None,
        data.nameOwner or None,
        data.nameFatherOwner or None,
        data.addressOwner or None,
        data.idCourt or None,
        data.statusCourt or None,
        data.idCourtRelated or None,
        data.imgOrderRelease or None,
        data.dateRelease or None,
        data.noReceiptCourt or None,
        data.byAction or None,
        data.noDispatch or None,
        data.nameCourt or None,
        data.chargesUser or None,
        data.challan_search_source or None,
        data.court_status_desc or None,
    ]

    client.insert("vehicle_challan", [row], column_names=[
        "forChallan","typeAccused","nameViolator","violatorFatherName","violatorContactNo",
        "dlRcNumber","challanNo","State","dateChallan",
        "detailsViolation.offence","detailsViolation.penalty",
        "investigateUnder","longLat","locationChallan","remarkChallan","typeBook","bookNo","formNo",
        "witness1","witness2","witness3","imagesChallan","imageVehicle","imageCCTV1","imageCCTV2",
        "numberDL","detailsDL","suspendISDL","accNameDL","accAddressDL","accFatherNameDL","accAgeDL",
        "accGenderDL","validityDL","issueDateDL","issuedByDL","amountChallan","status","sourcePayment",
        "datePayment","IDTransaction","noReceipt","noReceiptOffline","receiptOffline","noMobile","byPayment",
        "acfIS","amountACF","noReceiptACF","nameRTO","impoundDocument","impoundVehicle","classVehicle",
        "typeVehicle","uptoVehicle","uptoPermit","rcNo","noChassis","noEngine","noVehOwner",
        "nameOwner","nameFatherOwner","addressOwner","idCourt","statusCourt","idCourtRelated","imgOrderRelease",
        "dateRelease","noReceiptCourt","byAction","noDispatch","nameCourt","chargesUser",
        "challan_search_source","court_status_desc"
    ])
    return {"message": "Challan record inserted successfully", "challanNo": data.challanNo}


####### Vehicle RC - Blacklist Status & Insurance Check #####

@app.post("/add_vehicle_rc_black_list")
async def add_vehicle_rc_black_list(data: VehicleRCBlackList):
    create_vehicle_rc_black_list_table_if_not_exists()
    # Duplicate check
    # if client.query(f"SELECT count() FROM vehicle_rc_black_list WHERE regNo='{data.regNo}'").result_rows[0][0] > 0:
    #     raise HTTPException(status_code=409, detail="Duplicate blacklist entry")
    row = [
        data.regNo,
        data.stateCode,
        parse_date(data.regDate),
        data.vehicleClass,
        data.classCode,
        data.model,
        data.fuelType,
        data.owner,
        parse_date(data.rcExpiryDate),
        data.vehicleTaxUpto,
        data.emissionNorms,
        data.normsCode,
        data.insurance_companyName,
        parse_date(data.insurance_validUpto),
        data.financier_name,
        data.financedFrom,
        data.registrationAuthority,
        data.puccUpto,
        data.blacklistStatus,
        data.nocDetails,
        data.status,
        parse_date(data.statusAsOn)
    ]
    client.insert("vehicle_rc_black_list", [row], column_names=[
        "regNo", "stateCode", "regDate", "vehicleClass", "classCode", "model",
        "fuelType", "owner", "rcExpiryDate", "vehicleTaxUpto", "emissionNorms", "normsCode",
        "insurance_companyName", "insurance_validUpto", "financier_name", "financedFrom",
        "registrationAuthority", "puccUpto", "blacklistStatus", "nocDetails", "status", "statusAsOn"
    ])
    return {"message": "RC blacklist entry inserted successfully", "regNo": data.regNo}

#######  Vehicle Challan with all States and Interceptor Challans #####

@app.post("/add_vehicle_challan_all_state")
async def add_vehicle_challan_all_state(data: VehicleChallanAllState):
    create_vehicle_challan_all_state_table_if_not_exists()
    # Duplicate check: assumes challanNumber is unique
    #if client.query(f"SELECT count() FROM vehicle_challan_all_state WHERE challanNumber='{data.challanNumber}'").result_rows[0][0] > 0:
        #raise HTTPException(status_code=409, detail="Duplicate challanNumber entry")
    row = [
        data.number,
        data.challanNumber,
        data.offenseDetails,
        data.challanPlace,
        data.payment_url,
        data.image_url,
        parse_date(data.challanDate),
        data.state,
        data.rto,
        data.accusedName,
        data.accused_father_name,
        data.amount,
        data.challanStatus,
        data.court_status
    ]
    client.insert("vehicle_challan_all_state", [row], column_names=[
        "number", "challanNumber", "offenseDetails", "challanPlace", "payment_url", "image_url",
        "challanDate", "state", "rto", "accusedName", "accused_father_name",
        "amount", "challanStatus", "court_status"
    ])
    return {"message": "Challan data inserted successfully", "challanNumber": data.challanNumber}


##### for Reverse RC Chassis to RC Live API #############

@app.post("/add_rc_chassis")
async def add_rc_chassis(data: RcChassis):
    create_rc_chassis_table_if_not_exists()
    # Duplicate check
    # if client.query(f"SELECT count() FROM rc_chassis WHERE vehicle_num='{data.vehicle_num}'").result_rows[0][0] > 0:
    #     raise HTTPException(status_code=409, detail="Duplicate vehicle_num entry")
    row = [
        data.vehicle_num
    ]
    client.insert("rc_chassis", [row], column_names=[
        "vehicle_num"
    ])
    return {"message": "RC chassis data inserted successfully", "vehicle_num": data.vehicle_num}

### Vehicle Mahindra Service History API #####

@app.post("/add_mahindra_service")
async def add_mahindra_service(data: VehicleServiceHistory):
    create_vehicle_service_history_table_if_not_exists()
    now = datetime.now()
    rows = []
    for service in data.serviceHistoryDetails:
        row = [
            data.vehicleNumber or "",                          
            (service.register_no or None),
            (service.repair_order_no or None),
            (service.repair_order_bill_no or None),
            (service.chassis_no or None),
            (service.location_code or None),
            (service.location_name or None),
            (service.dealer_code or None),
            (service.dealer_name or None),
            parse_date(service.svc_date),
            parse_date(service.repair_order_bill_date),
            (service.mileage),
            (service.net_bill_amt),
            (service.out_standing_amt),
            (service.paid_amt),
            (service.online_payment_flag or None),
            (service.service_assistant_no or None),
            (service.service_assistant_name or None),
            (service.work_type or None),
            (service.status or None),
            (service.service_cate or None),
            now,
            now,
        ]
        rows.append(row)

    if not rows:
        return {"message": "No service records to insert", "vehicleNumber": data.vehicleNumber}

    client.insert(
        "vehicle_service_history",
        rows,
        column_names=[
            "vehicleNumber",
            "register_no",
            "repair_order_no",
            "repair_order_bill_no",
            "chassis_no",
            "location_code",
            "location_name",
            "dealer_code",
            "dealer_name",
            "svc_date",
            "repair_order_bill_date",
            "mileage",
            "net_bill_amt",
            "out_standing_amt",
            "paid_amt",
            "online_payment_flag",
            "service_assistant_no",
            "service_assistant_name",
            "work_type",
            "status",
            "service_cate",
            "created_on",
            "updated_on",
        ],
    )

    return {
        "message": "Mahindra service history inserted successfully",
        "vehicleNumber": data.vehicleNumber,
    }









# ----------------------------
# Run the API
# ----------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=5000, reload=True)
