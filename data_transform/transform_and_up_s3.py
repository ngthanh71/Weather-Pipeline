import os
import json
from datetime import datetime
import pandas as pd
import boto3
from dotenv import load_dotenv
from pymongo import MongoClient
from bson.json_util import dumps, RELAXED_JSON_OPTIONS

env_paths = [
    "/opt/airflow/config.env",          
    os.path.join(os.path.dirname(__file__), "..", "config.env"),
    "config.env",
]
for p in env_paths:
    if os.path.exists(p):
        load_dotenv(p)
        print(f"Loaded env from: {p}")
        break

uri = os.getenv("MONGODB_URI") or "mongodb://mongo:27017/"

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY') or None
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY') or None
S3_BUCKET = os.getenv('S3_BUCKET') or None
AWS_REGION = (os.getenv('AWS_REGION') or 'ap-southeast-2')

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY or not S3_BUCKET:
    print("S3 failed")


try:
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print("connect mongo success")
except Exception as e:
    print(f"mongo error {e}")
    raise

db = client["DataWeather"]
col = db["forecast"]

s3 = None
if AWS_ACCESS_KEY and AWS_SECRET_KEY and S3_BUCKET:
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
    except Exception as e:
        print(f"S3 error {e}")
        s3 = None
else:
    print("S3 creat failed")

doc = col.find_one(sort=[("_id", -1)], projection={"_id": 0})
print(dumps(doc, json_options=RELAXED_JSON_OPTIONS, indent=2, ensure_ascii=False))

CURRENT_COLS = [
    "last_updated_epoch", "last_updated",
    "temp_c", "temp_f", "is_day",
    "wind_mph", "wind_kph", "wind_degree", "wind_dir",
    "pressure_mb", "pressure_in",
    "precip_mm", "precip_in",
    "humidity", "cloud",
    "feelslike_c", "feelslike_f",
    "windchill_c", "windchill_f",
    "heatindex_c", "heatindex_f",
    "dewpoint_c", "dewpoint_f",
    "vis_km", "vis_miles",
    "uv",
    "gust_mph", "gust_kph",
    "short_rad", "diff_rad", "dni", "gti"
]

def to_row(doc: dict) -> dict:
    loc = (doc or {}).get("location", {}) or {}
    cur = (doc or {}).get("current", {}) or {}

    row = {
        "location": loc.get("name"),
        "country": loc.get("country"),
        "lat":     loc.get("lat"),
        "lon":     loc.get("lon"),
        "tz_id":   loc.get("tz_id"),
        "localtime":        loc.get("localtime"),
        "localtime_epoch":  loc.get("localtime_epoch"),
    }

    for k in CURRENT_COLS:
        row[k] = cur.get(k)

    return row

row = to_row(doc)

print(json.dumps(row, indent=2, ensure_ascii=False))

df = pd.DataFrame([row])

raw_time = row.get("localtime") or row.get("last_updated")
filename_base = None
if raw_time:
    try:
        dt = pd.to_datetime(raw_time, errors="coerce")
        if pd.notna(dt):
            if getattr(dt, "tzinfo", None) is not None:
                try:
                    dt = dt.tz_convert("Asia/Bangkok")
                except Exception:
                    pass
            filename_base = dt.strftime("%Y%m%d%H%M")
    except Exception:
        pass

if not filename_base:
    filename_base = datetime.utcnow().strftime("%Y%m%d%H%M%S")

file_name = f"{filename_base}.csv"
date_str = datetime.utcnow().strftime("%Y-%m-%d")
s3_path = f"processed/{date_str}/{file_name}"

local_dir = os.path.join(os.getcwd(), "data_to_train")
os.makedirs(local_dir, exist_ok=True)
local_file_path = os.path.join(local_dir, file_name)
df.to_csv(local_file_path, index=False)
print(f"File CSV: {local_file_path}")

if s3:
    try:
        s3.upload_file(local_file_path, S3_BUCKET, s3_path)
        print(f" Upload s3://{S3_BUCKET}/{s3_path}")
    except Exception as e:
        import traceback
        print("Upload failed")
        traceback.print_exc()
else:
    print("Upload failed")