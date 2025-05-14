from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
from datetime import datetime
import bcrypt
import requests
import os

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
BUCKET = "atpsolar_login"

app = FastAPI()

class LoginRequest(BaseModel):
    email: str
    password: str

class RegisterRequest(BaseModel):
    email: str
    password: str
    phone: str
    role: str = "user"

@app.post("/login")
def login(data: LoginRequest):
    flux = f'''
from(bucket: "{BUCKET}")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "user")
  |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> filter(fn: (r) => r.email == "{data.email}")
'''

    resp = requests.post(
        f"{INFLUX_URL}/api/v2/query?org={INFLUX_ORG}",
        headers={
            "Authorization": f"Token {INFLUX_TOKEN}",
            "Content-Type": "application/vnd.flux"
        },
        data=flux
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail="Lỗi truy vấn InfluxDB")

    records = resp.text.splitlines()
    hash_line = [line for line in records if "_field,password_hash" in line]
    if not hash_line:
        raise HTTPException(status_code=401, detail="Tài khoản không tồn tại")

    stored_hash = hash_line[-1].split(",")[-1]
    if not bcrypt.checkpw(data.password.encode(), stored_hash.encode()):
        raise HTTPException(status_code=401, detail="Sai mật khẩu")

    return {"token": f"mock-token-for-{data.email}"}

@app.post("/register")
def register(data: RegisterRequest):
    hashed = bcrypt.hashpw(data.password.encode(), bcrypt.gensalt()).decode()
    timestamp = int(datetime.utcnow().timestamp() * 1e9)
    line = f'user,email={data.email} role="{data.role}",password_hash="{hashed}",phone="{data.phone}" {timestamp}'

    write_resp = requests.post(
        f"{INFLUX_URL}/api/v2/write?org={INFLUX_ORG}&bucket={BUCKET}&precision=ns",
        headers={"Authorization": f"Token {INFLUX_TOKEN}"},
        data=line
    )
    if write_resp.status_code != 204:
        raise HTTPException(status_code=500, detail="Không ghi được dữ liệu")

    return {"status": "success", "email": data.email}

@app.get("/users")
def get_users():
    flux = f"""from(bucket: "{BUCKET}")
      |> range(start: -90d)
      |> filter(fn: (r) => r._measurement == "user")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> keep(columns: ["email", "role", "phone", "_time"])
    """
    resp = requests.post(
        f"{INFLUX_URL}/api/v2/query?org={INFLUX_ORG}",
        headers={
            "Authorization": f"Token {INFLUX_TOKEN}",
            "Content-Type": "application/vnd.flux"
        },
        data=flux
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail="Lỗi truy vấn InfluxDB")

    results = []
    lines = resp.text.strip().splitlines()
    headers = lines[0].split(",")
    for line in lines[1:]:
        values = line.split(",")
        row = dict(zip(headers, values))
        results.append({
            "email": row.get("email", ""),
            "role": row.get("role", ""),
            "phone": row.get("phone", ""),
            "created": row.get("_time", "")
        })
    return results

@app.post("/users/delete")
def delete_user(email: str = Body(...)):
    flux = f'''
    import "influxdata/influxdb/v1"
    v1.delete(
      bucket: "{BUCKET}",
      predicate: (r) => r._measurement == "user" and r.email == "{email}",
      start: time(v: 0),
      stop: now()
    )
    '''
    resp = requests.post(
        f"{INFLUX_URL}/api/v2/query?org={INFLUX_ORG}",
        headers={
            "Authorization": f"Token {INFLUX_TOKEN}",
            "Content-Type": "application/vnd.flux"
        },
        data=flux
    )
    if resp.status_code != 200:
        raise HTTPException(status_code=500, detail="Xoá thất bại")
    return {"status": "deleted", "email": email}
