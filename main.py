# main.py
import os, json, datetime
import pandas as pd
from pytrends.request import TrendReq

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# ---------- 1) NHẬN CẤU HÌNH TỪ ENV/VARS ----------
SHEET_ID   = os.environ["SHEET_ID"]
SHEET_TAB  = os.environ.get("SHEET_TAB", "Trends")
GEO        = os.environ.get("GEO", "US")
KEYWORDS   = os.environ.get("KEYWORDS", "메디큐브,코스알엑스,아누아,조선미녀,K-SECRET,ARENCIA,MIXSOON")

# previous month [start, end] nếu không truyền sẵn
def previous_month_range(today=None):
    if today is None:
        today = datetime.date.today()
    first_this_month = today.replace(day=1)
    last_prev_month = first_this_month - datetime.timedelta(days=1)
    first_prev_month = last_prev_month.replace(day=1)
    return first_prev_month, last_prev_month

start_date, end_date = previous_month_range()
timeframe = f"{start_date} {end_date}"    # 'YYYY-MM-DD YYYY-MM-DD'

# ---------- 2) TẠO CRED TỪ SECRET JSON ----------
svc_json = os.environ["GCP_SERVICE_ACCOUNT"]
svc_info = json.loads(svc_json)
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_info(svc_info, scopes=scopes)

gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)
try:
    ws = sh.worksheet(SHEET_TAB)
except gspread.WorksheetNotFound:
    ws = sh.add_worksheet(title=SHEET_TAB, rows="100", cols="20")

# ---------- 3) GỌI GOOGLE TRENDS ----------
kw_list = [k.strip() for k in KEYWORDS.split(",") if k.strip()]
pytrends = TrendReq(hl="en-US", tz=360)

pytrends.build_payload(
    kw_list=kw_list,
    cat=0,
    timeframe=timeframe,   # ví dụ "2024-08-01 2024-08-31"
    geo=GEO,               # ví dụ "US"
    gprop=""               # web search
)

df = pytrends.interest_over_time()

# Bỏ cột isPartial nếu có
if "isPartial" in df.columns:
    df = df.drop(columns=["isPartial"])

df = df.reset_index()  # đưa cột 'date' ra ngoài index

# Gắn thêm metadata để tiện lọc trong sheet
df["geo"]         = GEO
df["period_start"] = str(start_date)
df["period_end"]   = str(end_date)
df["run_at_utc"]   = datetime.datetime.utcnow().isoformat(timespec="seconds")

# ---------- 4) GHI VÀO GOOGLE SHEET ----------
# Nếu sheet đang trống -> ghi cả header. Nếu có dữ liệu -> append bên dưới
existing = ws.get_all_values()
start_cell = "A1" if len(existing) == 0 else f"A{len(existing)+1}"
set_with_dataframe(ws, df, row=int(start_cell[1:]), include_column_header=(len(existing) == 0))

print(f"Done. Wrote {len(df)} rows for timeframe {timeframe} to sheet '{SHEET_TAB}' (geo={GEO}).")

