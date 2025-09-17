import os
import json
from datetime import date, timedelta
import pandas as pd

from pytrends.request import TrendReq
from google.oauth2 import service_account
from googleapiclient.discovery import build


# ---------- Config ----------
# 1) Lấy Sheet ID từ biến môi trường (đã set trong GitHub Secret SHEET_ID)
SHEET_ID = os.environ["SHEET_ID"]  # sẽ lỗi KeyError nếu chưa map env

# 2) Đường dẫn file key service account mà workflow đã tạo
CREDS_JSON = "creds.json"

# 3) Danh sách keyword/brand (Hàn hoặc EN đều được)
BRANDS = [
    "메디큐브",      # Medicube
    "코스알엑스",     # COSRX
    "아누아",        # ANUA
    "조선미녀",      # Beauty of Joseon
    "K-SECRET",
    "ARENCIA",
    "MIXSOON",
]

# 4) Cấu hình quốc gia/thiết bị cho Google Trends
GEO = "US"   # United States
GPROP = "web"  # "web" | "images" | "news" | "youtube" | "froogle"
# ---------------------------


def prev_month_range(today: date | None = None) -> tuple[str, str, str]:
    """
    Trả về (yyyy-mm, start_iso, end_iso) của THÁNG TRƯỚC.
    Ví dụ hôm nay 2025-09-17 -> "2025-08", "2025-08-01", "2025-08-31"
    """
    if today is None:
        today = date.today()
    first_this = today.replace(day=1)
    last_prev = first_this - timedelta(days=1)
    first_prev = last_prev.replace(day=1)
    ym = f"{first_prev:%Y-%m}"
    return ym, f"{first_prev:%Y-%m-%d}", f"{last_prev:%Y-%m-%d}"


def auth_sheets(creds_path: str):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    credentials = service_account.Credentials.from_service_account_file(
        creds_path, scopes=scopes
    )
    return build("sheets", "v4", credentials=credentials).spreadsheets()


def trends_monthly_dataframe(brands: list[str], geo: str, gprop: str,
                             start_date: str, end_date: str) -> pd.DataFrame:
    """
    Lấy Interest over time trong khoảng (start_date, end_date) cho list từ khóa.
    start_date/end_date dạng 'YYYY-MM-DD'.
    """
    # Google Trends dùng định dạng: YYYY-MM-DD YYYY-MM-DD
    timeframe = f"{start_date} {end_date}"

    # Khởi tạo pytrends (ngôn ngữ & timezone mặc định)
    pytrends = TrendReq(hl="en-US", tz=0)

    # Pytrends hỗ trợ tối đa ~5 keyword/lượt, nên ta sẽ chunk
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    frames = []
    for group in chunks(brands, 5):
        pytrends.build_payload(group, timeframe=timeframe, geo=geo, gprop=gprop)
        df = pytrends.interest_over_time()
        if df.empty:
            continue
        # Bỏ cột isPartial (không cần)
        if "isPartial" in df.columns:
            df = df.drop(columns=["isPartial"])
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    # Gộp theo index (ngày) rồi giữ max theo cột trùng (nếu có)
    full = pd.concat(frames, axis=1)
    full = full.groupby(level=0, axis=1).max()
    full.reset_index(inplace=True)
    full.rename(columns={"date": "Date"}, inplace=True)
    return full


def write_to_sheet(svc, sheet_id: str, tab_name: str, df: pd.DataFrame):
    """
    Ghi DataFrame vào tab `tab_name`. Nếu tab đã tồn tại, sẽ clear trước khi ghi.
    """
    # 1) Tạo sheet nếu chưa có
    meta = svc.get(spreadsheetId=sheet_id).execute()
    sheets = [s["properties"]["title"] for s in meta.get("sheets", [])]
    requests = []
    if tab_name not in sheets:
        requests.append({
            "addSheet": {"properties": {"title": tab_name}}
        })
    else:
        # Clear dữ liệu cũ
        svc.values().clear(
            spreadsheetId=sheet_id,
            range=f"{tab_name}!A:Z"
        ).execute()

    if requests:
        svc.batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": requests}
        ).execute()

    # 2) Chuẩn bị values (header + rows)
    values = [df.columns.tolist()] + df.values.tolist()
    svc.values().update(
        spreadsheetId=sheet_id,
        range=f"{tab_name}!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


def main():
    ym, start_d, end_d = prev_month_range()

    # 1) Lấy dữ liệu trends
    df = trends_monthly_dataframe(BRANDS, GEO, GPROP, start_d, end_d)
    if df.empty:
        # tạo 1 dòng note để bạn dễ kiểm tra
        df = pd.DataFrame([{"Date": "No data", "Note": "Trends empty in range"}])

    # 2) Ghi vào Google Sheets (tab theo yyyy-mm)
    sheets = auth_sheets(CREDS_JSON)
    write_to_sheet(sheets, SHEET_ID, ym, df)

    print(f"Done. Wrote {len(df)} rows to sheet '{ym}'.")


if __name__ == "__main__":
    main()
