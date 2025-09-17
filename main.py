import os
import json
import datetime as dt
import pandas as pd

from pytrends.request import TrendReq

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# ========= Cấu hình từ ENV / Secrets =========
SHEET_ID = os.getenv("SHEET_ID", "").strip()           # BẮT BUỘC có (đặt trong Secrets GitHub)
CREDS_JSON_PATH = "creds.json"                          # Workflow sẽ ghi file này từ secret JSON
GPROP = os.getenv("GPROP", "").strip()                  # "", "images", "news", "youtube", "froogle"
GEO = os.getenv("GEO", "US").strip()                    # Mặc định lấy US

# Danh sách brand (chỉnh trong code cho nhanh); có thể thay bằng ENV nếu muốn
BRANDS = [
    "메디큐브",      # Medicube (KR)
    "코스알엑스",     # COSRX
    "아누아",        # ANUA
    "조선미녀",      # Beauty of Joseon
    "K-SECRET",
    "ARENCIA",
    "MIXSOON",
]


def assert_inputs():
    """Kiểm tra đầu vào & validate gprop theo pytrends."""
    if not SHEET_ID:
        raise ValueError("Thiếu SHEET_ID (GitHub Secrets)")

    allowed_gprops = {"", "images", "news", "youtube", "froogle"}
    if GPROP not in allowed_gprops:
        raise ValueError(
            f"GPROP='{GPROP}' không hợp lệ. Cho phép: {allowed_gprops}"
        )


def last_full_month_range():
    """Trả về (start_date, end_date) cho THÁNG TRƯỚC (YYYY-MM-DD)."""
    today = dt.date.today()
    first_of_this_month = today.replace(day=1)
    last_day_prev_month = first_of_this_month - dt.timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)
    return first_day_prev_month, last_day_prev_month


def trends_monthly_dataframe(brands, geo, gprop, start_d, end_d):
    """
    Lấy Google Trends cho từng brand trong khoảng start_d – end_d,
    trả về DataFrame gồm brand, start_date, end_date, avg_interest.
    """
    pytrends = TrendReq(hl="en-US", tz=0, retries=2, backoff_factor=0.2)
    timeframe = f"{start_d:%Y-%m-%d} {end_d:%Y-%m-%d}"

    rows = []
    for kw in brands:
        # Build payload theo chuẩn pytrends
        pytrends.build_payload(
            kw_list=[kw],
            timeframe=timeframe,
            geo=geo,
            gprop=gprop  # "", "images", "news", "youtube", "froogle"
        )

        iot = pytrends.interest_over_time()
        if iot is None or iot.empty:
            avg_val = None
        else:
            # Bỏ cột isPartial nếu có
            if "isPartial" in iot.columns:
                iot = iot.drop(columns=["isPartial"])
            # Lấy trung bình trong tháng
            avg_val = float(iot[kw].mean())

        rows.append({
            "brand": kw,
            "start_date": str(start_d),
            "end_date": str(end_d),
            "avg_interest": avg_val,
            "geo": geo,
            "gprop": gprop if gprop else "web"
        })

    return pd.DataFrame(rows)


def sheets_client(creds_json_path):
    """Tạo Google Sheets service client từ service account JSON."""
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(creds_json_path, scopes=scopes)
    return build("sheets", "v4", credentials=creds).spreadsheets()


def write_to_sheet(spreadsheets, sheet_id, df, sheet_name="Sheet1"):
    """
    Append data vào Google Sheet.
    Tạo header nếu trang còn trống.
    """
    values = df[
        ["brand", "start_date", "end_date", "avg_interest", "geo", "gprop"]
    ].values.tolist()

    # Kiểm tra có header chưa
    read = spreadsheets.values().get(
        spreadsheetId=sheet_id,
        range=f"{sheet_name}!A1:A1"
    ).execute()

    is_empty = ("values" not in read)

    if is_empty:
        header = [["brand", "start_date", "end_date", "avg_interest", "geo", "gprop"]]
        spreadsheets.values().append(
            spreadsheetId=sheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body={"values": header}
        ).execute()

    # Append dữ liệu
    spreadsheets.values().append(
        spreadsheetId=sheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


def main():
    assert_inputs()

    start_d, end_d = last_full_month_range()
    print(f"[INFO] Timeframe: {start_d} -> {end_d} | GEO={GEO} | gprop={GPROP or 'web'}")

    df = trends_monthly_dataframe(BRANDS, GEO, GPROP, start_d, end_d)
    print("[INFO] Data collected:")
    print(df)

    # Ghi Google Sheets
    sheets = sheets_client(CREDS_JSON_PATH)
    write_to_sheet(sheets, SHEET_ID, df, sheet_name="Sheet1")

    print("[DONE] Đã ghi dữ liệu vào Google Sheet.")


if __name__ == "__main__":
    main()
