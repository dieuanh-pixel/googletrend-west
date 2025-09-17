import os, json
import pandas as pd
from pytrends.request import TrendReq
from google.oauth2 import service_account
from googleapiclient.discovery import build

SHEET_ID = os.environ["SHEET_ID"]  # <-- Sẽ lỗi nếu env chưa map
CREDS_JSON = "creds.json"

# ... phần code kết nối Google Sheets & chạy pytrends của bạn ...
