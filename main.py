from pytrends.request import TrendReq

def run():
    pytrends = TrendReq(hl='en-US', tz=360)
    kw_list = ["메디큐브", "코스알엑스", "아누아", "조선미녀", "K-SECRET", "ARENCIA", "MIXSOON"]
    pytrends.build_payload(kw_list, cat=0, timeframe='2023-08-01 2023-08-31', geo='US', gprop='web')
    data = pytrends.interest_over_time()
    print(data)

if __name__ == "__main__":
    run()
