import asyncio
from apify import Actor
from pytrends.request import TrendReq

async def main():
    async with Actor:
        # Lấy input từ Apify UI (có default trong actor.json)
        cfg = await Actor.get_input() or {}
        keywords = cfg.get('keywords') or ["메디큐브", "코스알엑스", "아누아", "조선미녀", "K-SECRET", "ARENCIA", "MIXSOON"]
        timeframe = cfg.get('timeframe', '2023-08-01 2023-08-31')
        geo = cfg.get('geo', 'US')
        gprop = cfg.get('gprop', 'web')

        pytrends = TrendReq(hl='en-US', tz=360)
        pytrends.build_payload(
            kw_list=keywords,
            cat=0,
            timeframe=timeframe,
            geo=geo,
            gprop=gprop
        )

        df = pytrends.interest_over_time().reset_index()
        # Loại cột isPartial nếu có
        if 'isPartial' in df.columns:
            df.drop(columns=['isPartial'], inplace=True, errors='ignore')

        # Push từng dòng vào Dataset của Apify (để Make/Airtable/Sheets đọc được)
        for rec in df.to_dict(orient='records'):
            await Actor.push_data(rec)

        # Lưu một file summary vào Key-value store
        await Actor.set_value('summary.json', {
            'keywords': keywords,
            'rows': len(df),
            'timeframe': timeframe,
            'geo': geo,
            'gprop': gprop
        })

if __name__ == '__main__':
    asyncio.run(main())
