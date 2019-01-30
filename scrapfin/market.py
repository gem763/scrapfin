import pandas as pd
import asyncio
from functools import partial


def _period(start=None, end=None, cal_days=None, n=None):
    style = None
    
    # 시작일만 명시되어 있다면, 마지막일은...
    if (start is not None) and (end is None):
        # 그 이외 아무것도 명시되어 있지않다면, 무조건 오늘
        if (n is None) and (cal_days is None):
            end = pd.Timestamp.today()
        
        # 정확한 샘플수가 필요하다면, 시작일 대비 샘플수의 2배수 후
        elif n is not None:
            end = pd.Timestamp(start) + pd.DateOffset(days=2*n)
            style = 'from'
            
        # 그렇지 않다면, 마지막일 대비 달력일(cal_days)수 후 (al_days 필수)
        else:
            end = pd.Timestamp(start) + pd.DateOffset(days=cal_days)
    
    # 마지막일만 명시되어 있다면, 시작일은...
    elif (start is None) and (end is not None):
        # 정확한 샘플수가 필요하다면, 마지막일 대비 샘플수의 2배수 전
        if n is not None:
            start = pd.Timestamp(end) - pd.DateOffset(days=2*n)
            style = 'to'
            
        # 그렇지 않다면, 마지막일 대비 달력일(cal_days)수 전 (al_days 필수)
        else:
            start = pd.Timestamp(end) - pd.DateOffset(days=cal_days)
            
    return start, end, style

    
    
def yahoo(*tickers, what=None, start=None, end=None, cal_days=None, n=None):
    import yahoo_fin.stock_info as yf
    start, end, style = _period(start=start, end=end, cal_days=cal_days, n=n)
    
    
    async def _data(ticker):
        get_data_partial = partial(yf.get_data, start_date=start, end_date=end)
        data = await loop.run_in_executor(None, get_data_partial, ticker)
        data = data.drop(columns=['ticker'])
        
        if what is not None:
            data = data[what]
        
        return ticker.upper(), data


    async def main():
        fts = [asyncio.ensure_future(_data(ticker)) for ticker in tickers]
        return await asyncio.gather(*fts)
    
    
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    
    try:
        # 다음 코드를 주피터에서 돌리려면, tornado를 downgrade 해야함
        # pip install tornado==4.5.3
        res = loop.run_until_complete(main())        
        out = pd.concat(dict(res), axis=1, sort=True).fillna(method='ffill')
        #cols = [t.upper() for t in tickers]
        
        if style=='to':
            out = out.tail(n)
            
        elif style=='from':
            out = out.head(n)

        out = out[[t.upper() for t in tickers]]
        
    except Exception as ex:
        out = ex
    
    finally:
        loop.close()
    
    return out
    