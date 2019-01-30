import os
import pandas as pd
from urllib.request import urlretrieve


def _isnotebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell=='ZMQInteractiveShell':
            return True
        else:
            return False
    except:
        return False
    
        
def etfdb_alloc(*tickers, download_to='etfdb', cwd=os.getcwd()):
    ETFDB_BASE_URL = 'https://etfdb.com/etf/'
    dir_target = os.path.join(cwd, download_to)

    if _isnotebook():
        from tqdm import tqdm_notebook as prg
    else:
        from tqdm import tqdm as prg
    
    if not os.path.exists(dir_target):
        os.makedirs(dir_target)    
    
    def _name(df):
        name = df.index.name
        if name.lower() != 'region':
            return name
        elif df.index.str.contains('america|asia|europe|africa|middle', case=False).any():
            return name
        else:
            return 'Market Tier'

    def _df(df, ticker):
        return pd.DataFrame({ticker.upper():df.dropna().Percentage.str.rstrip('%').astype('float')})


    def _tables(ticker):
        file = os.path.join(dir_target, ticker + '.html')
        tables = pd.read_html(file, attrs={'class':'chart base-table'}, index_col=0)
        return {_name(df):_df(df, ticker) for df in tables}

    for ticker in prg(tickers):
        file = os.path.join(dir_target, ticker.upper() + '.html')
        if not os.path.exists(file):
            urlretrieve(ETFDB_BASE_URL + ticker, file)

    tables = []
    for ticker in tickers:
        tables.append(_tables(ticker))

    tables_dict = {
        k:pd.concat([dic[k] for dic in tables], axis=1, sort='False').fillna(0) for k in tables[0]
    }

    return pd.concat(tables_dict, axis=0)  