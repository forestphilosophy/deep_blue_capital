#!/usr/bin/env python3

import pandas as pd 
from datetime import datetime, time, date
from pandas.tseries.offsets import BDay
from dbc.postgres import Postgres_Support
import numpy as np
from dbc.dbcpass import dbcpass
import argparse
import logging
import calendar
from pandas.api.types import is_string_dtype
from pandas.api.types import is_numeric_dtype

pd.options.mode.chained_assignment = None

parser = argparse.ArgumentParser(
    description='Compare positions between core and baml given date and equity type.')

parser.add_argument('-d', help='date to check positions for in the format of "%YYYY-%mm-%dd"')
parser.add_argument('-o', '--output', help=(
    'Input the absolute path to output the html files.'),
    required=True, type=str)
parser.add_argument('-e', help='type "show" to print out the extra table where no difference in positions but no matching isin or currency found.')

dbcpass.add_arguments(parser)
args = parser.parse_args()
psql = Postgres_Support(args)

def baml_df(date_str):

    query = "select * from baml.smi_positions t1 left join core.exchange_rate_history t2 \
    on t1.price_ccy = t2.source and t1.price_date = t2.date where t1.price_date = %s and t2.target = 'EUR';"

    with psql.connect() as cnx:
        cursor = cnx.cursor()
        cursor.execute(query,(date_str,))
        column_names = [i[0] for i in cursor.description]
        baml = pd.DataFrame(cursor.fetchall())
        
        try:
            baml.columns = column_names
            
        except:#this breaks when baml has not run positions on the requested date yet in which case we will output differences on the previous date

            return False    
        
    baml["nominal_quantity"] = baml["nominal_quantity"].astype(float)     
    baml["rate"] = baml["fx_rate_to_base_ccy"].astype(float)
    
    stocks_idx = np.where(baml['security_type_id'] == 'EQTYSWAP')
    stocks_baml = baml.loc[stocks_idx]
    stocks_baml.drop_duplicates(inplace=True)
    stocks_baml['underlying_isin'] = stocks_baml['isin']
    
    futures_idx = np.where(baml['security_type_id'] == 'EQTYINDXFUT')
    futures_baml = baml.loc[futures_idx]
    
    trs_futures_idx = np.where((baml['security_type_id'] == 'EQTYSWAP') & \
    (baml['underlying_security_name'].isin(['KOREA SE KOSPI 200 INDEX','RTS INDEX'])))
    
    trs_futures_baml = baml.loc[trs_futures_idx]    
    futures_baml = futures_baml.append(trs_futures_baml)
    
    return stocks_baml, futures_baml

def core_df(date_str):
    
    """
    INPUT:
    date_str - datetime string in the form of '2021-06-25 23:00:00'
    OUTPUT:
    stocks_core, futures_core - stocks & futures dataframes.
    
    Function for retrieving the stocks and futures dataframes from core. 
    """
    
    query = "SELECT * "\
        "FROM trading.historical_positions t1 "\
        "LEFT JOIN core.my_names t2 "\
        "ON t1.syd = t2.syd "\
        "LEFT JOIN (select * from core.listings listings left join core.exchange_rate_history fx \
        on listings.currency = fx.source where fx.target = 'EUR' and fx.date = %s) t3 "\
        "ON t2.syd = t3.syd "\
        "LEFT JOIN (select * from core.history_of_isins where %s::tstzrange <@ period) t4 " \
        "ON t3.security_id = t4.security_id "\
        "WHERE t1.account = '/' AND %s::TIMESTAMP WITH TIME ZONE <@ t1.period AND broker_id = '6bb603f8-1014-58ba-8766-b6ea237da751'"
    
    with psql.connect() as cnx:
        cursor = cnx.cursor()
        cursor.execute(query,(date_str,''.join('[' + date_str + ',' + date_str + ']'),date_str))
        column_names = [i[0] for i in cursor.description]
        core = pd.DataFrame(cursor.fetchall())
        core.columns = column_names
    
    core["total_position"] = core["total_position"].astype(float)
    core["rate"] = core["rate"].astype(float)
    
    stocks_idx = np.where(core['kind'] == 'ST')
    stocks_core = core.loc[stocks_idx]
    stocks_core = stocks_core.loc[:,~stocks_core.columns.duplicated()].copy()
    stocks_core['isin'] = stocks_core.apply(lambda row: get_expired_isin(row['syd']) if row['isin'] is None else row['isin'], axis = 1)
    
    futures_idx = np.where(core['kind'] == 'FU')
    futures_core = core.loc[futures_idx]
    
    return stocks_core, futures_core

def get_expired_isin(syd):
    query = "SELECT isin FROM core.listings LEFT JOIN core.history_of_isins USING(security_id) where syd = %s;"
    
    with psql.connect() as cnx:
        cursor = cnx.cursor()
        cursor.execute(query,(int(syd),))
        isin = cursor.fetchall()[0][0]
        
    return isin
    
def get_fx_rates(date_str,ccy_list):
        
    """
    Function for getting the exchange rates dataframe. 
    """
    
    query = "select * from core.exchange_rate_history where target = 'EUR' and source in %s and date = %s"
    
    with psql.connect() as cnx:
        cursor = cnx.cursor()
        cursor.execute(query,(tuple(ccy_list),date_str))
        
        column_names = [i[0] for i in cursor.description]
        df = pd.DataFrame(cursor.fetchall())
        df.columns = column_names
        
        df["rate"] = df["rate"].astype(float)
        
        return df
    
def get_events(date_str):

    
    """
    Function for getting the events for baml adjustments which might explain our differences with baml. 
    """

    query= "SELECT underlying_isin, dealt_quantity, event_type_description "\
    "FROM baml.swap_transactions "\
    "WHERE period_date = %s AND event_type_description IS NOT NULL"
    
    with psql.connect() as cnx:
        cursor = cnx.cursor()
        cursor.execute(query,(date_str,))
        column_names = [i[0] for i in cursor.description]
        events = pd.DataFrame(cursor.fetchall())
        
        try:
            events.columns = column_names
            
        except ValueError:
            return pd.DataFrame()
            
    return events

def remove_prefix(text, prefix):
    
    if text.startswith(prefix):
        return text[len(prefix):].split()[0]
    
    return text.split()[0]

def get_table_where_isin_ccy_different(stocks_comparison):
    
    df = pd.DataFrame()
    
    #simple algo to filter out the ones where no matching currency found
    stocks_comparison = stocks_comparison.sort_values(by='isin').reset_index(drop=True)
    for i in range(1,len(stocks_comparison)):
        if stocks_comparison['isin'][i] == stocks_comparison['isin'][i-1] \
        and stocks_comparison['DBC Nominal'][i] == stocks_comparison['BAML Nominal'][i-1]:
            df = df.append([stocks_comparison.iloc[i-1], stocks_comparison.iloc[i]])
        
        if stocks_comparison['isin'][i] == stocks_comparison['isin'][i-1] \
        and stocks_comparison['DBC Nominal'][i] + stocks_comparison['DBC Nominal'][i-1] \
        == stocks_comparison['BAML Nominal'][i] + stocks_comparison['BAML Nominal'][i-1]:
            df = df.append([stocks_comparison.iloc[i-1], stocks_comparison.iloc[i]])
            
    stocks_comparison = pd.concat([df, stocks_comparison]).drop_duplicates(keep=False)
    
    stocks_comparison = stocks_comparison.sort_values(by='currency').reset_index(drop=True)
    
    to_remove = []
    #simple algo to filter out the ones where no matching isin found
    for i in range(len(stocks_comparison)):
        
        if stocks_comparison['only_in_baml'][i] == 1:
            baml_val = stocks_comparison['BAML Nominal'][i]
            
            if baml_val in stocks_comparison['DBC Nominal'].values:
                row_df = stocks_comparison.loc[stocks_comparison['DBC Nominal'] == baml_val]
                idx = row_df.index[0]
                baml_name = remove_prefix(stocks_comparison['stock_name'][i],'TRS')
                dbc_name = remove_prefix(stocks_comparison['stock_name'][idx],'TRS')

                if baml_name == dbc_name:
                    to_remove.append(True)
                    
                else:
                    to_remove.append(False)
                
            else:
                to_remove.append(False)
            
        elif stocks_comparison['only_in_core'][i] == 1:
            dbc_val = stocks_comparison['DBC Nominal'][i]

            if dbc_val in stocks_comparison['BAML Nominal'].values:

                row_df = stocks_comparison.loc[stocks_comparison['BAML Nominal'] == dbc_val]
                idx = row_df.index[0]
                
                baml_name = remove_prefix(stocks_comparison['stock_name'][idx],'TRS')
                dbc_name = remove_prefix(stocks_comparison['stock_name'][i],'TRS')
                
                if baml_name == dbc_name:
                    to_remove.append(True)
                    
                else:
                    to_remove.append(False)
                
            else:
                to_remove.append(False)
                
        else:
            to_remove.append(False)
        
    stocks_comparison['to_remove'] = to_remove
    no_ccy_match_df = stocks_comparison[stocks_comparison['to_remove']==True]
    no_ccy_match_df.drop('to_remove',axis=1,inplace=True)
    
    df = df.append([no_ccy_match_df, df]).reset_index(drop=True)
    
    return df

def remove_expired_isin_positions(df_baml,df_core):
    
    """    
    Function to remove the rows for which we actually have equal positions between 
    baml and core, but because the isin's are expired there's no isin from baml side
    and since no isin provided we cannot compare normally and will have to compare the positions using
    currency, position and first word of the security name
    """
    df_baml = df_baml.copy()
    df_core = df_core.copy()
    
    df_baml['compare_name'] = [i.split()[1] for i in df_baml['security_name']]
    df_core['compare_name'] = [i.split()[0] for i in df_core['name']]
    df_core.rename(columns={'total_position':'nominal_quantity', 
                            'currency':'price_ccy'}, inplace=True)
    to_remove =  df_baml[['security_name','compare_name', 'price_ccy', 'nominal_quantity',
                          'underlying_isin','price_date']].merge(df_core, 
                                           on=('compare_name', 'price_ccy', 'nominal_quantity'),   
                                           how='inner')
    to_remove = to_remove[['security_name','underlying_isin','price_ccy',
                       'nominal_quantity','price_date','rate']]
    
    df_baml.drop('compare_name',axis=1,inplace=True)
    df_baml = pd.concat([df_baml, to_remove]).drop_duplicates(subset=df_baml.columns.difference(['rate']),
                        keep=False)
    
    df_baml.reset_index(inplace=True)
    
    return df_baml

def comparison(date_str,equity_type):
    
    """
    INPUT:
    date_str - date to compare the differences for. 
    equity_type - "stocks" if checking for stocks and "futures" if checking for futures. 
    
    OUTPUT:
    final_df_stocks,final_df_futures - returns a dataframe of differences.
    
    Function for generating the dataframe which summarizes the differences for core's positions versus baml's. 
    """

    t = time(hour=23, minute=00)
    date_str = datetime.combine(date_str, t)
    date_str = date_str.strftime('%Y-%m-%d %H:%M:%S')
                 
    stocks_core,futures_core = core_df(date_str)
    stocks_baml,futures_baml = baml_df(date_str)
     
    if equity_type == "stocks":
        
        stocks_core = stocks_core.loc[:, ~stocks_core.columns.duplicated()]
        stocks_core = stocks_core.drop_duplicates()
        stocks_core = stocks_core.groupby(['isin','currency'],as_index=False).agg({\
        'total_position': 'sum', 'kind': 'first', 'bb': 'first', 'name': 'first', 'rate': 'first'})
        
        stocks_baml = stocks_baml[stocks_baml['underlying_security_type_id']!='INDEX']
        stocks_baml_no_isin = stocks_baml[stocks_baml['underlying_isin'].isnull()]   
        stocks_baml_no_isin = stocks_baml_no_isin.groupby(['security_name'],as_index=False).agg({
        'underlying_isin': 'first', 'price_ccy': 'first','nominal_quantity': 'sum',
        'price_date': 'first', 'rate': 'first',
        })  
        stocks_baml_no_isin = remove_expired_isin_positions(stocks_baml_no_isin
                                                            ,stocks_core)
        stocks_baml = stocks_baml.groupby(['underlying_isin', 'price_ccy'],as_index=False).agg({\
        'nominal_quantity': 'sum', 'price_date': 'first', 'security_name': 'first', 'rate': 'first'})   
        stocks_baml = stocks_baml.append(stocks_baml_no_isin)
        
        final_df_stocks = pd.merge(stocks_core, stocks_baml,  how='outer', \
        left_on=['isin','currency'], right_on = ['underlying_isin','price_ccy'])
        final_df_stocks.fillna({'total_position': 0, 'nominal_quantity': 0, \
        'isin': 'No matching isin and currency found in core.', 'bb': '',\
        'underlying_isin': 'No matching isin and currency found in baml.', \
        'currency': final_df_stocks['price_ccy'], 'name': final_df_stocks['security_name'],\
        'rate_x': final_df_stocks['rate_y']}, inplace=True)
        
        final_df_stocks["nominal_quantity"] = final_df_stocks["nominal_quantity"].astype(float)
        final_df_stocks["total_position"] = final_df_stocks["total_position"].astype(float)
        
        final_df_stocks['delta'] = final_df_stocks['total_position'] - final_df_stocks['nominal_quantity']
        final_df_stocks = final_df_stocks[[ 'isin', 'underlying_isin', 'currency',\
        'nominal_quantity', 'total_position', 'delta', 'bb', 'name', 'rate_x']]
        final_df_stocks.rename(columns={'isin':'core_isin', 'underlying_isin':'baml_isin',\
        'nominal_quantity':'BAML Nominal','total_position':'DBC Nominal', \
        'bb': 'stock_ticker', 'name': 'stock_name', 'rate_x': 'fx_rate'}, inplace=True)
        
        final_df_stocks['delta (EUR)'] = final_df_stocks['delta'] * final_df_stocks['fx_rate']
        
        return final_df_stocks[abs(final_df_stocks['delta (EUR)']) > 0]
    
    elif equity_type == "futures":

        futures_baml = futures_baml.copy()
        futures_baml['security_code_pref'] = futures_baml['security_code_pref'].str.replace('COMB', '')
        futures_baml['security_code_pref'] = futures_baml['security_code_pref'].str.replace('PIT', '')
        futures_baml['security_code_pref'] = [" ".join(i.split()) for i in futures_baml['security_code_pref']]
        futures_baml['bb_baml'] = futures_baml['security_code_pref']
            
        futures_baml = futures_baml[['price_date','security_name','security_type_id','bb_baml','nominal_quantity','rate','price_ccy']]
        
        futures_core = futures_core.loc[:, ~futures_core.columns.duplicated()]
        futures_core = futures_core.groupby(['bb'],as_index=False).agg({\
        'total_position': 'sum', 'kind': 'first', 'name': 'first', 'rate': 'first','currency': 'first'})
        futures_core = futures_core[futures_core['total_position'] != 0]
        
        final_df_futures = pd.merge(futures_core, futures_baml,  how='outer', left_on=['bb'], right_on = ['bb_baml'])
        final_df_futures = final_df_futures.fillna({'nominal_quantity': 0, \
        'price_date': date_str, 'currency': final_df_futures['price_ccy'], \
        'name': final_df_futures['security_name'], 'bb_baml': 'No matching security name found in baml.',\
        'rate_x': final_df_futures['rate_y']})
        
        final_df_futures['delta'] = final_df_futures['total_position'] - final_df_futures['nominal_quantity']
        final_df_futures = final_df_futures[['name', 'currency', 'total_position', 'nominal_quantity','delta', 'rate_x','bb']]
        final_df_futures = final_df_futures.rename(columns={'name':'futures_name','nominal_quantity':'BAML Nominal','total_position':'DBC Nominal','rate_x':'fx_rate'})
        
        final_df_futures['delta (EUR)'] = final_df_futures['delta'] * final_df_futures['fx_rate']
        final_df_futures.drop(['fx_rate','delta'],axis=1,inplace=True)
        
        return final_df_futures[abs(final_df_futures['delta (EUR)']) > 0]


def get_future_code(date,expiry_date):
    
    year = date.year
    month = date.month
    if month % 3 != 0:
        # Easy case, not in a month in which the future changes.  Get the month of
        # the next future.
        logging.debug('no new future this month')
        month = 3 * ((month + 2) // 3)
        
    else:
        expiry_date = datetime(expiry_date.year, expiry_date.month, expiry_date.day)
        if date < expiry_date:
            month = date.month
            
        else:
            
            if date.month == 12:
                month = 3
            else:
                month = date.month + 3
        
    # Before the expiry, use the future from this month.
    logging.debug('using future from %s, month %s' % (year, month))
    last_year_digit = year % 10
    
    month_codes = {
        1: 'F',
        2: 'G',
        3: 'H',
        4: 'J',
        5: 'K',
        6: 'M',
        7: 'N',
        8: 'Q',
        9: 'U',
        10: 'V',
        11: 'X',
        12: 'Z'
    }
    
    month_code = month_codes[month]
    code = month_code + str(last_year_digit)

    return code

def get_kospi_future_code(date):
    # KOSPI index futures: 3 month cycle with expiry 2nd Wednesday of the month.
    date = datetime.strptime(date,'%Y-%m-%d %H:%M:%S')
    year = date.year
    month = date.month
    
    c = calendar.Calendar(firstweekday=calendar.SUNDAY)
    monthcal = c.monthdatescalendar(year,month)
    second_Wednesday = [day for week in monthcal for day in week if \
                day.weekday() == calendar.WEDNESDAY and \
                day.month == month][1]
        
    return get_future_code(date,second_Wednesday)

def get_rts_future_code(date):
    # RTS index futures: 3 month cycle with expiry 3rd Monday of the month.
    date = datetime.strptime(date,'%Y-%m-%d %H:%M:%S')
    year = date.year
    month = date.month
    
    c = calendar.Calendar(firstweekday=calendar.SUNDAY)
    monthcal = c.monthdatescalendar(year,month)
    third_Monday = [day for week in monthcal for day in week if \
        day.weekday() == calendar.MONDAY and \
        day.month == month][2]
    
    return get_future_code(date,third_Monday)

def fix_pandas_html(html_df):
    style_dict = make_styler_dict(html_df)
#    html_df.style.format(make_styler_dict(html_df))
    html_out = html_df.to_html(formatters=style_dict)
    
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_out, 'html.parser')
    soup.table['class'] = "table-fill"
    soup.table['border'] = "0"
    soup.table['width'] = "1000"
    soup.table['height'] = "1000"
    
    html_out = str(soup)
    return html_out

def make_styler_dict(df):
    d_out = {}
    for col in df.columns:

        if is_string_dtype(df[col]):
            continue
        
        elif is_numeric_dtype(df[col]):          

            d_out[col] = lambda x: '{:,.0f}'.format(x)

    return d_out

def get_stylesheet():
    with open('/mnt/production-read/jimmy_stuff/recon_stylesheet.css','r') as fo:
        stylesheet = fo.read()  
    
    return stylesheet
        
def output_html_file(html_to_output,titles):
    
    final_html = ''
    title_count= 0 
    
    for html in html_to_output:
        title = "<p style = 'font-family:georgia,garamond,serif;font-size:16px;font-style:italic;'> \
        <font size='+1'> <center>" + titles[title_count] + "</center> </font> </p>"
        final_html += '<br /><br />' + title + '\n' + '<br /><br />' + html
        title_count += 1
        
    stylesheet = get_stylesheet()
    final_html = stylesheet + '\n' + '<body>' + final_html + '\n' + '</body>'

    file = open(args.output,"w")
    file.write(final_html)
    file.close()
    
    
def get_diffs(date_dt,equity_type):
    
    if equity_type == 'ST':
        global extra_table
        #Comparisons for all stocks for T-1 and T-2. 
        #When there is "No matching isin and currency found in baml/core.", i.e. not able to find the corresponding isin and currency on either core or baml side => a position of 0 is assumed on that side.
        stocks_comparison = comparison(date_dt,'stocks')
        
        stocks_comparison['isin'] = np.where(stocks_comparison['core_isin'] == \
        'No matching isin and currency found in core.', stocks_comparison['baml_isin'], stocks_comparison['core_isin'])
        stocks_comparison['only_in_core'] = np.where(stocks_comparison['baml_isin'] ==\
        'No matching isin and currency found in baml.', 1, 0)
        stocks_comparison['only_in_baml'] = np.where(stocks_comparison['core_isin'] ==\
        'No matching isin and currency found in core.', 1, 0)
        stocks_comparison.drop(['core_isin','baml_isin'],axis=1,inplace = True)
        stocks_comparison = stocks_comparison[abs(stocks_comparison['delta']) > 0]\
        [['isin', 'stock_name','currency','stock_ticker','BAML Nominal','DBC Nominal', 'delta (EUR)', 'only_in_core', 'only_in_baml']]
             
        extra_table = get_table_where_isin_ccy_different(stocks_comparison)
        extra_table = extra_table[~((extra_table['only_in_baml']== 1) & (extra_table['only_in_core'] == 1))]
        stocks_comparison = pd.concat([extra_table, stocks_comparison]).drop_duplicates(keep=False)
        stocks_comparison = stocks_comparison[~stocks_comparison['stock_ticker'].str.contains("-exp_")] # removing expired tikcers
        return stocks_comparison
    
    elif equity_type == 'FU':
        
        #Same bur for futures. 
        futures_comparison = comparison(date_dt,'futures')
        
        return futures_comparison

def get_yesterday_baml_position(date_str,isins):
    baml_query = "select isin, nominal_quantity, price_ccy from baml.smi_positions t1 \
    left join core.exchange_rate_history t2 \
    on t1.price_ccy = t2.source and t1.price_date = t2.date where t1.price_date = %s and \
    t2.target = 'EUR' and isin in %s;"
    
    with psql.connect() as cnx:
        cursor = cnx.cursor()
        cursor.execute(baml_query,(date_str,tuple(isins)))
        column_names = [i[0] for i in cursor.description]
        baml = pd.DataFrame(cursor.fetchall())
        
    try:
        baml.columns = column_names
        
    except:
       return False # there is no position for the isins on that day
   
    baml["nominal_quantity"] = baml["nominal_quantity"].astype(float)     
    
    return baml

def get_yesterday_core_position(date_str,isins,equity_type):
    stocks_core, futures_core = core_df(date_str)
    stocks_core = stocks_core.loc[:, ~stocks_core.columns.duplicated()]
    stocks_core = stocks_core.drop_duplicates()
    stocks_core = stocks_core.groupby(['isin','currency'],\
    as_index=False).agg({'total_position': 'sum'})
        
    futures_core = futures_core.loc[:, ~futures_core.columns.duplicated()]
    futures_core = futures_core.groupby(['bb'],as_index=False).agg({'total_position': 'sum',\
    'kind': 'first', 'name': 'first', 'rate': 'first','currency': 'first'})
    futures_core = futures_core[futures_core['total_position'] != 0]
    
    if equity_type == 'ST':
        return stocks_core
    
    elif equity_type == 'FU':
        return futures_core

def append_events_to_df(date_str,df):
    
    events = get_events(date_str)
    
    if not events.empty:
        df = df.merge(events,
           left_on='isin', right_on='underlying_isin', how='left')
        
        df.reset_index(drop=True,inplace=True)
        
        if df['event_type_description'].isnull().all():
            
            df = df.drop(['underlying_isin','dealt_quantity','event_type_description'],axis=1)
            
        else:

            df = df.fillna({'event_type_description': ' ', 'dealt_quantity': ' '}).drop('underlying_isin',axis=1)
            df = df.rename(columns={'dealt_quantity':'baml_adjustments'})
            
            df = df.groupby(['isin','stock_name'],as_index=False).agg({
                           'currency': 'first', 'stock_ticker': 'first',  \
                           'BAML Nominal': 'first', 'DBC Nominal': 'first', 'delta (EUR)': 'sum', \
                           'baml_adjustments': 'sum', 'event_type_description': ' '.join})
            df = df.loc[(df['baml_adjustments']!= 0) | (df['baml_adjustments'] != "")]
        
    return df

def calculate_eur_deltas(df,date_str):
    
    ccy_list = df['currency'].tolist()
    
    if len(ccy_list) == 0:
        return df 
    
    fx_df = get_fx_rates(date_str,ccy_list)
    df = pd.merge(df, fx_df,  how='left', left_on=['currency'], right_on = ['source'])
    df['delta (EUR)'] = (df['DBC Nominal'] - df['BAML Nominal']) * df['rate']
    df.drop(['source','target','date','rate'],axis=1,inplace=True)
    
    return df 


def get_baml_yesterday_position(date_str,isin,ccy):
    baml_query = "select nominal_quantity from baml.smi_positions t1 \
    left join core.exchange_rate_history t2 \
    on t1.price_ccy = t2.source and t1.price_date = t2.date where t1.price_date = %s and \
    t2.target = 'EUR' and isin = %s and price_ccy = %s;"
    
    with psql.connect() as cnx:
        cursor = cnx.cursor()
        cursor.execute(baml_query,(date_str,isin,ccy))
        row = cursor.fetchone()
   
    return float(row[0])

def get_core_yesterday_position(date_str,isin,ccy,equity_type='ST'):
    query = "SELECT total_position "\
        "FROM trading.historical_positions t1 "\
        "LEFT JOIN core.my_names t2 "\
        "ON t1.syd = t2.syd "\
        "LEFT JOIN (select * from core.listings listings left join \
        core.exchange_rate_history fx on listings.currency = fx.source \
        where fx.target = 'EUR' and fx.date = %s) t3 "\
        "ON t2.syd = t3.syd "\
        "LEFT JOIN (select * from core.history_of_isins where %s::tstzrange <@ period) t4 " \
        "ON t3.security_id = t4.security_id "\
        "WHERE t1.account = '/' AND %s::TIMESTAMP WITH TIME ZONE <@ t1.period AND \
        isin = %s AND broker_id = '6bb603f8-1014-58ba-8766-b6ea237da751' \
        AND source = %s"
    
    with psql.connect() as cnx:
        cursor = cnx.cursor()
        cursor.execute(query,(date_str,''.join('[' + date_str + ',' + date_str + ']'),date_str,isin,ccy))   
        row = cursor.fetchall()
        
    total_val = 0
    for row in row:
        total_val+= float(row[0])

    return total_val


def preprocess_core_df(stocks_core,futures_core):
    
    stocks_core = stocks_core.loc[:, ~stocks_core.columns.duplicated()]
    stocks_core = stocks_core.drop_duplicates()
    stocks_core = stocks_core.groupby(['isin','currency'],as_index=False).agg({'total_position': 'sum'})
    
    futures_core = futures_core.loc[:, ~futures_core.columns.duplicated()]
    futures_core = futures_core.groupby(['bb'],as_index=False).agg({'total_position': 'sum', 'currency': 'first'})
    futures_core = futures_core[futures_core['total_position'] != 0]    

    return stocks_core, futures_core

def preprocess_baml_futures(futures_baml):
    futures_baml = futures_baml.copy()
    futures_baml['security_code_pref'] = futures_baml['security_code_pref'].str.replace('COMB', '')
    futures_baml['security_code_pref'] = futures_baml['security_code_pref'].str.replace('PIT', '')
    futures_baml['security_code_pref'] = [" ".join(i.split()) for i in futures_baml['security_code_pref']]
    futures_baml['bb'] = futures_baml['security_code_pref']
    
    return futures_baml

def append_yesterday_position(df,stocks_baml_prev,stocks_core_prev):
    stocks_baml_prev = stocks_baml_prev[['price_ccy','nominal_quantity','underlying_isin']]
    df = df.merge(stocks_baml_prev,how='left',left_on=['isin','currency']\
                     ,right_on=['underlying_isin','price_ccy'])
    df['nominal_quantity'] = df['nominal_quantity'].fillna(0)
    
    df = df.merge(stocks_core_prev,how='left',left_on=['isin','currency']\
                     ,right_on=['isin','currency'])        
    df['total_position'] = df['total_position'].fillna(0)    
    
    df.drop(['currency','price_ccy','underlying_isin'],axis=1,inplace=True)
    df.rename(columns={'nominal_quantity':'BAML Nominal (T-1)', 'total_position': 'DBC Nominal (T-1)'}, inplace=True)
    
    if len(df.columns) == 10:
        column_order = ['isin', 'stock_name', 'stock_ticker', 'BAML Nominal',
       'DBC Nominal', 'BAML Nominal (T-1)', 'DBC Nominal (T-1)', 'delta (EUR)', 'baml_adjustments',
       'event_type_description']
        df = df.groupby(['stock_name'],as_index=False).agg({'isin':'first',
    'stock_ticker': 'first', 'BAML Nominal': 'first', 'DBC Nominal': 'first',\
    'BAML Nominal (T-1)': 'sum', 'DBC Nominal (T-1)': 'sum', 'delta (EUR)':'first'\
    ,'baml_adjustments':'first','event_type_description':'first'})  
        
    elif len(df.columns) == 8:
        column_order = ['isin', 'stock_name', 'stock_ticker', 'BAML Nominal',
       'DBC Nominal', 'BAML Nominal (T-1)', 'DBC Nominal (T-1)', 'delta (EUR)']
        df = df.groupby(['stock_name'],as_index=False).agg({'isin': 'first',
        'stock_ticker': 'first', 'BAML Nominal': 'first', 'DBC Nominal': 'first',\
        'BAML Nominal (T-1)': 'sum', 'DBC Nominal (T-1)': 'sum', 'delta (EUR)':'first'\
        })      
          
    df = df.reindex(column_order,axis=1) 
    df['isin'] = np.where(df['isin'] == 'No matching isin and currency found in baml.', '', df['isin'])
    return df.set_index('isin')

def append_yesterday_position_futures(df,futures_baml_prev,futures_core_prev):
    futures_baml_prev = futures_baml_prev[['bb','nominal_quantity']]
    df = df.merge(futures_baml_prev,how='left',left_on=['bb']\
                     ,right_on=['bb'])
    df['nominal_quantity'] = df['nominal_quantity'].fillna(0)
    
    df = df.merge(futures_core_prev,how='left',left_on=['bb']\
                     ,right_on=['bb'])        
    df['total_position'] = df['total_position'].fillna(0)    
    
    df.drop(['currency_x','currency_y'],axis=1,inplace=True)
    df.rename(columns={'nominal_quantity':'BAML Nominal (T-1)', 'total_position': 'DBC Nominal (T-1)'}, inplace=True)
    
    if len(df.columns) == 7:
        column_order = ['futures_name', 'BAML Nominal', 'DBC Nominal', \
                        'BAML Nominal (T-1)', 'DBC Nominal (T-1)', 'delta (EUR)', 'bb']

    elif len(df.columns) == 9:
        column_order = ['isin', 'stock_name', 'stock_ticker', 'BAML Nominal',
       'DBC Nominal', 'BAML Nominal (T-1)', 'DBC Nominal (T-1)', 'delta (EUR)']
            
    df = df.reindex(column_order,axis=1) 
    return df.set_index('bb')

def output_differences(equity_type, date_str):
    
    date_dt = datetime.strptime(date_str, '%Y-%m-%d')
    
    prev_date_dt = date_dt-BDay(1)
    
    stocks_baml_prev, futures_baml_prev = baml_df(datetime.strftime(prev_date_dt,'%Y-%m-%d'))
    futures_baml_prev = preprocess_baml_futures(futures_baml_prev)
    
    stocks_core_prev, futures_core_prev = core_df(datetime.strftime(prev_date_dt,'%Y-%m-%d'))
    stocks_core_prev, futures_core_prev = preprocess_core_df(stocks_core_prev,futures_core_prev)
    
    if equity_type == 'ST':
        
        #Comparisons for all stocks for T-1 and T-2. 
        #When there is "No matching isin and currency found in baml/core.", 
        #i.e. not able to find the corresponding isin and currency on either core or baml side 
        #=> a position of 0 is assumed on that side.
        columns = ['isin', 'stock_name', 'currency', 'stock_ticker', 'BAML Nominal', 'DBC Nominal']
        diffs_at_t = get_diffs(date_dt,'ST')[columns]
        diffs_at_t_minus_1 = get_diffs(prev_date_dt,'ST')[columns]
        
        merged = diffs_at_t.merge(diffs_at_t_minus_1, how='left', indicator=True)
        stock_differences_in_t_but_not_t_minus_1 = merged[merged['_merge']=='left_only'].drop(['_merge'],axis=1)
        
        diffs_at_t = calculate_eur_deltas(diffs_at_t,date_str)
        
        diffs_at_t = append_events_to_df(date_str,diffs_at_t)
        stock_differences_in_t_but_not_t_minus_1 = calculate_eur_deltas(stock_differences_in_t_but_not_t_minus_1,date_str)
        
        if not diffs_at_t.empty:
            diffs_at_t = append_yesterday_position(diffs_at_t,stocks_baml_prev,stocks_core_prev)
        
        if not stock_differences_in_t_but_not_t_minus_1.empty:
            stock_differences_in_t_but_not_t_minus_1 = append_yesterday_position\
        (stock_differences_in_t_but_not_t_minus_1,stocks_baml_prev,stocks_core_prev)
        
        return diffs_at_t, stock_differences_in_t_but_not_t_minus_1
    
    elif equity_type == 'FU':
        
        diffs_at_t = get_diffs(date_dt,'FU')
        diffs_at_t_minus_1 = get_diffs(prev_date_dt,'FU')
        
        merged = diffs_at_t.merge(diffs_at_t_minus_1, how='left', indicator=True)
        futures_differences_in_t_but_not_t_minus_1 = merged[merged['_merge']=='left_only'].drop(['_merge'],axis=1)
        
        if not diffs_at_t.empty:
            diffs_at_t = append_yesterday_position_futures(diffs_at_t,futures_baml_prev,futures_core_prev)
            
        if not futures_differences_in_t_but_not_t_minus_1.empty:
            futures_differences_in_t_but_not_t_minus_1 = append_yesterday_position_futures\
        (futures_differences_in_t_but_not_t_minus_1,futures_baml_prev,futures_core_prev)
        
        return diffs_at_t,futures_differences_in_t_but_not_t_minus_1
    
def main():
    
    requested_date = args.d

    if requested_date == None:
        requested_date = (date.today() - BDay(1)).strftime("%Y-%m-%d")
    
    elif datetime.strptime(requested_date, '%Y-%m-%d').weekday() > 4: #if it's weekened we use the latest workday. 
        requested_date = (datetime.strptime(requested_date, '%Y-%m-%d') - BDay(1)).strftime("%Y-%m-%d")
        print ('REQUESTED DATE IS NOT A WEEKDAY - OUTPUTTING SUMMARY FOR THE LATEST WORKING DAY %s' % requested_date)
        
    the_day_before = (datetime.strptime(requested_date, '%Y-%m-%d') - BDay(1)).strftime("%Y-%m-%d")
    

    if baml_df(requested_date) == False:
        print ('BAML HAS NOT RUN POSITIONS YET FOR REQUESTED DATE {requested_date} \
        - OUTPUTTING SUMMARY FOR PREVIOUS WORKING DAY {the_day_before}'.format(requested_date=requested_date,the_day_before=the_day_before))
        requested_date = the_day_before
        stock_differences, stock_differences_at_t_only = output_differences('ST',requested_date)
        futures_differences, futures_differences_at_t_only = output_differences('FU',requested_date)
        
    else:
        stock_differences, stock_differences_at_t_only = output_differences('ST',requested_date)
        futures_differences, futures_differences_at_t_only = output_differences('FU',requested_date)
    
    html_to_output = []
    titles = []
    
    if not stock_differences_at_t_only.empty:
        
        html_to_output.append(fix_pandas_html(stock_differences_at_t_only))
        title = f"STOCK DIFFERENCES PRESENT ON {requested_date} BUT NOT ON {the_day_before}"
        titles.append(title)
        #print('\nSTOCK DIFFERENCES PRESENT ON %s BUT NOT ON %s\n' % (requested_date,the_day_before))
        #print ()
        #print (stock_differences_at_t_only.to_string())
        #print ()
        
    if stock_differences.empty:
        pass
        #print ("\nTHERE ARE NO DIFFERENCES FOR STOCKS ON {date}.\n".format(date=requested_date))
        #print ()
    
    else:
        html_to_output.append(fix_pandas_html(stock_differences))
        title = f"RECONCILIATION SUMMARY FOR STOCKS ON {requested_date}"
        titles.append(title)
        #print('\nRECONCILIATION SUMMARY FOR STOCKS ON %s\n' % requested_date)
        #print ()
        #print (stock_differences.to_string())
        #print ()
    
    show_extra_table = args.e
    
    if show_extra_table == 'show' and not extra_table.empty:
        html_to_output.append(fix_pandas_html(extra_table.set_index('isin')))
        print('\nSTOCK DIFFERENCES WHERE NO ISIN OR CURRENCY FOUND ON %s\n' % (requested_date))
        print (extra_table.set_index('isin').to_string())
        print ()
        
    if not futures_differences_at_t_only.empty:
        title = f"FUTURES DIFFERENCES PRESENT ON {requested_date} BUT NOT ON {the_day_before}"
        titles.append(title)
        html_to_output.append(fix_pandas_html(futures_differences_at_t_only))
        #print('\nFUTURES DIFFERENCES PRESENT ON %s BUT NOT ON %s\n' % (requested_date,the_day_before))
        #print ()
        #print (futures_differences_at_t_only.to_string())
        #print ()       
        
    if futures_differences.empty:
        pass
        #print ("\nTHERE ARE NO DIFFERENCES FOR FUTURES ON {date}.\n".format(date=requested_date))
        #print ()
    
    else:
        title = f"RECONCILIATION SUMMARY FOR FUTURES ON {requested_date}"
        titles.append(title)
        html_to_output.append(fix_pandas_html(futures_differences))
        #print('\nRECONCILIATION SUMMARY FOR FUTURES ON %s\n' % requested_date)
        #print ()
        #print (futures_differences.to_string())
        #print ()
    
    output_html_file(html_to_output,titles)
    
if __name__ == '__main__':
    main()


