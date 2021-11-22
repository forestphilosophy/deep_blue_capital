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

    query = "select * from baml.smi_positions t1 left join core.exchange_rate_history t2 on t1.price_ccy = t2.source and t1.price_date = t2.date where t1.price_date = %s and t2.target = 'EUR';"

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
    
    trs_futures_idx = np.where((baml['security_type_id'] == 'EQTYSWAP') & (baml['underlying_security_name'].isin(['KOREA SE KOSPI 200 INDEX','RTS INDEX'])))
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
        "LEFT JOIN (select * from core.listings listings left join core.exchange_rate_history fx on listings.currency = fx.source where fx.target = 'EUR' and fx.date = %s) t3 "\
        "ON t2.syd = t3.syd "\
        "LEFT JOIN (select * from core.historical_isins where %s::daterange <@ period) t4 " \
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
    
    futures_idx = np.where(core['kind'] == 'FU')
    futures_core = core.loc[futures_idx]
    
    return stocks_core, futures_core

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
        
        if stocks_comparison['stock_ticker'][i] == 'No matching isin and currency found in core.':
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
            
        elif stocks_comparison['underlying_security_type_id'][i] == 'No matching isin and currency found in baml.':
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
        stocks_core = stocks_core.groupby(['isin','currency'],as_index=False).agg({'total_position': 'sum', 'kind': 'first', 'syd': 'first', 'bb': 'first', 'name': 'first', 'rate': 'first'})
        stocks_baml = stocks_baml.groupby(['underlying_isin', 'price_ccy'],as_index=False).agg({'nominal_quantity': 'sum', 'underlying_security_type_id': 'first','price_date': 'first', 'security_name': 'first', 'rate': 'first'})   
        
        final_df_stocks = pd.merge(stocks_core, stocks_baml,  how='outer', left_on=['isin','currency'], right_on = ['underlying_isin','price_ccy'])
        final_df_stocks.fillna({'total_position': 0, 'nominal_quantity': 0, 'syd': 'No matching isin and currency found in core.', 'isin': 'No matching isin and currency found in core.', 'bb': 'No matching isin and currency found in core.','underlying_isin': 'No matching isin and currency found in baml.', 'currency': final_df_stocks['price_ccy'], 'name': final_df_stocks['security_name'],'underlying_security_type_id': 'No matching isin and currency found in baml.', 'rate_x': final_df_stocks['rate_y']}, inplace=True)
        
        final_df_stocks["nominal_quantity"] = final_df_stocks["nominal_quantity"].astype(float)
        final_df_stocks["total_position"] = final_df_stocks["total_position"].astype(float)
        
        final_df_stocks['delta'] = final_df_stocks['total_position'] - final_df_stocks['nominal_quantity']
        final_df_stocks = final_df_stocks[['syd', 'isin', 'underlying_isin', 'currency', 'underlying_security_type_id', 'nominal_quantity', 'total_position', 'delta', 'bb', 'name', 'rate_x']]
        final_df_stocks.rename(columns={'isin':'core_isin', 'underlying_isin':'baml_isin','nominal_quantity':'BAML Nominal','total_position':'DBC Nominal', 'bb': 'stock_ticker', 'name': 'stock_name', 'rate_x': 'fx_rate'}, inplace=True)
        
        final_df_stocks['delta (EUR)'] = final_df_stocks['delta'] * final_df_stocks['fx_rate']
        
        return final_df_stocks[abs(final_df_stocks['delta (EUR)']) > 0.1]
    
    elif equity_type == "futures":

        
        futures_baml = futures_baml.copy()
        futures_baml['security_code_pref'] = futures_baml['security_code_pref'].str.replace('COMB', '')
        futures_baml['security_code_pref'] = [" ".join(i.split()) for i in futures_baml['security_code_pref']]
        futures_baml['bb_baml'] = futures_baml['security_code_pref']
            
        futures_baml = futures_baml[['price_date','security_name','security_type_id','bb_baml','nominal_quantity','rate','price_ccy']]
        
        futures_core = futures_core.loc[:, ~futures_core.columns.duplicated()]
        futures_core = futures_core.groupby(['bb'],as_index=False).agg({'total_position': 'sum', 'kind': 'first', 'name': 'first', 'syd': 'first', 'rate': 'first','currency': 'first'})
        futures_core = futures_core[futures_core['total_position'] != 0]
        
        final_df_futures = pd.merge(futures_core, futures_baml,  how='outer', left_on=['bb'], right_on = ['bb_baml'])
        final_df_futures = final_df_futures.fillna({'nominal_quantity': 0, 'price_date': date_str, 'currency': final_df_futures['price_ccy'], 'name': final_df_futures['security_name'], 'bb_baml': 'No matching security name found in baml.','rate_x': final_df_futures['rate_y']})
        
        final_df_futures['delta'] = final_df_futures['total_position'] - final_df_futures['nominal_quantity']
        final_df_futures = final_df_futures[['name', 'currency', 'total_position', 'nominal_quantity','delta', 'rate_x']]
        final_df_futures = final_df_futures.rename(columns={'name':'futures_name','nominal_quantity':'BAML Nominal','total_position':'DBC Nominal','rate_x':'fx_rate'})
        
        final_df_futures['delta (EUR)'] = final_df_futures['delta'] * final_df_futures['fx_rate']
        final_df_futures.drop(['fx_rate','delta'],axis=1,inplace=True)
        
        return final_df_futures[abs(final_df_futures['delta (EUR)']) >= 0.1]


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
    
    html_out = str(soup)
    return html_out

def make_styler_dict(df):
    d_out = {}
    for col in df.columns:
        try:
            if df[col].abs().mean() < 10.0:
                d_out[col] = lambda x: '{:,.4f}'.format(x)
            else:
                d_out[col] = lambda x: '{:,.0f}'.format(x)
        except TypeError: # --> it's a non-numerical column
            continue
    return d_out

def get_stylesheet():
    with open('/mnt/cluster/glob/simulations/settings/cluster_weekly_simulations/'\
         'assets/stylesheet.css','r') as fo:
        stylesheet = fo.read()
        
        return stylesheet
    
def output_html_file(html_to_output):
    
    final_html = ''
    for html in html_to_output:
        final_html += '\n' + '<br /><br />' + html
    
    stylesheet = get_stylesheet()
    final_html = stylesheet + '\n' + '<body>' + final_html + '\n' + '</body>'
    
    file = open(args.output,"w")
    file.write(final_html)
    file.close()
    
    
def get_diffs(date_str,equity_type):
    
    if equity_type == 'ST':
        global extra_table
        #Comparisons for all stocks for T-1 and T-2. 
        #When there is "No matching isin and currency found in baml/core.", i.e. not able to find the corresponding isin and currency on either core or baml side => a position of 0 is assumed on that side.
        stocks_comparison = comparison(date_str,'stocks')
        
        stocks_comparison['isin'] = np.where(stocks_comparison['core_isin'] == 'No matching isin and currency found in core.', stocks_comparison['baml_isin'], stocks_comparison['core_isin'])
        stocks_comparison.drop(['core_isin','baml_isin'],axis=1,inplace = True)
        stocks_comparison = stocks_comparison[abs(stocks_comparison['delta']) > 0.01][['isin', 'stock_name','currency','stock_ticker','underlying_security_type_id','BAML Nominal','DBC Nominal', 'delta (EUR)']]
             
        extra_table = get_table_where_isin_ccy_different(stocks_comparison)
        stocks_comparison = pd.concat([extra_table, stocks_comparison]).drop_duplicates(keep=False)
        
        return stocks_comparison
    
    elif equity_type == 'FU':
        
        #Same bur for futures. 
        futures_comparison = comparison(date_str,'futures')
        
        return futures_comparison

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
            
            df = df.groupby(['isin'],as_index=False).agg({'stock_name': 'first', \
                           'currency': 'first', 'stock_ticker': 'first', 'underlying_security_type_id': 'first', \
                           'BAML Nominal': 'first', 'DBC Nominal': 'first', 'delta (EUR)': 'sum', \
                           'baml_adjustments': 'sum', 'event_type_description': 'first'})
            df = df[df['baml_adjustments']!= 0]
        
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
    
def output_differences(equity_type, date_str):
    
    date_dt = datetime.strptime(date_str, '%Y-%m-%d')
    
    if equity_type == 'ST':
        
        #Comparisons for all stocks for T-1 and T-2. 
        #When there is "No matching isin and currency found in baml/core.", i.e. not able to find the corresponding isin and currency on either core or baml side => a position of 0 is assumed on that side.
        columns = ['isin', 'stock_name', 'currency', 'stock_ticker',
       'underlying_security_type_id', 'BAML Nominal', 'DBC Nominal']
        diffs_at_t = get_diffs(date_dt,'ST')[columns]
        diffs_at_t_minus_1 = get_diffs(date_dt-BDay(1),'ST')[columns]
        
        merged = diffs_at_t.merge(diffs_at_t_minus_1, how='left', indicator=True)
        stock_differences_in_t_but_not_t_minus_1 = merged[merged['_merge']=='left_only'].drop(['_merge'],axis=1)
        
        diffs_at_t = calculate_eur_deltas(diffs_at_t,date_str)
        
        diffs_at_t = append_events_to_df(date_str,diffs_at_t)
        stock_differences_in_t_but_not_t_minus_1 = calculate_eur_deltas(stock_differences_in_t_but_not_t_minus_1,date_str)
        
        return diffs_at_t.set_index('isin'), stock_differences_in_t_but_not_t_minus_1.set_index('isin')
    
    elif equity_type == 'FU':
        
        diffs_at_t = get_diffs(date_dt,'FU')
        diffs_at_t_minus_1 = get_diffs(date_dt-BDay(1),'FU')
        
        merged = diffs_at_t.merge(diffs_at_t_minus_1, how='left', indicator=True)
        futures_differences_in_t_but_not_t_minus_1 = merged[merged['_merge']=='left_only'].drop(['_merge'],axis=1)

        return diffs_at_t.set_index('futures_name'),futures_differences_in_t_but_not_t_minus_1.set_index('futures_name')
    
def main():
    
    requested_date = args.d

    if requested_date == None:
        requested_date = (date.today() - BDay(1)).strftime("%Y-%m-%d")
    
    elif datetime.strptime(requested_date, '%Y-%m-%d').weekday() > 4: #if it's weekened we use the latest workday. 
        requested_date = (datetime.strptime(requested_date, '%Y-%m-%d') - BDay(1)).strftime("%Y-%m-%d")
        print ('REQUESTED DATE IS NOT A WEEKDAY - OUTPUTTING SUMMARY FOR THE LATEST WORKING DAY %s' % requested_date)
        
    the_day_before = (datetime.strptime(requested_date, '%Y-%m-%d') - BDay(1)).strftime("%Y-%m-%d")
    

    if baml_df(requested_date) == False:
        print ('BAML HAS NOT RUN POSITIONS YET FOR REQUESTED DATE {requested_date} - OUTPUTTING SUMMARY FOR PREVIOUS WORKING DAY {the_day_before}'.format(requested_date=requested_date,the_day_before=the_day_before))
        requested_date = the_day_before
        stock_differences, stock_differences_at_t_only = output_differences('ST',requested_date)
        futures_differences, futures_differences_at_t_only = output_differences('FU',requested_date)
        
    else:
        stock_differences, stock_differences_at_t_only = output_differences('ST',requested_date)
        futures_differences, futures_differences_at_t_only = output_differences('FU',requested_date)
    
    html_to_output = []
    if stock_differences.empty:
        print ("\nTHERE ARE NO DIFFERENCES FOR STOCKS ON {date}.\n".format(date=requested_date))
        print ()
    
    else:
        html_to_output.append(fix_pandas_html(stock_differences))
        print('\nRECONCILIATION SUMMARY FOR STOCKS ON %s\n' % requested_date)
        print ()
        print (stock_differences.to_string())
        print ()
    
    if not stock_differences_at_t_only.empty:
        
        html_to_output.append(fix_pandas_html(stock_differences_at_t_only))
        print('\nSTOCK DIFFERENCES PRESENT ON %s BUT NOT ON %s\n' % (requested_date,the_day_before))
        print ()
        print (stock_differences_at_t_only.to_string())
        print ()
        
    show_extra_table = args.e
    
    if show_extra_table == 'show' and not extra_table.empty:
        html_to_output.append(fix_pandas_html(extra_table.set_index('isin')))
        print('\nSTOCK DIFFERENCES WHERE NO ISIN OR CURRENCY FOUND ON %s\n' % (requested_date))
        print (extra_table.set_index('isin').to_string())
        print ()
        
    if futures_differences.empty:
        print ("\nTHERE ARE NO DIFFERENCES FOR FUTURES ON {date}.\n".format(date=requested_date))
        print ()
    
    else:
        html_to_output.append(fix_pandas_html(futures_differences))
        print('\nRECONCILIATION SUMMARY FOR FUTURES ON %s\n' % requested_date)
        print ()
        print (futures_differences.to_string())
        print ()
    
    if not futures_differences_at_t_only.empty:
        
        html_to_output.append(fix_pandas_html(futures_differences_at_t_only))
        print('\nFUTURES DIFFERENCES PRESENT ON %s BUT NOT ON %s\n' % (requested_date,the_day_before))
        print ()
        print (futures_differences_at_t_only.to_string())
        print ()
    
    output_html_file(html_to_output)
    
if __name__ == '__main__':
    main()


