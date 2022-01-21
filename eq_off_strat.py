import os
os.environ['option_thd'] = '/data/trh_data-precooked/'
os.environ['option_ds'] = 'thos,thol,tho,qh,bbf'
os.environ['option_dsi'] = 'thos,thol,tho,qh,bbf'
os.environ['option_tld'] = '/precooked/trh_data-precooked'

import pandas as pd
from dbc.postgres import Postgres_Support
from dbc.dbc_mysql import MySQL_Support
from datetime import datetime, date
from pandas.tseries.offsets import BDay
import numpy as np
from tabulate import tabulate
import argparse
from dbc.dbcpass import dbcpass
from market_data import serializer
from market_data import aggregated_lasts_data
from datetime import timedelta
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

parser = argparse.ArgumentParser(
    description='Research for equity offering strategy.')

dbcpass.add_arguments(parser)
args = parser.parse_args()
mysql = MySQL_Support(args)
psql = Postgres_Support(args)

def data_preprocessing(in_sample=True,market='EU'):
    """
    Function to prepare the csv downloaded from bloomberg for analysis. 
    Operations include data type changes and change the exchange symbols to
    align with our engines. 
    """
    if in_sample == True:
        if market == 'EU':
            csv_path = '/home/jimmy/Desktop/eq_off_strat/input_data/in_sample_data_EU.csv'
        else:
            csv_path = '/home/jimmy/Desktop/eq_off_strat/input_data/in_sample_data_ASIA.csv'
    else:
        if market == 'EU':
            csv_path = '/home/jimmy/Desktop/eq_off_strat/input_data/out_of_sample_data_EU.csv'
        else:
            csv_path = '/home/jimmy/Desktop/eq_off_strat/input_data/out_of_sample_data_ASIA.csv'
            
    equity_offering = pd.read_csv(csv_path).dropna(how='all')
    equity_offering.dropna(axis=0, subset=['Security ID'], inplace=True)

    equity_offering['Announce/Declared Date'] = [
        datetime.strptime(equity_offering.iloc[i]['Announce/Declared Date'], '%m/%d/%Y').strftime('%Y-%m-%d') for i in
        range(len(equity_offering))]
    equity_offering['Effective Date'] = [
        datetime.strptime(equity_offering.iloc[i]['Effective Date'], '%m/%d/%Y').strftime('%Y-%m-%d') for i in
        range(len(equity_offering))]
    equity_offering = equity_offering.rename(columns={'Announce/Declared Date': 'Announcement Date'})

    equity_offering['Next Date'] = [
        (datetime.strptime(equity_offering.iloc[i]['Announcement Date'], '%Y-%m-%d') + BDay(1)).strftime('%Y-%m-%d') for
        i in range(len(equity_offering))]
    equity_offering.reset_index(inplace=True, drop=True)

    enter_dates = []
    exit_dates = []
    market_syds = []

    for i in range(len(equity_offering)):

        enter_dates.append(equity_offering.iloc[i]['Effective Date'])
        exit_dates.append(
            (datetime.strptime(equity_offering.iloc[i]['Effective Date'], '%Y-%m-%d') + BDay(1)).strftime('%Y-%m-%d'))

        nreplace = 1
        bb_string = equity_offering['Security ID'][i]
        words = bb_string.split(" ")

        if market == 'EU':
            
            if words[1] == 'SM':
    
                words[nreplace] = "SQ"
                equity_offering.loc[i, ['Security ID']] = " ".join(words)
                market_syds.append(45)
    
            elif words[1] == 'GR':
    
                words[nreplace] = "GY"
                equity_offering.loc[i, ['Security ID']] = " ".join(words)
                market_syds.append(45)
    
            elif words[1] == 'SW':
    
                words[nreplace] = "SE"
                equity_offering.loc[i, ['Security ID']] = " ".join(words)
                market_syds.append(38)
    
            elif words[1] == 'LN':
                market_syds.append(29)
                
            else:
                market_syds.append(45)
                continue
        
        elif market == 'ASIA':
            
            if words[1] == 'JP':
                words[nreplace] = "JT"
                equity_offering.loc[i, ['Security ID']] = " ".join(words)
                market_syds.append(552291)
    
            elif words[1] == 'AU':
                words[nreplace] = "AT"
                equity_offering.loc[i, ['Security ID']] = " ".join(words)
                market_syds.append(553485)  
    
            elif words[1] == 'KS':
                words[nreplace] = "KP"
                equity_offering.loc[i, ['Security ID']] = " ".join(words)
                market_syds.append(552293)  
    
            elif words[1] == 'HK':
                market_syds.append(552292)
                
            elif words[1] == 'SP':
                market_syds.append(553486)            

    equity_offering['enter_position_date'] = enter_dates
    equity_offering['exit_position_date'] = exit_dates
    equity_offering['market_syd'] = market_syds

    return equity_offering


def fetch_prices_and_mkt_cap(syd, price_date, price_to_get='price_close'):
    """
    Function to fetch the historical end of day prices and market cap for a given syd and date.
    """
    with psql.connect() as psql_connection:

        q = "SELECT price,currency,market_capitalization " \
            "FROM (SELECT t1.syd,t1.date,t1.price,t1.currency,t2.company_id FROM (SELECT syd, date, {price_to_get} * dividend_adj_factor AS price, currency FROM core.historical_end_of_day_prices WHERE syd = {syd} AND date = '{price_date}') t1 LEFT JOIN (SELECT syd,company_id FROM core.listings_all t1 LEFT JOIN core.historical_companies t2 on t1.security_id = t2.security_id WHERE '{today}'::date <@ t2.period) t2 ON t1.syd = t2.syd) t1 LEFT JOIN core.market_capitalization_history t2 ON t1.company_id = t2.company_id AND t1.date = t2.date;".format(
            price_to_get=price_to_get, syd=syd, price_date=price_date, today=date.today().strftime('%Y-%m-%d'))

        df = pd.read_sql_query(q, psql_connection)

        # if the stock price is not available for a certain date, we will
        # use the price from the next available date. We will try this for the next
        # 3 business days. If still no price available, give up.
        if df.empty:

            forward_business_days = 1

            while forward_business_days <= 3:

                price_date = (datetime.strptime(price_date, "%Y-%m-%d") + BDay(1)).strftime("%Y-%m-%d")
                df = pd.read_sql_query(q, psql_connection)

                if not df.empty:
                    break

                forward_business_days += 1

            empty_row = pd.Series([np.nan, np.nan, np.nan], index=['price', 'currency', 'market_capitalization'])

            df = df.append(empty_row, ignore_index=True)

            return df

        return df


def fetch_futures_prices(syd, date, price_to_get='price_close'):
    """
    Function to fetch the historical end of day prices of futures given syd and date.
    """
    args.psql_dbs = 'luuk'
    psql = Postgres_Support(args)

    with psql.connect() as psql_connection:

        q = "SELECT %s FROM future_prices.fut_prices WHERE date = %s AND syd = %s;" % (price_to_get, "'%s'" % date, syd)
        df = pd.read_sql_query(q, psql_connection)

        if not df.empty:

            price = df[price_to_get][0]
            return price

        else:
            return np.nan


def fetch_beta(syd, date, syd_market):
    """
    Function to fetch the historical end of day prices of futures given syd and date.
    """

    with mysql.connect() as cnx, cnx.cursor() as cursor:
        query = "select * from betas where syd = %s and available_at < %s and period_kind = '3month' and syd_market = %s and beta is not null order by available_at desc limit 1"
        cursor.execute(query % (syd, "'%s'" % date, syd_market))
        res = cursor.fetchall()
        beta = res[0][-3]

    return beta


def fetch_exchange_rates(cur, date):
    """
    Function to fetch the exchange rates for the relevant dates and currencies.
    """

    with psql.connect() as psql_connection:
        q = "select rate from core.exchange_rate_history where target = 'EUR' and source = '{cur}' and date = '{date}';".format(
            cur=cur, date=date)
        df = pd.read_sql_query(q, psql_connection)

        fx_rate = df.iloc[0]['rate']

    return fx_rate


def get_auction_volumes(cnx, syd, trade_date):
    
    OPENING_AUCTION_TRADE = aggregated_lasts_data.aggregated_lasts.Last_Data_Flag(2)
    CLOSING_AUCTION_TRADE = aggregated_lasts_data.aggregated_lasts.Last_Data_Flag(4)
    
    trade_date = datetime.strptime(trade_date,"%Y-%m-%d")
    
    try:
        begin_date = trade_date
        end_date = trade_date + timedelta(days=1)
    
        B = begin_date - timedelta(minutes=15)
        E = end_date - timedelta(minutes=15)
    
    
        qs = aggregated_lasts_data.Aggregated_Lasts_Loader(syd,B,E)
    
        quotes = []
    
        for quote,item_type in serializer.Serializer([qs]):
            quotes.append(quote)
    
        oa_trade = [i for i in quotes if i.get_flag() == OPENING_AUCTION_TRADE][0]
        ca_trade = [i for i in quotes if i.get_flag() == CLOSING_AUCTION_TRADE][0]
    
        if len([i for i in quotes if i.get_flag() == CLOSING_AUCTION_TRADE]) > 1:
            print("found", syd, trade_date)
    
        currency = str(ca_trade.get_currency())[-3:]
    
        oa_value = oa_trade.get_scalar_price() * oa_trade.get_quantity()
        ca_value = ca_trade.get_scalar_price() * ca_trade.get_quantity()
    
        return oa_value, ca_value, currency
    
    except:
        return 0, 0, 'EUR'
    

def mapping(list_of_names=[], find_leftovers=False, bb=np.nan):
    """
    Mapping function for finding the syd given certain bb name such as 'COFB BB Equity'
    """

    with psql.connect() as psql_connection:

        if find_leftovers == False:
            q = "SELECT * FROM core.my_names WHERE bb IN {list_of_names}".format(list_of_names=list_of_names)
            df = pd.read_sql_query(q, psql_connection)

        else:
            q = "SELECT * FROM core.my_names WHERE bb LIKE '{name}%'".format(name=bb)
            df = pd.read_sql_query(q, psql_connection)

    return df


def bb_to_syd_and_name_mapping(equity_offering):
    """
    Function to return a mapping dictionary where the key is the bb_name such as 
    'COFB BB Equity', and values are tuples containing its syd and full name 
    such as (618559, 'COVESTRO AG')
    The returned mapping dictionary is used as input in the get_df function. 
    """

    list_of_names = tuple(set(equity_offering['Security ID'].values))
    found_tickers = mapping(list_of_names)['bb']

    unfound_tickers = list(set(list_of_names) - set(found_tickers))

    unfound_df = pd.DataFrame()
    for i in range(len(unfound_tickers)):
        unfound_df = unfound_df.append(mapping(find_leftovers=True, bb=unfound_tickers[i]))

    final_df = mapping(list_of_names).append(unfound_df)
    final_df = final_df[['syd', 'name', 'bb']]

    final_df['merged'] = list(zip(final_df['syd'], final_df['name']))
    final_df = final_df.drop(['syd', 'name'], axis=1)
    final_df['bb'] = [final_df['bb'].iloc[i].split('-', 1)[0] for i in range(len(final_df['bb']))]

    mapping_dict = final_df.set_index('bb').to_dict()['merged']

    return mapping_dict



def plot_df_split_field(df, col_to_split, pnl):
    """
    Function to plot the P&L grouped by reasons of the equity offering.
    """
    unique_fields = df[col_to_split].unique()

    df_split = pd.DataFrame()
    
    for field in unique_fields:
        df_field = df.loc[df[col_to_split] == field]
        daily_pnl = df_field[pnl].groupby(level=0).sum().to_frame()
        daily_pnl = daily_pnl.rename(columns={pnl:field})
        df_split = df_split.join(daily_pnl,how='outer')
        
    df_split = df_split.fillna(0)

    return df_split

def get_data(equity_offering, mapping_dict):
    """
    Function to fetch all the relevent data.
    """

    syd_list = []
    name_list = []
    stocks_to_remove = []
    currency = []
    market_capitalization = []
    fx_rates = []
    betas = []
    
    t0_stock_close_prices = []
    t0_futures_close_prices = []
    
    t1_stock_open_prices = []
    t1_stock_close_prices = []
    t1_futures_open_prices = []
    t1_futures_close_prices = []
    
    t2_stock_open_prices = []
    t2_stock_close_prices = []
    t2_futures_open_prices = []
    t2_futures_close_prices = []
    
    volume_ccys = [] 
    t1_auction_open_volumes = []
    t1_auction_close_volumes = []
    t2_auction_open_volumes = []
    t2_auction_close_volumes = []
    
    for i in range(len(equity_offering)):

        try:
            syd_list.append(mapping_dict[equity_offering['Security ID'][i]][0])
            name_list.append(mapping_dict[equity_offering['Security ID'][i]][1])

        except:
            stocks_to_remove.append(equity_offering['Security ID'][i])
            syd_list.append(np.nan)
            name_list.append(np.nan)
            print("Unable to find syd for {bb}, removing from dataset.".format(bb=equity_offering['Security ID'][i]))

    equity_offering['syd'] = syd_list
    equity_offering['name'] = name_list

    equity_offering = equity_offering[~equity_offering['Security ID'].isin(stocks_to_remove)]
    equity_offering = equity_offering[
        ['syd', 'name', 'Security ID', 'Announcement Date', 'Effective Date', 'enter_position_date',
         'exit_position_date', 'market_syd','reason']]
    equity_offering = equity_offering[equity_offering['reason']!='figures']
    equity_offering = equity_offering.reset_index(drop=True)

    for i in range(len(equity_offering)):

        syd = int(equity_offering.iloc[i]['syd'])
        mkt_syd = equity_offering.iloc[i]['market_syd']
        print('Fetching data for syd {syd}...'.format(syd=syd))

        t0 = equity_offering.iloc[i]['Announcement Date']
        t1 = equity_offering.iloc[i]['enter_position_date']
        t2 = equity_offering.iloc[i]['exit_position_date']

        cur = fetch_prices_and_mkt_cap(syd, t0)['currency'].values[0]
        currency.append(cur)
        market_capitalization.append(fetch_prices_and_mkt_cap(syd, t0)['market_capitalization'].values[0])

        # get stock prices and futures prices
        t0_stock_close_price = fetch_prices_and_mkt_cap(syd, t0, price_to_get='price_close')['price'].values[0]
        
        t1_stock_open_price = fetch_prices_and_mkt_cap(syd, t1, price_to_get='price_open')['price'].values[0]
        t1_stock_close_price = fetch_prices_and_mkt_cap(syd, t1, price_to_get='price_close')['price'].values[0]
        
        t2_stock_open_price = fetch_prices_and_mkt_cap(syd, t2, price_to_get='price_open')['price'].values[0]
        t2_stock_close_price = fetch_prices_and_mkt_cap(syd, t2, price_to_get='price_close')['price'].values[0]
        
        exchange_symbol = equity_offering['Security ID'][i].split()[1]

        if exchange_symbol in ['LN', 'SJ']:
            t0_stock_close_prices.append(t0_stock_close_price * 100)
            t1_stock_open_prices.append(t1_stock_open_price * 100)
            t1_stock_close_prices.append(t1_stock_close_price * 100)
            t2_stock_open_prices.append(t2_stock_open_price * 100)
            t2_stock_close_prices.append(t2_stock_close_price * 100)

        else:
            t0_stock_close_prices.append(t0_stock_close_price)
            t1_stock_open_prices.append(t1_stock_open_price)
            t1_stock_close_prices.append(t1_stock_close_price)
            t2_stock_open_prices.append(t2_stock_open_price)
            t2_stock_close_prices.append(t2_stock_close_price)
        
        t0_futures_close_price = fetch_futures_prices(mkt_syd, t0, price_to_get='price_close')
        
        t1_futures_open_price = fetch_futures_prices(mkt_syd, t1, price_to_get='price_open')
        t1_futures_close_price = fetch_futures_prices(mkt_syd, t1, price_to_get='price_close')
        
        t2_futures_open_price = fetch_futures_prices(mkt_syd, t2, price_to_get='price_open')
        t2_futures_close_price = fetch_futures_prices(mkt_syd, t2, price_to_get='price_close')
        
        t1_auction_open_volume,t1_auction_close_volume,volume_ccy = get_auction_volumes(psql.connect(),syd,t1)
        t2_auction_open_volume,t2_auction_close_volume,volume_ccy = get_auction_volumes(psql.connect(),syd,t2)
        
        volume_ccys.append(volume_ccy)
        
        t1_auction_open_volumes.append(t1_auction_open_volume)
        t1_auction_close_volumes.append(t1_auction_close_volume)
        t2_auction_open_volumes.append(t2_auction_open_volume)
        t2_auction_close_volumes.append(t2_auction_close_volume)
        
        t0_futures_close_prices.append(t0_futures_close_price)
        
        t1_futures_open_prices.append(t1_futures_open_price)
        t1_futures_close_prices.append(t1_futures_close_price)
        
        t2_futures_open_prices.append(t2_futures_open_price)
        t2_futures_close_prices.append(t2_futures_close_price)
        
        try:
            beta = fetch_beta(syd, t2, mkt_syd)
            fx_rates.append(fetch_exchange_rates(cur, t1))
            betas.append(beta)

        except:
            print (f'no beta found for syd {syd}')
            fx_rates.append(np.nan)
            betas.append(np.nan)

    equity_offering['currency'] = currency
    equity_offering['market_capitalization'] = market_capitalization
    equity_offering['fx_rates'] = fx_rates
    equity_offering['beta'] = betas
    
    equity_offering['t0_stock_close_price'] = t0_stock_close_prices
    
    equity_offering['t1_stock_open_price'] = t1_stock_open_prices
    equity_offering['t1_stock_close_price'] = t1_stock_close_prices
    equity_offering['t0_futures_close_price'] = t0_futures_close_prices
    equity_offering['t1_futures_open_price'] = t1_futures_open_prices
    equity_offering['t1_futures_close_price'] = t1_futures_close_prices
    
    equity_offering['t2_stock_open_price'] = t2_stock_open_prices
    equity_offering['t2_stock_close_price'] = t2_stock_close_prices
    equity_offering['t2_futures_open_price'] = t2_futures_open_prices
    equity_offering['t2_futures_close_price'] = t2_futures_close_prices
    
    equity_offering['t1_auction_open_volume'] = t1_auction_open_volumes
    equity_offering['t1_auction_close_volume'] = t1_auction_close_volumes
    equity_offering['t2_auction_open_volume'] = t2_auction_open_volumes
    equity_offering['t2_auction_close_volume'] = t2_auction_close_volumes
    equity_offering['volume_ccy'] = volume_ccys
    
    equity_offering.dropna(axis=0, how='any', inplace=True)
    equity_offering.reset_index(drop=True, inplace=True)
    equity_offering = equity_offering.sort_values(by=['Announcement Date'])
    return equity_offering


    
def calculate_pl(final_df, investment_level, investment_cap, strat1=False, strat2=False, strat3=False, strat4=False):
    # 7500 euros investment per 100m market cap
    final_df['investment'] = [final_df['market_capitalization'][i] / 100000000 * (investment_level / final_df['fx_rates'][i]) for i
                              in range(len(final_df))]
    
    # converting the 75000 investment cap to corresponding vlaues in own currencies
    final_df['max_investment'] = investment_cap / final_df['fx_rates']
    
    # capping the maximum investment
    final_df['investment'] = np.where(final_df['investment'] > final_df['max_investment'], final_df['max_investment'],
                                      final_df['investment'])
    
    if strat1 == True:
        final_df['investment'] = np.fmin(final_df['investment'],final_df['t1_auction_close_volume']*0.05)
    
    elif strat2 == True or strat3 == True or strat4 == True:
        final_df['investment'] = np.fmin(final_df['investment'],final_df['t1_auction_open_volume']*0.05)
        
    final_df['investment (EUR)'] = final_df['investment'] * final_df['fx_rates']
    
    final_df['investment_volume_cap (EUR)'] = final_df['t1_auction_open_volume'] * 0.05 * final_df['fx_rates']
    
    if strat1 == True:

        mkt_corrected_returns = (final_df['t1_stock_close_price'] / final_df['t0_stock_close_price'] - 1) \
                            - (final_df['beta'] * (final_df['t1_futures_close_price'] / final_df['t0_futures_close_price'] - 1))
        final_df['mkt_corrected_returns'] = mkt_corrected_returns             
        
        mkt_corrected_return_signals = np.where(mkt_corrected_returns<0,"Negative","Positive")
        final_df['mkt_corrected_return_signal'] = mkt_corrected_return_signals
            
        final_df['return'] = (final_df['t2_stock_close_price'] / final_df['t1_stock_close_price'] - 1) - final_df[
            'beta'] * (final_df['t2_futures_close_price'] / final_df['t1_futures_close_price'] - 1)

        final_df['P&L (EUR)'] = final_df['return'] * final_df['investment (EUR)'] 
        final_df['Cumulative P&L (EUR)'] = final_df['P&L (EUR)'].cumsum()
        return final_df
    
    elif strat2 == True:

        mkt_corrected_returns = (final_df['t1_stock_open_price'] / final_df['t0_stock_close_price'] - 1) \
                            - (final_df['beta'] * (final_df['t1_futures_open_price'] / final_df['t0_futures_close_price'] - 1))
        final_df['mkt_corrected_returns'] = mkt_corrected_returns  
        
        mkt_corrected_return_signals = np.where(mkt_corrected_returns<0,"Negative","Positive")
        final_df['mkt_corrected_return_signal'] = mkt_corrected_return_signals
            
        final_df['return'] = (final_df['t1_stock_close_price'] / final_df['t1_stock_open_price'] - 1) - \
                             final_df['beta'] * (final_df['t1_futures_close_price'] / final_df['t1_futures_open_price'] - 1)

        final_df['P&L (EUR)'] = final_df['return'] * final_df['investment (EUR)'] 
        final_df['Cumulative P&L (EUR)'] = final_df['P&L (EUR)'].cumsum()
        return final_df
    
    elif strat3 == True:
        
        mkt_corrected_returns = (final_df['t1_stock_open_price'] / final_df['t0_stock_close_price'] - 1) \
                            - (final_df['beta'] * (final_df['t1_futures_open_price'] / final_df['t0_futures_close_price'] - 1))
        final_df['mkt_corrected_returns'] = mkt_corrected_returns 
                            
        mkt_corrected_return_signals = np.where(mkt_corrected_returns<0,"Negative","Positive")
        final_df['mkt_corrected_return_signal'] = mkt_corrected_return_signals
        
        final_df['return'] = (final_df['t2_stock_close_price'] / final_df['t1_stock_open_price'] - 1) - \
                             final_df['beta'] * (final_df['t2_futures_close_price'] / final_df[
            't1_futures_open_price'] - 1)
            
        final_df['P&L (EUR)'] = final_df['return'] * final_df['investment (EUR)'] 
        final_df['Cumulative P&L (EUR)'] = final_df['P&L (EUR)'].cumsum()
        
        return final_df
    
    elif strat4 == True:
        
        mkt_corrected_returns = (final_df['t1_stock_open_price'] / final_df['t0_stock_close_price'] - 1) \
                            - (final_df['beta'] * (final_df['t1_futures_open_price'] / final_df['t0_futures_close_price'] - 1))
        final_df['mkt_corrected_returns'] = mkt_corrected_returns 
                            
        mkt_corrected_return_signals = np.where(mkt_corrected_returns<0,"Negative","Positive")
        final_df['mkt_corrected_return_signal'] = mkt_corrected_return_signals
        
        final_df['return'] = (final_df['t2_stock_close_price'] / final_df['t1_stock_open_price'] - 1) - \
                             final_df['beta'] * (final_df['t2_futures_close_price'] / final_df[
            't1_futures_open_price'] - 1)
            
        final_df['futures_return'] = final_df['beta'] * (final_df['t2_futures_close_price'] / final_df[
            't1_futures_open_price'] - 1)
            
        final_df['investment_cap'] = investment_cap
        
        final_df['volume_bought_at_open'] = final_df['investment_volume_cap (EUR)'] * final_df['fx_rates'] / final_df['t1_stock_open_price'] 
        final_df['volume_to_fill'] = np.fmin( final_df['investment_cap'] / final_df['t1_stock_open_price'] - final_df['volume_bought_at_open'], final_df['t1_auction_close_volume'] * 0.05 / final_df['t1_stock_close_price'])
          
        final_df['avg_enter_prices'] = (final_df['t1_stock_open_price'] * final_df['volume_bought_at_open'] + final_df['t1_stock_close_price'] * final_df['volume_to_fill']) / (final_df['volume_bought_at_open'] + final_df['volume_to_fill'])
        
        final_df['adjusted_return'] = (final_df['t2_stock_close_price'] / final_df['avg_enter_prices']-1) - final_df['beta'] * final_df['futures_return']
        
        final_df['return'] = np.where(final_df['investment_volume_cap (EUR)']==\
                final_df['investment (EUR)'],\
                final_df['adjusted_return'],\
                final_df['return'])
        
        final_df['adjusted_investment (EUR)'] = final_df['volume_to_fill'] * final_df['t2_stock_close_price'] + final_df['volume_bought_at_open'] * final_df['avg_enter_prices']
        final_df['investment (EUR)'] = np.where(final_df['investment_volume_cap (EUR)']==\
        final_df['investment (EUR)'],\
        final_df['adjusted_investment (EUR)'],\
        final_df['investment (EUR)'])
        
        final_df['P&L (EUR)'] = final_df['return'] * final_df['investment (EUR)'] 
        final_df['Cumulative P&L (EUR)'] = final_df['P&L (EUR)'].cumsum()
        final_df.dropna(inplace=True)
        return final_df
    
def evaluate_performance(df, investment_level, investment_cap):
    """
    Function to output the performance metrics for strategies.
    """
    # calculate total investment
    total_investment = int(sum(df['investment (EUR)']))
    
    # calculate net profit
    net_profit = int(sum(df['P&L (EUR)']))

    # calculate average winner and loser
    avg_profit = int(df['P&L (EUR)'].mean())
    
    final_return = int(net_profit / total_investment * 10000)
    
    print(f'\nPerformance for investment level of EUR{investment_level} per 100m mkt cap and max investment of EUR{investment_cap}:\n')
    print(f'\nTotal investment in EUR is €{total_investment}.\n')
    print(f'Total return is {final_return} basis points.\n')
    print(f'Net profit is €{net_profit}.\n')
    print(f'Average profit is €{avg_profit}.\n')
    
def aggregate_by_reason(df):
    
    df['Average Investment (EUR)'] = df['investment (EUR)']
    df['Frequency'] = 1
    aggregated_df = df.groupby('reason').agg({'P&L (EUR)': 'sum', 'investment (EUR)': 'sum', 'Average Investment (EUR)': 'mean', 'Frequency': 'sum'})
    aggregated_df['Total Return (Basis Points)'] = aggregated_df['P&L (EUR)'] / aggregated_df['investment (EUR)'] * 10000
    
    aggregated_df = aggregated_df.astype(int)
    aggregated_df['P&L (EUR)'] = aggregated_df['P&L (EUR)'].apply(lambda x : "{:,}".format(x))
    aggregated_df['investment (EUR)'] = aggregated_df['investment (EUR)'].apply(lambda x : "{:,}".format(x))
    aggregated_df['Average Investment (EUR)'] = aggregated_df['Average Investment (EUR)'].apply(lambda x : "{:,}".format(x))
    
    return aggregated_df


def aggregate_by_reason_with_signal(strat_df,reason):
    
    strat_df.reset_index(drop=True,inplace=True)
    idx = np.where(strat_df['reason']==reason)
    df = strat_df.loc[idx]
    
    df['Frequency'] = 1
    df['Avg Investment (EUR)'] = df['investment (EUR)']
    
    if reason == 'secondary offering':
        df.reset_index(drop=True,inplace=True)
        df['quantile'] = None

        q1_val,q2_val,q3_val,q4_val,q5_val = get_quantile_vals(df['mkt_corrected_returns'])
        
        idx_q1 = np.where((df['mkt_corrected_returns']>=q1_val) & (df['mkt_corrected_returns']<q2_val))[0]
        idx_q2 = np.where((df['mkt_corrected_returns']>=q2_val) & (df['mkt_corrected_returns']<q3_val))[0]
        idx_q3 = np.where((df['mkt_corrected_returns']>=q3_val) & (df['mkt_corrected_returns']<q4_val))[0]
        idx_q4 = np.where((df['mkt_corrected_returns']>=q4_val) & (df['mkt_corrected_returns']<=q5_val))[0]

        df.loc[idx_q1,['quantile']] = "1st Quantile"
        df.loc[idx_q2,['quantile']] = "2nd Quantile"
        df.loc[idx_q3,['quantile']] = "3rd Quantile"
        df.loc[idx_q4,['quantile']] = "4th Quantile"
        
        aggregated_df = df.groupby(['reason','quantile']).agg({'P&L (EUR)': 'sum', 'Avg Investment (EUR)':'mean', 'investment (EUR)': 'sum', 'Frequency': 'sum'})

    else:   
        aggregated_df = df.groupby(['reason','mkt_corrected_return_signal']).agg({'P&L (EUR)': 'sum', 'Avg Investment (EUR)':'mean', 'investment (EUR)': 'sum', 'Frequency': 'sum'})
        
    aggregated_df['Total Return (Basis Points)'] = aggregated_df['P&L (EUR)'] / aggregated_df['investment (EUR)'] * 10000
    
    aggregated_df = aggregated_df.astype(int)
    aggregated_df['P&L (EUR)'] = aggregated_df['P&L (EUR)'].apply(lambda x : "{:,}".format(x))
    aggregated_df['investment (EUR)'] = aggregated_df['investment (EUR)'].apply(lambda x : "{:,}".format(x))
    aggregated_df['Avg Investment (EUR)'] = aggregated_df['Avg Investment (EUR)'].apply(lambda x : "{:,}".format(x))
    
    return aggregated_df

def plot_cum_pl_graph(strat_7500,strat_15000,strat_25000):
    
    plot_df_split_field(strat_25000.set_index('Announcement Date'),'reason','P&L (EUR)').cumsum().plot(figsize=(8,6))
    
    strat_7500 = strat_7500[['Announcement Date','Cumulative P&L (EUR)']]
    strat_15000 = strat_15000['Cumulative P&L (EUR)']
    strat_25000 = strat_25000['Cumulative P&L (EUR)']
    
    df = pd.concat([strat_7500, strat_15000, strat_25000], axis=1)
    df.set_index('Announcement Date',inplace=True)
    df.columns = ['Investment Level 7500', 'Investment Level 15000', 'Investment Level 25000']
    df.plot(figsize=(10,8))
    
def plot_comparison(strat_df):
    """
    Function to plot the difference in P&L with positive vs negative mkt corrected returns signals.
    """
    df1 = strat_df[strat_df['mkt_corrected_return_signal']=='Negative']
    df2 = strat_df[strat_df['mkt_corrected_return_signal']=='Positive']
    
    df = pd.merge(df1, df2,  how='outer',left_on=['Announcement Date','syd'],right_on=['Announcement Date','syd'])
    df = df[['Announcement Date', 'P&L (EUR)_x', 'P&L (EUR)_y']]
    df[['P&L (EUR)_x', 'P&L (EUR)_y']] = df[['P&L (EUR)_x', 'P&L (EUR)_y']].fillna(value=0)
    
    df = df.sort_values('Announcement Date')
    df['Cumulative P&L (EUR)_x'] = df['P&L (EUR)_x'].cumsum()
    df['Cumulative P&L (EUR)_y'] = df['P&L (EUR)_y'].cumsum()
    
    df.drop(['P&L (EUR)_x', 'P&L (EUR)_y'],1,inplace=True)
    df.set_index('Announcement Date',inplace=True)
    df.columns = ['With Negative Mkt Corrected Return', 'With Positive Mkt Corrected Return']
    df.plot(figsize=(10,8))

def plot_comparison_by_reason(strat_df,reason,suffix):
    """
    Function to plot the difference in P&L gruoped by reason of offering.
    """
    
    if reason == 'secondary offering':
        strat_df = strat_df[strat_df['reason']==reason]
        plot_quantiles(strat_df,suffix)
        return
    
    idx = np.where((strat_df['mkt_corrected_return_signal']=='Negative') & (strat_df['reason']==reason))
    df1 = strat_df.loc[idx]
    
    idx = np.where((strat_df['mkt_corrected_return_signal']=='Positive') & (strat_df['reason']==reason))
    df2 = strat_df.loc[idx]    
    
    df = pd.merge(df1, df2,  how='outer',left_on=['Announcement Date','syd'],right_on=['Announcement Date','syd'])
    df = df[['Announcement Date', 'P&L (EUR)_x', 'P&L (EUR)_y']]
    df[['P&L (EUR)_x', 'P&L (EUR)_y']] = df[['P&L (EUR)_x', 'P&L (EUR)_y']].fillna(value=0)
    
    df = df.sort_values('Announcement Date')
    df['Cumulative P&L (EUR)_x'] = df['P&L (EUR)_x'].cumsum()
    df['Cumulative P&L (EUR)_y'] = df['P&L (EUR)_y'].cumsum()
    
    df.drop(['P&L (EUR)_x', 'P&L (EUR)_y'],1,inplace=True)
    df.set_index('Announcement Date',inplace=True)
    df.columns = ['With Negative Mkt Corrected Return', 'With Positive Mkt Corrected Return']
    df.plot(figsize=(10,8),title=f"P&L Graph Comparison For {reason.title()} " + suffix)

def get_quantile_vals(series):
    
    q1_val,q2_val,q3_val,q4_val,q5_val = series.quantile([0.00, 0.25, 0.50, 0.75, 1.00]).tolist()
    
    return q1_val,q2_val,q3_val,q4_val,q5_val

def plot_quantiles(df,suffix):
    """
    Function to break dataframe into 4 quantiles based on mkt corrected returns and plot the P&L graph.
    """
    q1_val,q2_val,q3_val,q4_val,q5_val = get_quantile_vals(df['mkt_corrected_returns'])
    
    q1 = df[(df['mkt_corrected_returns']>=q1_val) & (df['mkt_corrected_returns']<q2_val)]
    q2 = df[(df['mkt_corrected_returns']>=q2_val) & (df['mkt_corrected_returns']<q3_val)] 
    q3 = df[(df['mkt_corrected_returns']>=q3_val) & (df['mkt_corrected_returns']<q4_val)]
    q4 = df[(df['mkt_corrected_returns']>=q4_val) & (df['mkt_corrected_returns']<=q5_val)]
    
    merged_df = pd.merge(q1, q2,  how='outer',left_on=['Announcement Date','syd'],right_on=['Announcement Date','syd'])
    merged_df = merged_df.rename(columns={"P&L (EUR)_x": "P&L (EUR)_q1", "P&L (EUR)_y": "P&L (EUR)_q2"})
    
    merged_df = pd.merge(merged_df, q3,  how='outer',left_on=['Announcement Date','syd'],right_on=['Announcement Date','syd'])
    merged_df = merged_df.rename(columns={"P&L (EUR)": "P&L (EUR)_q3"})
    
    merged_df = pd.merge(merged_df, q4,  how='outer',left_on=['Announcement Date','syd'],right_on=['Announcement Date','syd'])
    merged_df = merged_df.rename(columns={"P&L (EUR)": "P&L (EUR)_q4"})
    
    merged_df = merged_df[['Announcement Date', 'P&L (EUR)_q1', 'P&L (EUR)_q2','P&L (EUR)_q3','P&L (EUR)_q4']]
    merged_df[['P&L (EUR)_q1', 'P&L (EUR)_q2','P&L (EUR)_q3','P&L (EUR)_q4']] = merged_df[['P&L (EUR)_q1', 'P&L (EUR)_q2','P&L (EUR)_q3','P&L (EUR)_q4']].fillna(value=0) 
    
    merged_df = merged_df.sort_values('Announcement Date')
    merged_df['Cumulative P&L (EUR)_q1'] = merged_df['P&L (EUR)_q1'].cumsum()
    merged_df['Cumulative P&L (EUR)_q2'] = merged_df['P&L (EUR)_q2'].cumsum()
    merged_df['Cumulative P&L (EUR)_q3'] = merged_df['P&L (EUR)_q3'].cumsum()
    merged_df['Cumulative P&L (EUR)_q4'] = merged_df['P&L (EUR)_q4'].cumsum()
    
    merged_df.drop(['P&L (EUR)_q1', 'P&L (EUR)_q2','P&L (EUR)_q3','P&L (EUR)_q4'],1,inplace=True)
    merged_df.set_index('Announcement Date',inplace=True)
    merged_df.columns = ['1st Quantile', '2nd Quantile', '3rd Quantile', '4th Quantile']
    merged_df.plot(figsize=(10,8),title="P&L Graph Comparison For Secondary Offering " + suffix) 
    
def print_pl_per_reason(strat_df,reason): 
    print (tabulate(aggregate_by_reason_with_signal(strat_df,reason), tablefmt="grid",headers=['Reason','Total P&L','Avg Investment','Total Investment','Frequency','Total Return']))

def print_total_pl_per_reason(strat_df):
    df = aggregate_by_reason(strat_df)
    print (tabulate(df, tablefmt="grid",headers=['Reason','Total P&L','Total Investment','Avg Investment','Frequency','Total Return']))
    
def main():
    
    # data cleanse the csv file
    equity_offering = data_preprocessing(in_sample=True,market='ASIA')

    # get the mapping between bb names and (syd,security name)
    mapping_dict = bb_to_syd_and_name_mapping(equity_offering)

    # fetching all the relevant data
    data = get_data(equity_offering, mapping_dict)
    
    # evaluate strategy performances
    ###########################################################################
    print('\nStrategy 1 Performance Metrics:\n')
    strat1_7500 = calculate_pl(data.copy(), investment_level=7500, investment_cap=75000, strat1=True)  # strat 1: the strategy currently being implemented - we buy at market close and sell on the next market close
    evaluate_performance(strat1_7500, investment_level=7500, investment_cap=75000) 
    plot_comparison(strat1_7500)
    print_total_pl_per_reason(strat1_7500)
    
    strat1_15000 = calculate_pl(data.copy(), investment_level=15000, investment_cap=150000, strat1=True)  
    evaluate_performance(strat1_15000, investment_level=15000, investment_cap=75000) 
    for reason in data['reason'].unique():
        print_pl_per_reason(strat1_15000,reason)
        plot_comparison_by_reason(strat1_15000,reason,"When We Buy On Close of T And Sell on Close of T+1")
    print_total_pl_per_reason(strat1_15000)
    
    strat1_25000 = calculate_pl(data.copy(), investment_level=25000, investment_cap=250000, strat1=True)  
    evaluate_performance(strat1_25000, investment_level=25000, investment_cap=250000) 
    print_total_pl_per_reason(strat1_25000)
    
    plot_cum_pl_graph(strat1_7500,strat1_15000,strat1_25000)
    
    ###########################################################################
    print('\nStrategy 2 Performance Metrics:\n')
    strat2_7500 = calculate_pl(data.copy(), investment_level=7500, investment_cap=75000, strat2=True)  # strat 2: buy on t1 open and sell on t1 close
    evaluate_performance(strat2_7500, investment_level=7500, investment_cap=75000) 
    plot_comparison(strat2_7500)
    print_total_pl_per_reason(strat2_7500)
    
    strat2_15000 = calculate_pl(data.copy(), investment_level=15000, investment_cap=150000, strat2=True)  
    evaluate_performance(strat2_15000, investment_level=15000, investment_cap=150000) 
    for reason in data['reason'].unique():
        print_pl_per_reason(strat2_15000,reason)
        plot_comparison_by_reason(strat2_15000,reason,"When We Buy On Open of T And Sell on Close of T")
    print_total_pl_per_reason(strat2_15000)
    
    strat2_25000 = calculate_pl(data.copy(), investment_level=25000, investment_cap=250000, strat2=True)  
    evaluate_performance(strat2_25000, investment_level=25000, investment_cap=250000) 
    print_total_pl_per_reason(strat2_25000)
    
    plot_cum_pl_graph(strat2_7500,strat2_15000,strat2_25000)

    ###########################################################################
    print('\nStrategy 3 Performance Metrics:\n')
    strat3_7500 = calculate_pl(data.copy(), investment_level=7500, investment_cap=75000, strat3=True)  # strat 3: buy on t1 open and sell on t2 close
    evaluate_performance(strat3_7500, investment_level=7500, investment_cap=75000) 
    print_total_pl_per_reason(strat3_7500)
    
    strat3_15000 = calculate_pl(data.copy(), investment_level=15000, investment_cap=150000, strat3=True)   
    evaluate_performance(strat3_15000, investment_level=15000, investment_cap=150000) 
    for reason in data['reason'].unique():
        print_pl_per_reason(strat3_15000,reason)
        plot_comparison_by_reason(strat3_15000,reason,"When We Buy On Open of T And Sell on Close of T+1")
    print_total_pl_per_reason(strat3_15000)
    
    strat3_25000 = calculate_pl(data.copy(), investment_level=25000, investment_cap=250000, strat3=True)  
    evaluate_performance(strat3_25000, investment_level=25000, investment_cap=250000) 
    print_total_pl_per_reason(strat3_25000)

    plot_cum_pl_graph(strat3_7500,strat3_15000,strat3_25000)
    
    ###########################################################################
    print('\nStrategy 4 Performance Metrics:\n')
    strat4_7500 = calculate_pl(data.copy(), investment_level=7500, investment_cap=75000, strat4=True)  # strat 4: same as strategy 3 but now we buy the remaining shares in the close if not completely filled in the open
    evaluate_performance(strat4_7500, investment_level=7500, investment_cap=75000) 
    print_total_pl_per_reason(strat4_7500)
    
    strat4_15000 = calculate_pl(data.copy(), investment_level=15000, investment_cap=150000, strat4=True)  
    evaluate_performance(strat4_15000, investment_level=15000, investment_cap=150000) 
    for reason in data['reason'].unique():
        print_pl_per_reason(strat4_15000,reason)
        plot_comparison_by_reason(strat4_15000,reason,"When We Buy On Open of T (buy remaining in Close of T if restricted in Open) And Sell on Close of T+1")
    print_total_pl_per_reason(strat4_15000)
    
    strat4_25000 = calculate_pl(data.copy(), investment_level=25000, investment_cap=250000, strat4=True) 
    evaluate_performance(strat4_25000, investment_level=25000, investment_cap=250000) 
    print_total_pl_per_reason(strat4_25000)
    
    plot_cum_pl_graph(strat4_7500,strat4_15000,strat4_25000)

    
if __name__ == '__main__':
    main()
