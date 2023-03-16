# -*- coding: utf-8 -*-
"""
Created on Mon Jan  9 11:14:25 2017

@author: Sameer.Kulkarni
"""
import pandas as pd
import numpy as np
import getData
import logging


logger = logging.getLogger(f'Recomservicelog.{__name__}')
n = 10


def getrcp(df_raw, pid, db_confg):

    myquery = ("SELECT season_number as seasonid, dow1, dow2, dow3, dow4, dow5, dow6, dow7 FROM slopes WHERE "
               "client_id =:prid and season_number > 0")
    
    try:
        logger.info("--- Fetching the Slope ---")
        slope = getData.getData(myquery=myquery, pid=pid, db_confg=db_confg)
        logger.debug(slope.head(n))
    except:
        logger.error("--- No Slope data available. Check the calibration ---")

    myquery = ("SELECT season_number as seasonid, dow1, dow2, dow3, dow4, dow5, dow6, dow7 FROM intercepts WHERE "
               "client_id =:prid and season_number > 0")
    try:
        logger.info("--- Fetching the Intercept ---"   )
        intercept =  getData.getData(myquery=myquery, pid=pid, db_confg=db_confg)
        logger.debug(intercept.head(n))
    except:
        logger.error("--- No Intercept data available. Check the calibration ---")

    myquery = "SELECT capacity FROM clients WHERE id =:prid"
    try:
        logger.info("Get the Hotel Details")
        hCapacity = getData.getData(myquery=myquery, pid=pid, db_confg=db_confg)
    except:
        logger.error("--- Unable to fetch Hotel Details ---")

    hCap = hCapacity.loc[0].values[0]
    logger.info("The Hotel Capacity - %s ", hCap)
    logger.info('calculating dow factor ')
    df_slop = pd.DataFrame(slope)
    slope_collist = slope.columns.to_list()
    df_slop = pd.melt(df_slop, id_vars=slope_collist[0], var_name='dow',
                      value_name='slop_value')
    intercept_collist = intercept.columns.to_list()
    df_intercept = pd.DataFrame(intercept)
    df_intercept = pd.melt(df_intercept, id_vars=intercept_collist[0], var_name='dow',
                           value_name='intercept_value')
    df_slopintercept = pd.merge(df_slop, df_intercept, on=['seasonid', 'dow'])
    df_slopintercept['lRate'] = (hCap - df_slopintercept['intercept_value'])/df_slopintercept['slop_value']
    df_slopintercept = pd.DataFrame(df_slopintercept)
    df_csonminrate= df_slopintercept.groupby(by='seasonid').min()['lRate'].reset_index().rename(
        columns={'lRate': 'min_cson_Rate'})
    df_slopintercept = df_slopintercept.merge(df_csonminrate, on='seasonid', how='left')
    df_slopintercept['dow_factor'] = df_slopintercept['lRate']/df_slopintercept['min_cson_Rate']
    logger.info('creating dow factor df')
    df_dowfact = pd.DataFrame(df_slopintercept, columns=['seasonid', 'dow', 'dow_factor'])
    df_dowfact['dow'] = df_dowfact['dow'].str.replace('dow', '').astype(int)
    logger.debug(df_dowfact.head(n))

    # ============================================================
    # myquery = ("select seasonnumber, concat('WkNum', fromweekcondition, fromweek, ' ',condition , ' ', 'WkNum',"
    #            " toweekcondition, toweek) as array_cson from season_details WHERE seasonnumber > 0 and"
    #            " propertyid =:prid")
    # try:
    #     logger.info("--- Fetching the Seasonality Details ---")
    #     df_cson =  getData.getData(myquery=myquery, pid=pid, db_confg=db_confg)
    #     # logger.debug(df_cson)
    # except:
    #     logger.error("--- No Seasonality Details avilable. Please check the configuration ---"   )
    # ==============================================================

    # qcson = ("select hotel_id, start_week, end_week, max_capacity, min_price FROM seasonality_definitions WHERE
    # client_id =:prid and number > 0" )
    # qcson = ("select hotel_id, start_week, end_week, (case when cap_override <= 0 then  max_capacity
    # else cap_override"
    #          " end) as max_capacity, (case when price_override <= 0 then  min_price else price_override end)"
    #          " as  min_price FROM seasonality_definitions WHERE client_id = :prid and number > 0")

    qcson = ("select hotel_id, start_week, end_week, (case when cap_override <= 0 then  max_capacity"
             " when cap_override is null then  max_capacity else cap_override end) as max_capacity,"
             " (case when price_override <= 0 then  min_price when price_override is null then  min_price "
             "else price_override end) as  min_price FROM seasonality_definitions "
             "WHERE client_id = :prid and number > 0")
    try:
        logger.info("--- Fetching the Seasonal Definition ---"   )
        df_cson_db = getData.getData(myquery=qcson, pid=pid, db_confg=db_confg)
        logger.debug(df_cson_db.head(n))
    except:
        logger.error("--- No Seasonality Details avilable. Please check the configuration ---"   )

    df_cson_db['jCon'] = np.where((df_cson_db['start_week']>df_cson_db['end_week']),"or","and")

    df_cson_db['array_cson'] = df_cson_db.apply(lambda x:"WkNum >= %s %s WkNum <= %s"  % (x['start_week'],x['jCon'],x['end_week']),axis=1)

    df_cson = pd.DataFrame(df_cson_db,columns=['array_cson'])

    aSeas = df_cson['array_cson'].values
    
    mCap = df_cson_db['max_capacity'].values

    mPrice = df_cson_db['min_price'].values

    df_raw=df_raw.fillna(0)

    # ==============================================================

    logger.info("--- Identifying the Seasons for the dates in question ---")
    df_rcp=pd.DataFrame(df_raw,columns=['occupancydate', 'cm_capacity','rm_sold'])
    df_rcp['occupancydate'] = pd.to_datetime(df_rcp['occupancydate'])
    df_rcp['dow'] = df_rcp['occupancydate'].dt.dayofweek+1
    df_rcp['dtyr'] = df_rcp['occupancydate'].dt.year
    df_rcp['dt1'] = pd.to_datetime(dict(year=df_rcp.dtyr,month=1,day=8))
    df_rcp['dt2'] = pd.to_datetime(dict(year=df_rcp.dtyr,month=1,day=6))
    df_rcp['dtwk'] = df_rcp['dt2'].dt.dayofweek + 2                          
    df_rcp['dt3'] = df_rcp['dt1'] - pd.TimedeltaIndex(df_rcp['dtwk'], unit='D')
    df_rcp['dtdf'] = (df_rcp['dt3']-df_rcp['occupancydate']).apply(lambda x: x/np.timedelta64(1,'D'))    
    df_rcp.loc[(df_rcp['dtdf'] <= 0.0), 'WkNum'] = df_rcp['occupancydate'].apply(lambda x: x.strftime('%W'))
    df_rcp.loc[(df_rcp['dtdf'] > 0.0), 'WkNum'] = 52
    df_rcp['WkNum'] = pd.to_numeric(df_rcp['WkNum'])
    df_rcp['WkNum'] = np.where((df_rcp['WkNum'] == 53), 0, df_rcp['WkNum'])

    logger.debug("Printing the seasonality identified data")
    logger.debug(df_rcp.head(n))
    
    df_rcp_season = pd.DataFrame()

    for i in range(len(aSeas)):
        season = aSeas[i]
        mxCap = mCap[i]

        # Minimum rate set by the rate calibration/client
        hPrice = mPrice[i]

        cson1 = pd.DataFrame(df_rcp.query(season))
        # Fetching data by Season
        # cson1 = df_rcp.query(season)
        cson1['min_cap'] = 0
        cson1['max_cap'] = mxCap
        cson1['min_price'] = hPrice
        if len(cson1.index) > 0:    
            logger.info(" %s - %s ",season,i)
            cson2 = pd.DataFrame(cson1)
            cson2['seasonid'] = i+1
            # logger.debug(cson2)
            df_rcp_season = df_rcp_season.append(cson2,ignore_index=True)

    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    # This entire code will be rewritten based on the changes in the Algorithm
    # The pricing will be based on pickup and number of rooms allocated by the
    # hotel to sell on "On Line Distribution Platforms"
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    # df_rcp_season['yval'] = df_rcp_season.apply(lambda row:min([row['capacity'], max([row['remcapacity'],
    #                                                                                   row['cmacapacity']])]), axis=1)

    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    # df_rcp['yval'] = np.where((df_rcp['yval']==0),df_rcp['Capacity'],df_rcp['yval'])
    logger.info("--- Fetching the Slop Details and merging with the data ---")
    df_rcp_season = df_rcp_season.merge(slope,on='seasonid',how='left')
    df_rcp_season.loc[df_rcp_season['dow']==1,'slope'] = df_rcp_season['dow1']
    df_rcp_season.loc[df_rcp_season['dow']==2,'slope'] = df_rcp_season['dow2']
    df_rcp_season.loc[df_rcp_season['dow']==3,'slope'] = df_rcp_season['dow3']
    df_rcp_season.loc[df_rcp_season['dow']==4,'slope'] = df_rcp_season['dow4']
    df_rcp_season.loc[df_rcp_season['dow']==5,'slope'] = df_rcp_season['dow5']
    df_rcp_season.loc[df_rcp_season['dow']==6,'slope'] = df_rcp_season['dow6']
    df_rcp_season.loc[df_rcp_season['dow']==7,'slope'] = df_rcp_season['dow7']

    delcols = ['dtyr','dt1', 'dt2', 'dt3', 'dtwk','dtdf','dow1', 'dow2', 'dow3', 'dow4', 'dow5', 'dow6', 'dow7']
    df_rcp_season = df_rcp_season.drop(delcols, axis=1)
    
    logger.info("--- Fetching the Intercept Details and merging with the data ---")
    df_rcp_season = df_rcp_season.merge(intercept,on='seasonid',how='left')
    df_rcp_season.loc[df_rcp_season['dow']==1,'intercept'] = df_rcp_season['dow1']
    df_rcp_season.loc[df_rcp_season['dow']==2,'intercept'] = df_rcp_season['dow2']
    df_rcp_season.loc[df_rcp_season['dow']==3,'intercept'] = df_rcp_season['dow3']
    df_rcp_season.loc[df_rcp_season['dow']==4,'intercept'] = df_rcp_season['dow4']
    df_rcp_season.loc[df_rcp_season['dow']==5,'intercept'] = df_rcp_season['dow5']
    df_rcp_season.loc[df_rcp_season['dow']==6,'intercept'] = df_rcp_season['dow6']
    df_rcp_season.loc[df_rcp_season['dow']==7,'intercept'] = df_rcp_season['dow7']

    delcols = ['dow1', 'dow2', 'dow3', 'dow4', 'dow5', 'dow6', 'dow7']
    df_rcp_season = df_rcp_season.drop(delcols, axis=1)
    logger.info('merge df_rcp_season and df_dowfact')
    df_rcp_season = df_rcp_season.merge(df_dowfact, on=['seasonid', 'dow'], how='left')
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    # This Formulation will be changed according to new Enhanced Algorithm
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    #    logger.info("--- Calculating the Rate Recommendation based on RCP Algo ---"   )
    #    df_rcp_season['rcp']= (df_rcp_season['yval'] - df_rcp_season['intercept'])/df_rcp_season['slope']
    
    df_rcp_season['min_rate_temp'] = np.where(df_rcp_season['max_cap'] >= hCap,
                                          df_rcp_season['max_cap'], hCap)
    df_rcp_season['min_rate'] = (np.where(df_rcp_season['min_rate_temp'] >= df_rcp_season['intercept'],
                                          hCap, df_rcp_season['min_rate_temp']) -
                                 df_rcp_season['intercept']) / df_rcp_season['slope']

    df_rcp_season['max_rate']= (df_rcp_season['min_cap'] - df_rcp_season['intercept'])/df_rcp_season['slope']
    df_rcp_season['rate_dif']= (df_rcp_season['max_rate'] - df_rcp_season['min_rate'])

    logger.info('calculate dow min rate')
    df_rcp_season['dow_min_price'] = df_rcp_season['min_price']*df_rcp_season['dow_factor']
    # df_rcp_season['dow_min_price'] = np.where(
    #     df_rcp_season['dow_min_price'] > df_rcp_season['min_rate'] * 2.5, df_rcp_season['min_rate'] * 2.5,
    #     np.where(df_rcp_season['dow_min_price'] < df_rcp_season['min_rate'] * 0.5,
    #              df_rcp_season['min_rate'] * 0.5, df_rcp_season['dow_min_price']))
    df_rcp_season['dow_min_price'] = np.where(
        df_rcp_season['dow_min_price'] < df_rcp_season['min_rate'] * 0.5,
        df_rcp_season['min_rate'] * 0.5, df_rcp_season['dow_min_price'])
    df_rcp_season['rate_factor'] = df_rcp_season['dow_min_price']/df_rcp_season['min_rate']

    # df_rcp_season['min_rate'] = np.where(df_rcp_season['min_rate'] < df_rcp_season['dow_min_price'],
    #                                      df_rcp_season['dow_min_price'], df_rcp_season['min_rate'])

    df_rcp_season['ota_max']= (df_rcp_season['cm_capacity'] + df_rcp_season['rm_sold'])

    # df_rcp_season['cma_sqrt'] = np.sqrt(df_rcp_season['cm_capacity'])
    df_rcp_season['cma_sqrt'] =np.where(df_rcp_season['cm_capacity']<=0,1, np.sqrt(df_rcp_season['cm_capacity']))

    # df_rcp_season['cap_sqrt'] = np.sqrt(df_rcp_season['max_cap'])
    df_rcp_season['cap_sqrt'] = np.where(df_rcp_season['max_cap']<=0, 1, np.sqrt(df_rcp_season['max_cap']))

    df_rcp_season['cap_sqrt1'] = 1.5*df_rcp_season['cap_sqrt']
    df_rcp_season['cap_sqrt2'] = 2.5*df_rcp_season['cap_sqrt']
    
    df_rcp_season['ota_cap'] = np.where((df_rcp_season['ota_max']==0),df_rcp_season['max_cap'],df_rcp_season['ota_max'])
    
    # df_rcp_season['ota_sqrt'] = np.sqrt(df_rcp_season['ota_cap'])
    df_rcp_season['ota_sqrt'] = np.where(df_rcp_season['ota_cap']<=0, 1, np.sqrt(df_rcp_season['ota_cap']))

    df_rcp_season['ota_sqrt1'] = 1.5*df_rcp_season['ota_sqrt']
    df_rcp_season['ota_sqrt2'] = 2.5*df_rcp_season['ota_sqrt']
    
    df_rcp_season['sqrt0'] = df_rcp_season[['ota_sqrt','rm_sold']].max(axis=1)
    df_rcp_season['sqrt1'] = df_rcp_season[['ota_sqrt1','rm_sold']].max(axis=1)
    df_rcp_season['sqrt2'] = df_rcp_season[['ota_sqrt2','rm_sold']].max(axis=1)
        
    df_rcp_season['numarator'] = np.where((
            df_rcp_season['ota_max'] <= df_rcp_season['cap_sqrt']), df_rcp_season['sqrt2'],
        np.where((df_rcp_season['ota_max']<=df_rcp_season['cap_sqrt1']), df_rcp_season['sqrt1'],
                 np.where((df_rcp_season['ota_max'] <= df_rcp_season['cap_sqrt2']), df_rcp_season['sqrt0'],
                          df_rcp_season['rm_sold'])))

    df_rcp_season['denominator'] = df_rcp_season['max_cap'] - (df_rcp_season['max_cap'] - np.where((
            df_rcp_season['ota_max'] == 0), df_rcp_season['cap_sqrt'], df_rcp_season['ota_max']))
    
    # df_rcp_season['ratio_top'] = np.where((df_rcp_season['ota_max'] == 0), 1,
    #                                       (np.where((df_rcp_season['rm_sold'] == 0), df_rcp_season['cma_sqrt'],
    #                                                 df_rcp_season['rm_sold'])/df_rcp_season['ota_max']))

    # SAM 16FEB2022: Changes the logic slightly to accommodate the situation
    # where the inventory is very less and no on books or rooms sold as well

    df_rcp_season['ratio_top'] = np.where((df_rcp_season['ota_max'] == 0), 1,
                                          np.where((df_rcp_season['rm_sold'] > 0),
                                                   (df_rcp_season['rm_sold'] / df_rcp_season['ota_max']),
                                                   np.where((df_rcp_season['cm_capacity'] < df_rcp_season['max_cap']),
                                                            1, 0)))

    # df_rcp_season['ratio_top'] = np.where((df_rcp_season['ota_max'] == 0), 1,
    #                                       (np.where((df_rcp_season['rm_sold'] == 0), 0,
    #                                                 df_rcp_season['rm_sold']) / df_rcp_season['ota_max']))

    df_rcp_season['rcp_raw'] = (((df_rcp_season['rate_dif']*df_rcp_season['numarator'])/df_rcp_season['denominator']) *
                            df_rcp_season['ratio_top']) + df_rcp_season['min_rate']

    df_rcp_season['rcp'] = df_rcp_season['rcp_raw']
    # df_rcp_season['rcp'] = df_rcp_season['rcp_raw'] * df_rcp_season['rate_factor']
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    delcols = ['WkNum', 'min_cap', 'max_cap', 'seasonid', 'slope', 'intercept', 'max_rate', 'min_price',
               'rate_dif', 'ota_max', 'cma_sqrt', 'cap_sqrt', 'cap_sqrt1', 'cap_sqrt2', 'ota_cap', 'ota_sqrt',
               'ota_sqrt1', 'ota_sqrt2', 'sqrt0', 'sqrt1', 'sqrt2', 'numarator', 'denominator', 'ratio_top', 'rcp_raw']
    df_rcp_season = df_rcp_season.drop(delcols, axis=1)

    return df_rcp_season
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~


