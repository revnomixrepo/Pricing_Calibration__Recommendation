# -*- coding: utf-8 -*-
"""
Created on Fri Dec 23 12:40:30 2016

@author: Sameer.Kulkarni
"""

import sys
import pandas as pd
import numpy as np
import logging
from datetime import datetime
import getData
import rcpfunction
import traceback

logger = logging.getLogger(f'Calbrservicelog.{__name__}')


def print_exception():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    # logging.info(traceback.print_tb(exc_traceback, limit=2, file=sys.stdout))
    logger.info(traceback.format_exception_only(exc_type, exc_value))
    logger.info(traceback.format_exception(exc_type, exc_value, tb=exc_traceback, limit=2, ))


def rcpalgo(pid, hCap, db_config):
    # print("--- Start ---")

    import getData
    import rcpfunction
    import connectdb

    myquery = "SELECT checkin_date as CheckInDate, no_of_rooms as Nights, los, total_amount as RoomRevenue " \
                  "FROM bookings WHERE client_id =:prid"

    try:
        logger.info("--- Fetching the Booking data ---")
        df = getData.getData(myquery=myquery, pid=pid, db_confg=db_config)
        logger.debug("--- Booking data ---")
        logger.debug(df)
    except:
        logger.error("--- No Booking data available. Check the database ---")

    myquery = "SELECT param_value FROM property_parameters "\
              "where client_id =:prid and param_name = 'Use_Min_Max_for_Calibration'"

    try:
        logger.info("--- Fetching the Use_Min_Max_for_Calibration ---")
        df_pp = getData.getData(myquery=myquery, pid=pid, db_confg=db_config)
        ummc = int(df_pp['param_value'].values)
        logger.debug("--- Use_Min_Max_for_Calibration ---")
        logger.debug(df_pp)
    except:
        ummc = 0
        logger.error("--- No Use_Min_Max_for_Calibration ---")
        pass

    qcson = "select hotel_id, start_week, end_week, max_capacity FROM seasonality_definitions WHERE client_id =:prid"

    try:
        logger.info("--- Fetching the Seasonal Definition ---")
        df_cson_db = getData.getData(myquery=qcson, pid=pid, db_confg=db_config)
        logger.debug(df_cson_db)
    except:
        logger.error("--- Seasons are not defined ---")

    df_cson_db['jCon'] = np.where((df_cson_db['start_week'] > df_cson_db['end_week']), "or", "and")

    df_cson_db['array_cson'] = df_cson_db.apply(lambda x: "WkNum >= %s %s WkNum <= %s" % (x['start_week'], x['jCon'],
                                                                                          x['end_week']), axis=1)

    df_cson = pd.DataFrame(df_cson_db, columns=['array_cson'])

    htlid = df_cson_db.loc[0].values[0]

    qrdow = "select day as dow, type as daytype from dow_definitions WHERE client_id =:prid and season_no=0"

    try:
        logger.info("--- Fetching the Day of Week Definition ---")
        df_dow = getData.getData(myquery=qrdow, pid=pid, db_confg=db_config)
        logger.debug(df_dow)
    except:
        logger.error("--- Days of Week are not defined ---")

    df_dow['dow'] = df_dow.dow.str.capitalize()

    df_dow['daytype'] = np.where((df_dow['daytype'] == "weekday"), "WD", "WE")

    logger.info("--Changed the day types--")

    dfwd = df_dow.query('daytype=="WD"')
    dfwe = df_dow.query('daytype=="WE"')

    logger.info("--Created two data frames by WD and WE--")

    liwd = dfwd['dow'].values
    liwe = dfwe['dow'].values

    logger.info("--Created two list by WD and WE--")

    aDOW = ['All', 'WD', 'WE', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    logger.info("--Columns List--")

    aSeas = df_cson['array_cson'].values
    logger.debug(aSeas)

    mCap = df_cson_db['max_capacity'].values
    logger.debug(mCap)

    logger.info("--- Calculate Week Number, Day of Week and ADR ---")

    logger.debug(df)

    df.columns = ['CheckInDate', 'Nights', 'LOS', 'RoomRevenue']

    logger.info("--- %s ---", df.dtypes)

    df['CheckInDate'] = pd.to_datetime(df['CheckInDate'])
    df['dow'] = df['CheckInDate'].dt.day_name()
    df['dtyr'] = df['CheckInDate'].dt.year
    df['dt1'] = pd.to_datetime(dict(year=df.dtyr, month=1, day=8))
    df['dt2'] = pd.to_datetime(dict(year=df.dtyr, month=1, day=6))
    df['dtwk'] = df['dt2'].dt.dayofweek + 2
    df['dt3'] = df['dt1'] - pd.TimedeltaIndex(df['dtwk'], unit='D')
    df['dtdf'] = (df['dt3'] - df['CheckInDate']).apply(lambda x: x / np.timedelta64(1, 'D'))
    df.loc[(df['dtdf'] > 0.0), 'WkNum'] = 52
    df.loc[(df['dtdf'] <= 0.0), 'WkNum'] = df['CheckInDate'].apply(lambda x: x.strftime('%W'))
    df['WkNum'] = pd.to_numeric(df['WkNum'])
    df['WkNum'] = np.where((df['WkNum'] == 53), 0, df['WkNum'])
    df['ADR'] = df['RoomRevenue'] / (df['Nights'] * df['LOS'])

    logger.debug(df)

    delcols = ['dtyr', 'dt1', 'dt2', 'dt3', 'dtwk', 'dtdf']
    df = df.drop(delcols, axis=1)
    logger.info("--- Removing following columns %s ---", delcols)

    # for k in range(len(delcols)):
    #    del df[delcols[k]]

    df1 = df.query('ADR > 0' or 'ADR != NaN')

    logger.info("--- Removing the rows with ADR 0 or NaN ---")

    # Start the loop here
    # ----------------------

    dfSlope = pd.DataFrame()
    dfIntercept = pd.DataFrame()
    dfMinRate = pd.DataFrame()
    logger.info("--- Created two empty dataframes ---")

    for i in range(len(aSeas)):
        logger.debug(" season - %s", i)
        season = aSeas[i]  # assigning the seasons
        hCap = int(hCap)  # assigning the seasons
        logger.debug(season)
        mSlope = []  # creating a blank array to hold the slope values
        cIntercept = []  # creating a blank array to hold the intercept values
        # MIN RATE CALCULATION
        mRates = []
        # print(season)

        # SAM 27FEB22 get the min and max overrides here if the "Use_Min_Max_for_Calibration" is set to "TURE"
        if ummc == 1:
            myquery = "SELECT price_override as min_rate, max_price as max_rate " \
                      "FROM seasonality_definitions where client_id = :prid and number = {}".format(i)
            df_mmr = getData.getData(myquery=myquery, pid=pid, db_confg=db_config)

        for j in range(len(aDOW)):
            DOW = aDOW[j]  # assigning day of weeks
            cson1 = df1.query(season)  # Fetching data by Season
            logger.info("%s - %s - %s", season, i, DOW)
            logger.info("--- Calculate Bounds for Outlier Removal ---")

            lbnd, ubnd = rcpfunction.LUBound(cson1['ADR'])

            #        print(lbnd, ubnd)

            logger.info("--- Calculated the Lower and Upper Bounds ---")

            adrDF1 = cson1[(cson1['ADR'] >= lbnd) & (cson1['ADR'] <= ubnd)]
            # disc = adrDF1.describe()

            logger.info("--- Calculate Percentiles and Mu Sigma ---")

            mu, sigma, p95, p05 = rcpfunction.perTile(adrDF1, DOW, liwd, liwe)

            if ummc == 1:
                p05 = int(df_mmr['min_rate'].values)
                p95 = int(df_mmr['max_rate'].values)

# SAM 03JAN22:
# Add a new line here to restrict the spread based of the Min and Max rate details submitted by the client
# We will refer to newly added column either "Max_Price" or "Max_Price_Override" along with existing columns
# i.e. "Min_Price" and "Min_Price_Override"
# Amend the below line
# adrDF2 = cson1[(cson1['ADR'] >= p05) & (cson1['ADR'] <= p95)]
# to reflect the Min and Max rate requirements of the hotel

            logger.info("--- Removing outliers using percentile values ---")
            adrDF2 = cson1[(cson1['ADR'] >= p05) & (cson1['ADR'] <= p95)]

            logger.info("--- Calculating z Values ---")
            zRate, zSale, rowCnt = rcpfunction.zValues(adrDF2, DOW, mu, sigma, liwd, liwe)

            logger.info("Setting the minimum number of observations."
                        "If there are less than 30 then not processing the data ahead")
            if rowCnt >= 30:
                logger.info("There is enough data and proceeding with calibration")
                vrDF = pd.DataFrame({'rates': zRate, 'volume': zSale})

                logger.info("Sorting the ADR data or z scores in descending order")
                vrDF1 = vrDF.sort_values(by='rates', axis=0, ascending=0)

                logger.info("Calculating Cumulative Solds")
                vrDF1['cum_sum'] = vrDF1.volume.cumsum()

                logger.info("Calculating y Value")
                vrDF1['y_val'] = round(vrDF1.cum_sum / vrDF1.volume.sum() * hCap, 0)

                logger.info("Resorting in ascending order")
                vrDF1 = vrDF1.sort_values(by='rates', axis=0, ascending=1)

                logger.debug(vrDF1)
                logger.info("Remove the rows with Zero volume or Sold")
                mcDF = vrDF1.query('volume>0')

                logger.info("Calculating the Slope and Intercept")
                # mval, cval = rcpfunction.mcVal(mcDF.rates,mcDF.y_val)
                mval, cval, min_rate = rcpfunction.mcVal(mcDF.rates, mcDF.y_val)

                logger.info("Calculated the Slope (m value)")
                logger.debug(mval)
                logger.info("Calculated the Intercept (c value)")
                logger.debug(cval)
            else:
                # mval, cval = (0,0)
                mval, cval, min_rate = (0, 0, 0)
                logger.info("--- Insufficient Data ---")

            mSlope.append(mval)
            cIntercept.append(cval)
            mRates.append(min_rate)

            logger.debug(mSlope)
            logger.debug(cIntercept)

        dfms = pd.DataFrame(mSlope, columns=['mValue'])
        dfci = pd.DataFrame(cIntercept, columns=['cValue'])
        # del(mSlope,cIntercept)
        dfmr = pd.DataFrame(mRates, columns=['mRate'])
        del (mSlope, cIntercept, mRates)

        dfSlope = dfSlope.append(dfms.transpose(), ignore_index=True)
        dfIntercept = dfIntercept.append(dfci.transpose(), ignore_index=True)
        dfMinRate = dfMinRate.append(dfmr.transpose(), ignore_index=True)

        logger.debug(dfSlope)
        del (dfms, dfci, dfmr)
        # del(dfms,dfci)

    logger.info("--- Start the calculation of the Slope the Intercept ---")

    dfSlope['PID'] = pid
    dfIntercept['PID'] = pid
    dfMinRate['PID'] = pid

    dfSlope['Season'] = dfSlope.index
    dfIntercept['Season'] = dfIntercept.index
    dfMinRate['Season'] = dfMinRate.index

    logger.debug(dfIntercept)
    dfSlope.columns = ['All', 'WD', 'WE', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday',
                       'PID', 'Season']

    dfIntercept.columns = ['All', 'WD', 'WE', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday',
                           'Sunday', 'PID', 'Season']

    dfMinRate.columns = ['All', 'WD', 'WE', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday',
                         'Sunday', 'PID', 'Season']

    cols = ['PID', 'Season', 'All', 'WD', 'WE', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday',
            'Sunday']
    drop_col = ['WD', 'WE', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

    dfSlope = dfSlope.reindex(columns=cols)
    dfIntercept = dfIntercept.reindex(columns=cols)
    dfMinRate = dfMinRate.reindex(columns=cols)
    dfMinRate.drop(columns=drop_col, inplace=True)
    update_date = datetime.today()
    dfMinRate['update_date'] = str(update_date)
    dfMinRate['All'] = np.where(dfMinRate['All'] <= 0, dfMinRate.loc[0, 'All'], dfMinRate['All'])
    logger.debug(dfSlope)
    dfSlope = setDF(dfSlope, liwd)

    logger.info("--- Calculated the Slope ---")
    logger.debug(dfSlope)

    logger.debug(dfIntercept)
    dfIntercept = setDF(dfIntercept, liwd)

    logger.info("--- Calculated the Intercept ---")
    logger.debug(dfIntercept)

    # dfSlope.columns=['propertyid', 'seasonid', 'allval', 'weekdays', 'weekends', 'dow1', 'dow2', 'dow3', 'dow4', 'dow5', 'dow6', 'dow7']

    dfSlope.columns = ['client_id', 'season_number', 'all_days', 'week_days', 'week_ends', 'dow1', 'dow2', 'dow3',
                       'dow4', 'dow5', 'dow6', 'dow7']
    dfIntercept.columns = ['client_id', 'season_number', 'all_days', 'week_days', 'week_ends', 'dow1', 'dow2', 'dow3',
                           'dow4', 'dow5', 'dow6', 'dow7']

    dfSlope['hotel_id'] = htlid
    logger.debug(dfSlope)

    dfIntercept['hotel_id'] = htlid
    logger.debug(dfIntercept)

    cols = ['hotel_id', 'client_id', 'season_number', 'dow1', 'dow2', 'dow3', 'dow4', 'dow5', 'dow6', 'dow7',
            'week_days', 'week_ends', 'all_days']

    dfSlope = dfSlope.reindex(columns=cols)
    dfIntercept = dfIntercept.reindex(columns=cols)
    logger.debug(dfIntercept)

    cnx = connectdb.conectdb(db_config)

    try:
        logger.info("--- Deleting the old SLOPE calibration values ---")
        cnx.execute('Delete from slopes where client_id =%s', pid)
    except:
        logger.error("--- Unable to delete the SLOPE calibration values ---")

    try:
        logger.info("--- Deleting the old INTERCEPT calibration values ---")
        cnx.execute('Delete from intercepts where client_id =%s', pid)
    except:
        logger.error("--- Unable to delete the INTERCEPT calibration values ---")

    try:
        logger.info("--- Inserting the new SLOPE calibration values ---")
        dfSlope.to_sql(con=cnx, name='slopes', if_exists='append', index=False, index_label='id')
    except:
        logger.error("--- Unable to add the SLOPE calibration values ---")

    try:
        logger.info("--- Inserting the new INTERCEPT calibration values ---")
        dfIntercept.to_sql(con=cnx, name='intercepts', if_exists='append', index=False, index_label='id')
    except:
        logger.error("--- Unable to add the INTERCEPT calibration values ---")

    dfMaxcap = maxcapacity(pid, hCap, db_config=db_config)
    dfmcmr = dfMaxcap.merge(dfMinRate, on=['PID', 'Season'], how='left')

    # try:
    #     for row in dfMinRate.itertuples():
    #         logger.info("update seasonality_definitions " \
    #                      "set min_price = %s ,regdate = %s "  \
    #                      "where client_id = %s " \
    #                      "and number = %s", \
    #                      float(row.All), row.update_date, int(row.PID), int(row.Season))
    #
    #         cnx.execute("update seasonality_definitions " \
    #                     "set min_price = %s ,regdate = %s "  \
    #                     "where client_id = %s " \
    #                     "and number = %s ", \
    #                     [float(row.All), row.update_date, int(row.PID), int(row.Season)])
    # except:
    #     logger.error("--- Unable to UPDATED the MIN RATE calibration values ---")

    try:
        for row in dfmcmr.itertuples():
            logger.info("update seasonality_definitions set max_capacity = {}, min_price= {},"
                        " regdate = '{}' where client_id = {} and number = {}".format(float(row.max_capacity),
                                                                                      float(row.All), row.update_date,
                                                                                      int(row.PID), int(row.Season)))

            cnx.execute("update seasonality_definitions set max_capacity = {}, min_price= {},"
                        " regdate = '{}' where client_id = {} and number = {}".format(float(row.max_capacity),
                                                                                      float(row.All), row.update_date,
                                                                                      int(row.PID), int(row.Season)))
    except:
        logger.error("--- Unable to UPDATED the Max Capacity and  MIN RATE calibration values ---")

    cnx.close()

    logger.debug(liwd)
    logger.debug(liwe)

    logger.debug(aSeas)
    logger.info("--- DONE ---")
    logger.info("==============================================================")


def setDF(df_input, wkdList):
    c0al = df_input.iloc[0, 2]
    c0wd = np.where(df_input.iloc[0, 3] == 0, c0al, df_input.iloc[0, 3])
    c0we = np.where(df_input.iloc[0, 4] == 0, c0al, df_input.iloc[0, 4])

    logger.debug(c0al)
    logger.debug(c0wd)
    logger.debug(c0we)

    df_input['WD'] = np.where(df_input['WD'] == 0.0,
                              np.where(df_input['All'] == 0.0, c0wd, df_input['All']), df_input['WD'])
    df_input['WE'] = np.where(df_input['WE'] == 0.0,
                              np.where(df_input['All'] == 0.0, c0we, df_input['All']), df_input['WE'])
    df_input['All'] = np.where(df_input['All'] == 0.0, c0al, df_input['All'])

    for row in df_input.itertuples():
        if row.Monday == 0:
            isthere = 'Monday' in wkdList
            if isthere:
                rdow = row.WD
            else:
                rdow = row.WE
            df_input.at[row.Index, 'Monday'] = rdow
        if row.Tuesday == 0:
            isthere = 'Tuesday' in wkdList
            if isthere:
                rdow = row.WD
            else:
                rdow = row.WE
            df_input.at[row.Index, 'Tuesday'] = rdow
        if row.Wednesday == 0:
            isthere = 'Wednesday' in wkdList
            if isthere:
                rdow = row.WD
            else:
                rdow = row.WE
            df_input.at[row.Index, 'Wednesday'] = rdow
        if row.Thursday == 0:
            isthere = 'Thursday' in wkdList
            if isthere:
                rdow = row.WD
            else:
                rdow = row.WE
            df_input.at[row.Index, 'Thursday'] = rdow
        if row.Friday == 0:
            isthere = 'Friday' in wkdList
            if isthere:
                rdow = row.WD
            else:
                rdow = row.WE
            df_input.at[row.Index, 'Friday'] = rdow
        if row.Saturday == 0:
            isthere = 'Saturday' in wkdList
            if isthere:
                rdow = row.WD
            else:
                rdow = row.WE
            df_input.at[row.Index, 'Saturday'] = rdow
        if row.Sunday == 0:
            isthere = 'Sunday' in wkdList
            if isthere:
                rdow = row.WD
            else:
                rdow = row.WE
            df_input.at[row.Index, 'Sunday'] = rdow

    return df_input


def maxcapacity(pid, hCap, db_config):
    qrmcap = ("select t.occupancy_date as occupancydate, sum(t.no_of_rooms) as no_of_rooms"
              " from(SELECT bp.occupancy_date, bp.no_of_rooms, bk.cm_status, bk.status"
              " from booking_pace_occupancy_by_date as bp inner join bookings as bk on (bk.id = bp.booking_id)"
              " where bp.client_id = {} and cm_status in {}) as t"
              " group by t.occupancy_date".format(pid, ('BOOKED', 'C', 'Confirmed', 'New', 'Reserved', 'M', 'Modified', 'Modify')))
    try:
        logger.info("--- Fetching hotel capacity data  ---")
        df_cap = getData.getData(myquery=qrmcap, pid=pid, db_confg=db_config)
        logger.debug(df_cap)
    except:
        logger.error("--- Days of Week are not defined ---")

    qcson = "select hotel_id, start_week, end_week, max_capacity FROM seasonality_definitions WHERE client_id =:prid"

    try:
        logger.info("--- Fetching the Seasonal Definition ---")
        df_cson_db = getData.getData(myquery=qcson, pid=pid, db_confg=db_config)
        logger.debug(df_cson_db)
    except:
        logger.error("--- Seasons are not defined ---")

    df_cson_db['jCon'] = np.where((df_cson_db['start_week'] > df_cson_db['end_week']), "or", "and")

    df_cson_db['array_cson'] = df_cson_db.apply(
        lambda x: "WkNum >= %s %s WkNum <= %s" % (x['start_week'], x['jCon'], x['end_week']), axis=1)

    df_cson = pd.DataFrame(df_cson_db, columns=['array_cson'])
    qrdow = "select day as dow, type as daytype from dow_definitions WHERE client_id =:prid and season_no=0"

    try:
        logger.info("--- Fetching the Day of Week Definition ---")
        df_dow = getData.getData(myquery=qrdow, pid=pid, db_confg=db_config)
        logger.debug(df_dow)
    except:
        logger.error("--- Days of Week are not defined ---")

    aSeas = df_cson['array_cson'].values
    logger.debug(aSeas)

    logger.info("--- Identifying the Seasons for the dates in question ---")
    df_mxcap = pd.DataFrame(df_cap, columns=['occupancydate', 'no_of_rooms'])
    df_mxcap['occupancydate'] = pd.to_datetime(df_mxcap['occupancydate'])
    df_mxcap['dow'] = df_mxcap['occupancydate'].dt.dayofweek + 1
    df_mxcap['dtyr'] = df_mxcap['occupancydate'].dt.year
    df_mxcap['dt1'] = pd.to_datetime(dict(year=df_mxcap.dtyr, month=1, day=8))
    df_mxcap['dt2'] = pd.to_datetime(dict(year=df_mxcap.dtyr, month=1, day=6))
    df_mxcap['dtwk'] = df_mxcap['dt2'].dt.dayofweek + 2
    df_mxcap['dt3'] = df_mxcap['dt1'] - pd.TimedeltaIndex(df_mxcap['dtwk'], unit='D')
    df_mxcap['dtdf'] = (df_mxcap['dt3'] - df_mxcap['occupancydate']).apply(lambda x: x / np.timedelta64(1, 'D'))
    df_mxcap.loc[(df_mxcap['dtdf'] <= 0.0), 'WkNum'] = df_mxcap['occupancydate'].apply(lambda x: x.strftime('%W'))
    df_mxcap.loc[(df_mxcap['dtdf'] > 0.0), 'WkNum'] = 52
    df_mxcap['WkNum'] = pd.to_numeric(df_mxcap['WkNum'])
    df_mxcap['WkNum'] = np.where((df_mxcap['WkNum'] == 53), 0, df_mxcap['WkNum'])
    # df_mxcap.to_csv(r'C:\Users\RDM2\Desktop\Pricing test/df_mxcap.csv')
    logger.debug("Printing the seasonality identified data")
    logger.debug(df_mxcap)
    delcols = ['dtyr', 'dt1', 'dt2', 'dt3', 'dtwk', 'dtdf']
    df_mxcap = df_mxcap.drop(delcols, axis=1)
    logger.info("--- Removing following columns %s ---", delcols)

    dfMaxcap = pd.DataFrame()
    logger.info("--- Created two empty dataframes ---")
    mcap = []
    for i in range(len(aSeas)):
        logger.debug(" season - %s", i)
        season = aSeas[i]  # assigning the seasons
        logger.debug(season)
        # MAX RATE CALCULATION
        cson1 = df_mxcap.query(season)  # Fetching data by Season
        logger.info("%s - %s ", season, i)
        logger.info("--- Calculate Percentiles---")

        p95 = rcpfunction.mcap_percent(cson1, hCap)

        mcap.append(p95)
    dfmcap = pd.DataFrame(mcap, columns=['max_capacity'])
    del (mcap)

    dfMaxcap = dfmcap
    del (dfmcap)
    dfMaxcap['PID'] = pid
    dfMaxcap['Season'] = dfMaxcap.index
    cols = ['PID', 'Season', 'max_capacity']
    dfMaxcap = dfMaxcap.reindex(columns=cols)

    logger.debug(aSeas)
    logger.debug(dfMaxcap)
    logger.info("--- max capacity DONE ---")
    logger.info("==============================================================")
    return dfMaxcap


if __name__ == '__main__':
    # pid, hCap, rhost, ruser, rpwd, rdb, dbtype
    if sys.argv[7] == 'PGS':
        pid = int(sys.argv[1])
    else:
        pid = sys.argv[1]

    hCap = int(sys.argv[2])
    rhost = sys.argv[3]
    ruser = sys.argv[4]
    rpwd = sys.argv[5]
    rdb = sys.argv[6]
    dbtype = sys.argv[7]
    config = {'rhost': rhost, 'ruser': ruser, 'rpwd': rpwd, 'rdb': rdb, 'dbtype': dbtype}

    rcpalgo(pid, hCap, config)
