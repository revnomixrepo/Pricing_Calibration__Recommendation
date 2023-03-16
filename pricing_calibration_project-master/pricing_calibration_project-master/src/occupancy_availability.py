"""
@Chakradhar T.

This module to calculate availability and sold data


"""
import logging
import sys
import pandas as pd
import numpy  as np
import getData
import datetime
logger = logging.getLogger(f'Recomservicelog.{__name__}')
n = 10

def non_hnf(hid, db_confg, start_date, end_date):
    logger.info("-- Getting availability data from non_hnf method")
    myquery = ("SELECT distinct inv_date as occupancydate, sum(availability) as cm_capacity FROM "
               "staah_inventory where client_id = :prid and inv_date between '" + start_date + "' and '"
               + end_date + "' group by inv_date")

    try:
        logger.info("--- Get number of rooms available to sell ---")
        df_avl = getData.getData(myquery=myquery, pid=hid, db_confg=db_confg)
        logger.debug(df_avl.head(n))
    except:
        logger.error("--- No Data Available. Check the data availability ---")

    # ======================================================

    myquery = ("SELECT checkin_date as StrDt, checkout_date as EndDt, no_of_rooms as rooms FROM bookings " +
               "WHERE client_id =:prid and checkin_date <= '" + end_date + "' and checkout_date >'" + start_date +
               "' and cm_status in  ('BOOKED', 'C', 'Confirmed', 'New', 'Reserved', 'M', 'Modified', 'Modify')")

    try:
        logger.info("--- Get number of rooms sold ---")
        df_ob = getData.getData(myquery=myquery, pid=hid, db_confg=db_confg)
    except:
        logger.error("--- No Data Available. Check the data availability ---")

    if df_ob.__len__() <= 0:
        df_avl = pd.DataFrame(df_avl)
        df_avl['rm_sold'] = np.nan
        df_avl['occupancydate'] = pd.to_datetime(df_avl['occupancydate'], format="%Y-%m-%d")
    else:
        df_date = pd.concat([pd.DataFrame({'occupancydate': pd.date_range(row.StrDt, row.EndDt),
                                           'StrDt': row.StrDt, 'EndDt': row.EndDt, 'rooms': row.rooms},
                                          columns=['StrDt', 'EndDt', 'occupancydate', 'rooms'])
                             for i, row in df_ob.iterrows()], ignore_index=True)

        df_date['StrDt'] = pd.to_datetime(df_date['StrDt'], format="%Y-%m-%d")
        df_date['EndDt'] = pd.to_datetime(df_date['EndDt'], format="%Y-%m-%d")

        df_date['dtDif'] = (df_date['EndDt']-df_date['occupancydate']).apply(lambda x: x/np.timedelta64(1, 'D'))

        df_date2 = df_date.query('(dtDif > 0)')

        delcols = ['StrDt', 'EndDt', 'dtDif']
        df_date3 = df_date2.drop(delcols, axis=1)

        df_sold = pd.DataFrame(df_date3.groupby('occupancydate')["rooms"].sum().reset_index(name="rm_sold"))

        df_avl['occupancydate'] = pd.to_datetime(df_avl['occupancydate'], format="%Y-%m-%d")
        logger.info('availability data with room sold ')
        df_avl = df_avl.merge(df_sold, on='occupancydate', how='left')

    logger.debug(df_avl.head(n))
    return df_avl


def hnf(hid, db_confg, start_date, end_date):
    logger.info("-- Getting availability data from hnf method ")
    myquery = ("SELECT distinct date as occupancydate, "
               "(case when availability < 0 then 0 else availability end) as cm_capacity, rooms_sold as rm_sold "
               "FROM history_and_forecast  where client_id = :prid and date between '" + start_date
               + "' and '" + end_date + "'  and updated_date like '" + start_date + "%'")

    try:
        logger.info("--- Get number of rooms available to sell ---")
        df_avl = getData.getData(myquery=myquery, pid=hid, db_confg=db_confg)
        logger.debug(df_avl.head(n))
    except:
        logger.error("--- No Data Available. Check the data availability ---")

    df_avl['occupancydate'] = pd.to_datetime(df_avl['occupancydate'], format="%Y-%m-%d")

    logger.debug(df_avl.head(n))
    return df_avl


def hybrid_hnf(hid, db_confg, start_date,end_date, htl_cap):
    logger.info("-- Getting availability data from hybrid_hnf method ")

    myquery = ("SELECT distinct inv_date as occupancydate, sum(availability) as cm_capacity FROM staah_inventory " +
               "where client_id = :prid and inv_date between '" + start_date + "' and '" + end_date +
               "' group by inv_date")

    try:
        logger.info("--- Get number of rooms available to sell ---")
        df_avl = getData.getData(myquery=myquery, pid=hid, db_confg=db_confg)
        logger.debug(df_avl.head(n))
    except:
        logger.error("--- No Data Available. Check the data availability ---")

    df_avl['rm_sold'] = htl_cap - df_avl['cm_capacity']

    df_avl['occupancydate'] = pd.to_datetime(df_avl['occupancydate'], format="%Y-%m-%d")

    logger.debug(df_avl.head(n))
    return df_avl


def get_occ_avil(htl_cap, hid, db_confg, start_date,end_date, hnf_cond=0):
    if hnf_cond == 1:
        df_avl = hnf(hid, db_confg, start_date, end_date)
    elif hnf_cond == 2:
        df_avl = hybrid_hnf(hid, db_confg, start_date, end_date, htl_cap)
    else:
        df_avl = non_hnf(hid, db_confg, start_date, end_date)
    return df_avl
