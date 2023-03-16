# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 12:29:54 2017

@author: sameer kulkarni
"""

import logging
import sys
import pandas as pd
import numpy as np

import getrcp
import getData
import mapdata
import datetime
import connectdb
import occupancy_availability as occ_av
import pacebyalgo
import sys, traceback


logger = logging.getLogger(f'Recomservicelog.{__name__}')
n = 10


def print_exception():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    # logger.info(traceback.print_tb(exc_traceback, limit=2, file=sys.stdout))
    logger.error(traceback.format_exception_only(exc_type, exc_value))
    logger.error(traceback.format_exception(exc_type, exc_value, tb=exc_traceback, limit=2,))


def getRecommendations(db_confg, pid, start_date):
    logger.setLevel(logging.INFO)
    logger.info("--- Start Generating Recommendations---")

    hid = int(pid)

    try:
        pp = pacebyalgo.PaceByAlgo(pid, db_confg)
        logger.info("--- Fetching the hotel parameter list From parameter ---")
        prmt_dict = pp.getparameters()
    except Exception as E:
        prmt_dict = pd.DataFrame()
        logger.error("---Check the data availability in parameter table----, Error:{}".format(E))

    if prmt_dict['start_date'] != 'TODAY':
        start_date = prmt_dict['start_date']
    else:
        start_date = start_date

    date_range = int(prmt_dict['date_range'])
    calc_ari = prmt_dict['calc_ari']
    calc_mpi = prmt_dict['calc_mpi']
    calc_pqm = prmt_dict['calc_pqm']
    cmp_rt = int(prmt_dict['rates_history'])
    # cmp_rt = 200
    Run_Recom_All_Diff = prmt_dict['Run_Recom_All_Diff']
    logger.info('-- Setting psymult and psysub value--')

    if prmt_dict['Apply_Psycological_Factor'] == 'YES':
        psymult = 10 if prmt_dict.get('Psychological_Factor_Mulitipler') is None \
            else prmt_dict['Psychological_Factor_Mulitipler']
        psysub = 1 if prmt_dict.get(
            'Psychological_Factor_subtractor') is None else prmt_dict['Psychological_Factor_subtractor']
    else:
        psymult = 1
        psysub = 0

    logger.info("--- psymulti = {} and psysub = {} ---".format(psymult, psysub))
    logger.info("--- Calculating the End date ---")
    edt = datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=date_range)

    end_date = edt.strftime("%Y-%m-%d")
    logger.info("--- Recommendation Period is between %s and %s ---", start_date, end_date)

    cdt = datetime.datetime.strptime(start_date, "%Y-%m-%d") - datetime.timedelta(days=cmp_rt)
    clt_date = cdt.strftime("%Y-%m-%d")

    myquery = ("SELECT h.rm_code as propertydetailsid, h.id, c.capacity, c.hnf FROM hotels h "
               "INNER JOIN clients c ON (h.id=c.hotel_id) WHERE c.id=:prid")
    try:
        logger.info("--- Fetching the hotel id from the List ---")
        df_rsh = getData.getData(myquery=myquery, pid=hid, db_confg=db_confg)
    except:
        logger.error("--- No Data Available. Check the data availability ---")
        return "-1", "-1"

    htlid = df_rsh.iloc[0, 0]
    hotel_id = df_rsh.iloc[0, 1]
    htl_cap = df_rsh.iloc[0, 2]
    hnf_type = df_rsh.iloc[0, 3]
    hnf_dict = {'NONE': 0, 'NORMAL': 1, 'HYBRID': 2}

    myquery = "SELECT h.rm_code propertydetailsid FROM hotels h INNER JOIN clients c ON (h.id=c.hotel_id) " \
              "WHERE c.id=:prid UNION SELECT h.rm_code as propertydetailsid FROM hotels h INNER JOIN" \
              " clients_competitors cc ON (h.id=cc.hotel_id) WHERE cc.client_id=:prid"

    try:
        logger.info("--- Fetching the Competition List ---")
        df_comps = getData.getData(myquery=myquery, pid=hid, db_confg=db_confg)
    except:
        logger.error("--- No Data Available. Check the data availability ---")
        return "-1", "-1"

    logger.info("--- Joining the Competition List with Hotel ---")
    df_rsh = pd.DataFrame(df_comps)
    logger.info("--- Converting the Dataframe into a string ---")

    x = df_rsh.to_string(header=False,
                         index=False,
                         index_names=False).split(',')
    valStr = [','.join(ele.split()) for ele in x]
    comp_ids = valStr[0]

    htl_count = len(df_rsh.index)

    logger.info("--- List of all hotels is ready ---")
    # logger.debug(comp_ids)

    # Create the Dataframe that holds the capacity, remaining capacity and
    # CM Availability For each date into future. Generally for next 90days
    hnf_cond = hnf_dict[hnf_type]
    logger.info("---hnf_cond = {}---".format(hnf_cond))
    df_avl = occ_av.get_occ_avil(htl_cap, hid, db_confg, start_date, end_date, hnf_cond)
    # ----------------------------------------------------------------

    # Calling the RCP Algo to generate the recommendations

    logger.info("--- Generate the Recommendations based on RCP Algo ---")
    try:
        df_rcp = getrcp.getrcp(df_raw=df_avl, pid=hid, db_confg=db_confg)
        logger.debug(df_rcp.head(n))
    except:
        logger.error("--- No base recommendation is derived. Check the data availability ---")
        logger.error("--- Check the booking data availability if df_avl is 0, df_avl={}---".format(df_avl.__len__()))
        return "-1", "-1"

    mapr = pd.DataFrame(df_rcp, columns=['occupancydate', 'rcp', 'rate_factor', 'dow_min_price'])
    mapr.rename(columns={'dow_min_price': 'min_rate'}, inplace=True)
    logger.info("--- Applying Psychological Factor ---")
    mapr['rcp'] = mapr['rcp'].astype(int)

    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    # Generate the Recommendations Market based Pricing
    # call the MPI, PQM and ARI Algorithms to generate the recommendations
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    logger.info("--- Generate the Recommendations Market based Pricing ---")

    myquery = (f"select distinct hotel_code as propertydetailsid, " 
               "DATE_FORMAT(checkin_date,'%Y-%m-%d') as occupancydate, " 
               "greatest(onsite_rate,rate) as htl_rate, DATE_FORMAT(date_collected,'%Y-%m-%d') as regdate " 
               "FROM rm_rates where hotel_code in ({}) " 
               "and checkin_date between '{}' and '{}' " 
               "and date_collected >= '{}' order by hotel_code, occupancydate;".format(comp_ids, start_date, end_date, clt_date))

    try:
        logger.info("--- Fetching the Rate Shopping Data ---")
        df_comprate = getData.getData(myquery=myquery, pid=hid, db_confg=db_confg)
    except:
        logger.error("--- Rate Shopping Data Not Available. Check the data availability ---")
        return "-1", "-1"

    df_comprate['occupancydate'] = pd.to_datetime(df_comprate['occupancydate'], format="%Y-%m-%d")
    df_comprate['regdate'] = pd.to_datetime(df_comprate['regdate'], format="%Y-%m-%d")
    df_comprate = df_comprate.query('(htl_rate > 0)')

    df_maxdate = pd.DataFrame(df_comprate.groupby(['propertydetailsid', 'occupancydate'])['regdate'].max().reset_index())

    merged = pd.merge(df_comprate, df_maxdate, on=['propertydetailsid', 'occupancydate', 'regdate'], how='inner')

    df_rsdata = pd.DataFrame(merged.groupby(['propertydetailsid', 'occupancydate'])["htl_rate"].min().reset_index())

    #=================================
    df_mean = pd.DataFrame(df_rsdata.groupby(['occupancydate'])['htl_rate'].median().reset_index())

    df_mean.rename(columns={'htl_rate': 'mu'}, inplace=True)

    df_rsdata0 = pd.merge(df_rsdata, df_mean, on='occupancydate', how='left')

    df_rsdata0['sigma'] = np.sqrt(df_rsdata0.mu)

    df_rsdata0['htl_rate'] = np.where(df_rsdata0.mu + 1.5 * df_rsdata0.sigma >= df_rsdata0['htl_rate'],
                                      df_rsdata0['htl_rate'], df_rsdata0.mu + 1.5 * df_rsdata0.sigma)

    df_rsdata1 = mapr.merge(df_rsdata, on='occupancydate', how='left')

    logger.debug("--- Rate Shopping Data ---")
    logger.debug(df_rsdata1.head(n=10))

    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    # Generate the Recommendations using the MPI Algo
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    logger.info("--- Generating MPI Recommendations ---")
    if calc_mpi == "1":
        try:
            logger.info("--- Calculating Weights for MPI Recommendations ---")
            df_mpi = mapdata.mapdf(asc_dec=0, raw_df=df_rsdata1, pid=htlid, rcp_df=mapr, algo="MPI", client_id=htlid)
        except:
            logger.warning("--- Failed to Calculate Weights for MPI Recommendations ---")
            # return "-1", "-1"

        logger.info("--- Restricting the highest weight to number of hotels ---")

        df_mpi['wgt'] = np.where((df_mpi['wgt'] > htl_count), htl_count, df_mpi['wgt'])

        mpi_rate = mapdata.mpi_ari_pqm(df_mpi, mapr)

        logger.info("--- Restricting the weighted average to 2 time the RCP ---")
        mpi_rate['wavg'] = np.where((mpi_rate['wavg']/mpi_rate['rcp'] > 2), mpi_rate['rcp']*2, mpi_rate['wavg'])

        logger.info("--- MPI Recommendations ---")
        mpi_rate['rtmpi'] = mpi_rate.apply(lambda row: mapdata.optRate(row['wavg']), axis=1)

        # SAM 19JAN2020: Added this line to restrict the drop in rate
        mpi_rate['rtmpi'] = np.where((mpi_rate['rtmpi'] / mpi_rate['rcp'] < 0.75), (mpi_rate['rcp'] * 0.75), mpi_rate['rtmpi'])
        mpi_rate['rtmpi'] = mpi_rate['rtmpi'].astype('int')

        logger.debug("--- Printing MPI Recommendations ---")
        logger.debug(mpi_rate.head(n=10))

        logger.info("--- Generated MPI Recommendations ---")

    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    # Generate the Recommendations using the ARI Algo
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    logger.info("--- Generating ARI Recommendations ---")
    if calc_ari == "1":
        try:
            logger.info("--- Calculating Weights for ARI Recommendations ---")
            df_ari = mapdata.mapdf(asc_dec=1, raw_df=df_rsdata1, pid=htlid, rcp_df=mapr, algo="ARI", client_id=htlid)
        except:
            logger.warning("--- Failed to Calculate Weights for ARI Recommendations ---")
            # return "-1", "-1"

        logger.info("--- Restricting the highest weight to number of hotels ---")

        df_ari['wgt'] = np.where((df_ari['wgt'] > htl_count), htl_count, df_ari['wgt'])

        ari_rate = mapdata.mpi_ari_pqm(df_ari, mapr)

        logger.info("--- Restricting the weighted average to 3 time the RCP ---")
        ari_rate['wavg'] = np.where((ari_rate['wavg']/ari_rate['rcp'] > 3), ari_rate['rcp']*3, ari_rate['wavg'])

        logger.info("--- ARI Recommendations ---")
        ari_rate['rtari'] = ari_rate.apply(lambda row: mapdata.optRate(row['wavg']), axis=1)

        # SAM 19JAN2020: Added this line to restrict the drop in rate
        ari_rate['rtari'] = np.where((ari_rate['rtari'] / ari_rate['rcp'] < 0.90), ari_rate['rcp'] * 0.90, ari_rate['rtari'])
        ari_rate['rtari'] = ari_rate['rtari'].astype('int')

        # logger.debug("--- Printing ARI Recommendations ---")
        # logger.debug(ari_rate)

        logger.info("--- Generated ARI Recommendations ---")

    # Generate the Recommendations using the PQM Algo
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    # This might have to change based on the changed DB Schema
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    logger.info("--- Generating PQM Recommendations ---")
    if calc_pqm == "1":

        myquery = ("select h.rm_code as propertydetailsid, " +
                   "(avg(hotel_score/max_score) * sum(quantity)/tot_rev) * 5 as score " +
                   "from clients_qm as q " +
                   "inner join hotels as h on (h.id = q.hotel_id) " +
                   "inner join (select client_id,  sum(quantity) as tot_rev " +
                   "from clients_qm group by client_id)  as tr " +
                   "using (client_id) where client_id = :prid group by h.rm_code")

        try:
            logger.info("--- Fetch the Quality data for the Competition Hotels ---")
            qic = getData.getData(myquery=myquery, pid=hid, db_confg=db_confg)
        except:
            logger.warning("--- Failed Fetch the Quality data for the Competition Hotels ---")
            # return "-1", "-1"

        logger.info("--- Merge both the datasets ---")
        qi = qic
        # logger.debug(qi)

        logger.info("--- Identify the client hotel score ---")

        try:
            clnt = qi.loc[qi['propertydetailsid'] == htlid, 'score'].iloc[0]
        except:
            logger.warning("--- Setting the value as Quality Metrics since there is no value set ---")
            clnt = 1

        logger.info("--- Calculate the score distance of each hotels from the client hotel ---")

        #qi['rnk'] = ((qi['score']-clnt)/clnt)+clnt
        qi['rnk'] = qi['score']
        qi = qi.drop('score', axis=1)
        # logger.debug(qi)

        try:
            logger.info("--- Calculating Weights for PQM Recommendations ---")
            df_pqm = mapdata.mapdf(asc_dec=True, raw_df=df_rsdata1, pid=hid, rcp_df=mapr, algo="PQM", pqmdf=qi)
        except:
            logger.warning("--- Failed to Calculate Weights for PQM Recommendations ---")

        df_pqm['wgt'] = np.where((df_pqm['wgt'] > htl_count), htl_count, df_pqm['wgt'])

        pqm_rate = mapdata.mpi_ari_pqm(df_pqm, mapr)

        pqm_rate['wavg'] = np.where((pqm_rate['wavg']/pqm_rate['rcp'] > 2.5), pqm_rate['rcp']*2.5, pqm_rate['wavg'])

        logger.info("--- PQM Recommendations ---")
        pqm_rate['rtpqm'] = pqm_rate.apply(lambda row: mapdata.optRate(row['wavg']), axis=1)

        logger.debug("--- Printing PQM Recommendations ---")
        logger.debug(pqm_rate.head(n))

        logger.info("--- Generated PQM Recommendations ---")

    #~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    # Clubing all the 4 types of recommendations in order
    # to create a super set for either update or insert
    #~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    logger.info("--- Clubing all Recommendations togather to right into the database ---")

    df_bar = pd.DataFrame(mapr, columns=['occupancydate', 'rcp', 'min_rate', 'rate_factor'])

    if calc_ari == "1":
        df_bar = df_bar.merge(pd.DataFrame(ari_rate, columns=['occupancydate', 'rtari']), on='occupancydate', how='left')
    if calc_ari == "0":
        df_bar['rtari'] = -1
    if calc_mpi == "1":
        df_bar = df_bar.merge(pd.DataFrame(mpi_rate, columns=['occupancydate', 'rtmpi']), on='occupancydate', how='left')
    if calc_mpi == "0":
        df_bar['rtmpi'] = -1
    if calc_pqm == "1":
        df_bar = df_bar.merge(pd.DataFrame(pqm_rate, columns=['occupancydate', 'rtpqm']), on='occupancydate', how='left')
    if calc_pqm == "0":
        df_bar['rtpqm'] = -1

    df_bar['creation_date'] = pd.datetime.now()
    df_bar['client_id'] = hid
    df_bar['hotel_id'] = hotel_id

    # ~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~
    # Calculating rates for updating into the database
    # ~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~o~~
    df_bar['room_id'] = 0
    df_bar['psymulti'] = int(psymult)
    df_bar['psysub'] = int(psysub)

    logger.info("--- If ARI is smaller than MPI set MPI as ARI ---")
    df_bar['rtari_temp'] = np.where(df_bar['rtari'] < df_bar['rtmpi'], df_bar['rtmpi'], df_bar['rtari'])

    logger.info("--- If MPI is higher than ARI set ARI as MPI ---")
    df_bar['rtmpi_temp'] = np.where(df_bar['rtari'] < df_bar['rtmpi'], df_bar['rtari'], df_bar['rtmpi'])

    logger.info("--- If PQM is higher than ARI or Lower than MPI then set the PQM based on the skewness ---")
    df_bar['rtpqm_sum'] = df_bar['rtari_temp'] - df_bar['rtmpi_temp']

    df_bar['rtpqm_avg'] = df_bar['rtpqm_sum'] * 0.25

    df_bar['rtpqm_temp'] = np.where(df_bar['rtari_temp'] < df_bar['rtpqm'], df_bar['rtari_temp'] - df_bar['rtpqm_avg'],
                                    np.where(df_bar['rtmpi_temp'] > df_bar['rtpqm'],
                                             df_bar['rtmpi_temp'] + df_bar['rtpqm_avg'],
                                             df_bar['rtpqm']))

    df_bar['rtpqm_temp'] = np.where(df_bar['rtari_temp'] < df_bar['rtpqm_temp'], df_bar['rtpqm_avg'],
                                    np.where(df_bar['rtmpi_temp'] > df_bar['rtpqm_temp'], df_bar['rtpqm_avg'],
                                             df_bar['rtpqm_temp']))

    df_bar['rtpqm_temp'] = df_bar['rtpqm_temp'].astype(float)

    df_bar['rtari'] = df_bar['rtari_temp']

    df_bar['rtmpi'] = df_bar['rtmpi_temp']

    df_bar['rtpqm'] = df_bar['rtpqm_temp']

    delcol = ['rtari_temp', 'rtmpi_temp', 'rtpqm_temp', 'rtpqm_sum', 'rtpqm_avg']
    df_bar.drop(columns=delcol, inplace=True)

    logger.info("--- Applying rate Factor on rcp, rtmpi, rtpqm and rtari Recommendations ---")
    df_bar['rcp'] = df_bar['rcp'] * df_bar['rate_factor']

    df_bar['ratio_ari'] = df_bar['rtari'] / df_bar['rtmpi']
    df_bar['ratio_pqm'] = df_bar['rtpqm'] / df_bar['rtmpi']

    df_bar['rtmpi_rf'] = np.where(df_bar['rtmpi'] > df_bar['min_rate'], df_bar['rtmpi'],
                                  np.where(df_bar['rtmpi'] * df_bar['rate_factor']/df_bar['rcp'] >= 2,
                                           df_bar['rcp'] * 1.25,
                                           df_bar['rtmpi'] * df_bar['rate_factor']))

    df_bar['rtmpi_b'] = np.where(df_bar['rtmpi_rf'] > df_bar['min_rate'], df_bar['rtmpi_rf'],
                                 np.where(df_bar['min_rate'] * df_bar['rate_factor'] / df_bar['rcp'] >= 2,
                                          df_bar['min_rate'] * 1.25,
                                          df_bar['min_rate'] * df_bar['rate_factor']))

    df_bar['rtmpi_a'] = np.where(df_bar['rtmpi_b'] < df_bar['min_rate'], df_bar['min_rate'],
                                 df_bar['rtmpi_b'])

    df_bar['rtari_a'] = df_bar['rtmpi_a'] * df_bar['ratio_ari']
    df_bar['rtpqm_a'] = df_bar['rtmpi_a'] * df_bar['ratio_pqm']

    logger.info("--- Applying Psychological Factor on Recommendations ---")
    df_bar['rcp'] = ((round(df_bar['rcp'] / df_bar['psymulti'])).astype(int) * df_bar['psymulti']) - df_bar['psysub']
    df_bar['rtmpi'] = ((round(df_bar['rtmpi_a'] / df_bar['psymulti'])).astype(int) * df_bar['psymulti']) - df_bar['psysub']
    df_bar['rtpqm'] = ((round(df_bar['rtpqm_a'] / df_bar['psymulti'])).astype(int) * df_bar['psymulti']) - df_bar['psysub']
    df_bar['rtari'] = ((round(df_bar['rtari_a'] / df_bar['psymulti'])).astype(int) * df_bar['psymulti']) - df_bar['psysub']
    df_bar_all_data = pd.DataFrame(df_bar)
    logger.info("--- Rearanging the Column Headers ---")

    df_bar = df_bar[['hotel_id', 'client_id', 'room_id', 'creation_date', 'occupancydate', 'rcp', 'rtmpi', 'rtpqm', 'rtari']]

    df_bar.rename(columns={'occupancydate': 'checkin_date'}, inplace=True)

    logger.info("~o~o~o~o~o~o~o~o~o~o~o~o~o DATA TYPE ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")
    logger.debug(df_bar.dtypes)
    logger.debug(df_bar.head(n=10))
    logger.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")

    myquery = ("select hotel_id, client_id, room_id, creation_date, "
               "DATE_FORMAT(checkin_date,'%Y-%m-%d') as checkin_date, rate_rcp as prcp, rate_mpi as pmpi,"
               " rate_pqm as ppqm, rate_ari as pari from recommendations_all_by_date where client_id =:prid and"
               " checkin_date between '" + start_date + "' and '" + end_date + "' ")

    try:
        logger.info("--- Fetching the existing BAR Recommendations for the given dates ---")
        df_bar_raw = getData.getData(myquery=myquery, pid=hid, db_confg=db_confg)
    except:
        logger.warning("--- Failed to fetching BAR Recommendations. Check the database for data availability ---")
        df_bar_raw = pd.DataFrame()

    logger.info("--- Existing BAR Recommendations - %s ---", len(df_bar_raw.index))                #STOP HERE
    if len(df_bar_raw.index) > 0:
        df_bar_raw['checkin_date'] = pd.to_datetime(df_bar_raw['checkin_date'], format="%Y-%m-%d")
        df_maxdate = pd.DataFrame(df_bar_raw.groupby(['client_id', 'room_id',
                                                      'checkin_date'])['creation_date'].max().reset_index())
        merged = df_bar_raw.merge(df_maxdate, on=['client_id', 'room_id', 'creation_date',
                                                  'checkin_date'], how='inner')

        #SAM 08JAN22: Need to add a validation step here
        #---------------------------------------------------

        df_bar_db = merged.drop('creation_date', axis=1)
        df_bar1 = pd.DataFrame(df_bar_db)
        logger.info("--- Existing BAR Recommendations after filtering - %s ---", len(df_bar1.index))
        logger.debug(df_bar1.head(n))
        logger.debug(df_bar1.dtypes)
    else:
        logger.info(" --- Ceating empty data frame df_bar1 --- ")
        df_bar1 = pd.DataFrame(df_bar_raw)

    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    # Clubbing all the 4 types of recommendations in order to create a super set for either update or insert
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    logger.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")
    logger.info("~o~o~o~o~o~o~o~o~ Recommendations for insertion ~o~o~o~o~o~o~o~o~")
    logger.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")
    if len(df_bar1.index) > 0:
        logger.info("--- Finding new Recommendations ---")
        df_bar2 = df_bar.merge(df_bar1, on=['hotel_id', 'client_id', 'checkin_date', 'room_id'], how='left')

        df_bar2['new_rate'] = np.where((df_bar2['rtari'] != df_bar2['pari']), 1,
                                       np.where((df_bar2['rtmpi'] != df_bar2['pmpi']), 1,
                                                np.where((df_bar2['rtpqm'] != df_bar2['ppqm']), 1,
                                                         np.where((df_bar2['rcp'] != df_bar2['prcp']), 1, 0))))

        logger.info("--- Finding the change in the inventory position for new Recommendations ---")
        df_bar2['new_rcp'] = np.where((df_bar2['rcp'] != df_bar2['prcp']), 1, 0)

        logger.info("--- Changing the column type to float ---")

        df_bar2['rcp'] = df_bar2['rcp'].astype('float64')
        df_bar2['rtari'] = df_bar2['rtari'].astype('float64')
        df_bar2['rtmpi'] = df_bar2['rtmpi'].astype('float64')
        df_bar2['rtpqm'] = df_bar2['rtpqm'].astype('float64')

        logger.debug(df_bar2.head(n))

        delcol = ['pari', 'pmpi', 'ppqm', 'prcp']
        df_bar_in_1 = df_bar2.drop(delcol, axis = 1)

        df_bar_in = pd.DataFrame(df_bar_in_1)

        logger.info("--- New Recommendations are ready for upload after filtering ---")
        logger.debug(df_bar_in)
    else:
        logger.info("--- All are New Recommendations ---")
        df_bar_in = pd.DataFrame(df_bar)
        df_bar_in['rcp'] = df_bar_in['rcp'].astype('float64')
        df_bar_in['rtari'] = df_bar_in['rtari'].astype('float64')
        df_bar_in['rtmpi'] = df_bar_in['rtmpi'].astype('float64')
        df_bar_in['rtpqm'] = df_bar_in['rtpqm'].astype('float64')
        df_bar_in['new_rate'] = 1

    df_bar_in.rename(columns={'rcp': 'rate_rcp', 'rtmpi': 'rate_mpi', 'rtpqm': 'rate_pqm', 'rtari': 'rate_ari'},
                     inplace=True)

    logger.debug(df_bar_in.head(n))
    logger.debug(" --- Recommendations ready to be inserted  --- %s",  df_bar_in)

    df_bar_in['checkin_date'] = pd.to_datetime(df_bar_in['checkin_date'], format="%Y%m%d")
    df_bar_in['creation_date'] = pd.to_datetime(df_bar_in['creation_date'], format="%Y%m%d")

    logger.debug(df_bar_in.head(n))

    # Connecting to the Database
    cnx = connectdb.conectdb(db_confg)

    try:
        logger.info("--- insert into recommendations_all_by_date ---")
        if len(df_bar1.index) > 0:
            logger.debug("--- There are existing BAR recommendations - %s ---", len(df_bar1.index))
            logger.debug(df_bar_in.head(n))
            df_bar_in_2 = df_bar_in.drop('new_rcp', axis = 1)
            logger.debug(df_bar_in_2.head(n))
            df_bar_all_in = pd.DataFrame(df_bar_in_2)
        else:
            logger.debug("--- There are no existing BAR recommendations --- ")
            df_bar_all_in = pd.DataFrame(df_bar_in)

        df_bar_all_in = df_bar_all_in.query('(new_rate > 0)')
        df_bar_all_in = df_bar_all_in.drop('new_rate', axis=1)
        df_bar_all_in.to_sql(con=cnx, name='recommendations_all_by_date', if_exists='append', index=False,
                             index_label='id')
        # recom_count = len(df_bar_all_in)
    except:
        logger.error("--- Failed to insert BAR Recommendations. Check the database ---")
        return "-1", "-1"

    # =============================================

    logger.info("--- Get final Recommendations from DB---")

    myquery = ("SELECT a1.hotel_id, a1.client_id, a1.room_id, a1.update_date, " +
               "DATE_FORMAT(a1.checkin_date, '%Y-%m-%d') AS checkin_date, " +
               "a1.final_rate as cur_rate, a1.id FROM recommendations_final_by_date AS a1 " +
               "INNER JOIN (SELECT client_id, checkin_date, MAX(id) AS id " +
               "FROM recommendations_final_by_date " +
               "WHERE client_id =:prid  " +
               "AND  checkin_date BETWEEN '" + start_date + "' AND '" + end_date + "' " +
               "GROUP BY client_id, checkin_date) AS a2 " +
               "ON (a1.id = a2.id )  " +
               "ORDER BY a1.checkin_date")

    try:
        logger.info("--- Fetching the existing BAR Recommendations from recommendations_final_by_date for"
                     " the given dates ---")
        df_bar_fnl = getData.getData(myquery=myquery, pid=hid, db_confg=db_confg)
    except:
        logger.error("--- Failed to fetch Recommendations . Check the database ---")
        return "-1", "-1"

    logger.info("--- Get latest Recommendations from DB---")
    if len(df_bar_fnl.index) > 0:
        df_bar_fnl = df_bar_fnl.drop(columns=['id'])
    # use to merge
    else:
        df_bar_fnl = pd.DataFrame(df_bar_fnl)

    # ===================================================================
    logger.info("--- Get Override details ---")
    myquery = ("select hotel_id, client_id, room_id, DATE_FORMAT(checkin_date,'%Y-%m-%d') as checkin_date,"
               " override_algo, override_value from recommendations_override_by_date where client_id =:prid and "
               "checkin_date between '" + start_date + "' and '" + end_date + "' and active_override = 'active' ")

    try:
        logger.info("--- Fetching the existing BAR overrides for the given dates ---")
        df_bar_ord = getData.getData(myquery=myquery, pid=hid, db_confg=db_confg)
    except:
        df_bar_ord = pd.DataFrame(columns=['hotel_id', 'client_id', 'room_id', 'checkin_date',
                                           'override_algo', 'override_value'])
        logger.error("--- Failed to fetch overrides. Check the database --- Empty DataFrame created")

    df_ord_algo = pd.DataFrame(df_bar_ord, columns=['checkin_date', 'override_algo',  'override_value'])
    df_ord_algo.rename(columns={'checkin_date': 'date'}, inplace=True)
    logger.info("--- Get Default Algo ---")
    myquery = "select use_algo from clients where id =:prid"


    # SAM 21FEB2022: Call the new module here that selects one rate amongst the 4
    # this code will be invoked based on the setup in client table
    # if the "Use Alog" is set to "System" then one of the 4 will be selected by
    # the system else it will be as per the setup


    try:
        logger.info("--- Geting the default Algo used for pricing ---")
        use_algo = getData.getData(myquery=myquery, pid=hid, db_confg=db_confg)
        dAlog = use_algo.iloc[0, 0]
    except:
        logger.error("--- Failed to fetch Algo setup. Check the database. Set the default as RCP ---")
        dAlog = "RCP"

    try:
        logger.info("--- Geting the Algo by pace point to used for pricing ---")
        pp = pacebyalgo.PaceByAlgo(pid, db_confg)
        pacealgo_df = pp.select_algo(start_date, end_date)
    except Exception as E:
        pacealgo_df = pd.DataFrame(columns=['Date', 'algo'])
        logger.info("--- Failed to fetch pace point Algo . Check the database --- Empty DataFrame created ")
        logger.error("--- Failed to fetch pace point Algo. Check the database and error:{}".format(E))

    df_bar_in = df_bar_in.drop('new_rate', axis=1)
    df_bar_in['default_algo'] = dAlog

    # SAM 23FEB22: call the new subroutine to identify the rate to push

    df_bar_in['date'] = pd.to_datetime(df_bar_in['checkin_date'])
    df_bar_in['pace_point'] = (df_bar_in['date'] - pd.to_datetime(start_date)).dt.days
    df_bar_in['date'] = pd.to_datetime(df_bar_in['date']).dt.date.astype(str)

    pacealgo_df['date'] = pd.to_datetime(pacealgo_df['date']).dt.date.astype(str)
    try:
        df_bar_in_algo = df_bar_in.merge(pacealgo_df, on=['date', 'pace_point'], how='left')
    except Exception as E:
        df_bar_in_algo = pd.DataFrame()
        logger.info('error @ merge pacealgo with df_bar_in: {}'.format(E))
    try:
        df_bar_all_algo = df_bar_in_algo.merge(df_ord_algo, on='date', how='left')
    except Exception as E:
        logger.info('error @ merge pacealgo with over ride algo: {}'.format(E))
        pass

    df_bar_all_algo['final_algo'] = np.where((
            df_bar_all_algo['override_algo'].astype(str) != 'nan'), df_bar_all_algo['override_algo'],
        np.where((df_bar_all_algo['algo'].astype(str) != 'nan'), df_bar_all_algo['algo'],
                 df_bar_all_algo['default_algo']))

    df_bar_all_algo['selected_rate'] = \
        np.where(df_bar_all_algo['final_algo'] == 'other', df_bar_all_algo['override_value'],
                 np.where(df_bar_all_algo['final_algo'] == 'MPI', df_bar_all_algo['rate_mpi'],
                          np.where(df_bar_all_algo['final_algo'] == 'ARI', df_bar_all_algo['rate_ari'],
                                   np.where(df_bar_all_algo['final_algo'] == 'PQM', df_bar_all_algo['rate_pqm'],
                                            df_bar_all_algo['rate_rcp']))))

    df_bar_all_algo['checkin_date'] = pd.to_datetime(df_bar_all_algo['checkin_date'], format="%Y-%m-%d").dt.date
    df_bar_fnl['checkin_date'] = pd.to_datetime(df_bar_fnl['checkin_date'], format="%Y-%m-%d").dt.date

    # SAM 27APR20: put a try catch here if there is no data in df_bar_fnl create required columns
    # SAM 05MAY2020: does this work without the try catch - test it for various data conditions
    df_bar_all_algo = df_bar_all_algo.merge(df_bar_fnl, on=['hotel_id', 'client_id', 'room_id', 'checkin_date'], how='left')

    df_bar_all_algo['new_rate'] = np.where((df_bar_all_algo['selected_rate'] != df_bar_all_algo['cur_rate']), 1, 0)
    df_bar_all_algo_for_ord = pd.DataFrame(df_bar_all_algo)

    #SAM 11JAN22: Make changes here to put the data into new table
    #--------------------------------------------------------------

    if Run_Recom_All_Diff == '1':
        df_bar_all_algo = df_bar_all_algo.query('(new_rate > 0)')
    else:
        df_bar_all_algo['new_rate'] = 1

    # df_bar_final = df_bar_all_algo.drop(['update_date', 'new_rate', 'final_rate'],axis = 1)
    df_bar_final = pd.DataFrame(df_bar_all_algo, columns=['hotel_id', 'client_id', 'room_id', 'creation_date',
                                                          'checkin_date', 'selected_rate'])
    logger.debug("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")
    logger.debug(df_bar_final.head(n))
    logger.debug("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")
    try:
        logger.info("--- Inserting the New BAR Recommendations directly into recommendations_final_by_date ---")
        df_bar_final.rename(columns={'selected_rate': 'final_rate', 'creation_date': 'update_date'}, inplace=True)
        df_bar_final.to_sql(con=cnx, name='recommendations_final_by_date', if_exists='append', index=False,
                            index_label='id')
    except:
        logger.error("--- Failed to insert BAR Recommendations into recommendations_final_by_date."
                      " Check the database ---")
        logger.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")
        return "-1", "-1"

    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
    #    SAM 10FEB22 New code to store data into newly created recommendations_to_show table
    # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~

    try:
        logger.info("--- Inserting or updated recommendations_to_show table to use for display purpose ---")

        df_bar_for_show = pd.DataFrame(df_bar_all_algo_for_ord, columns=['update_date', 'creation_date', 'checkin_date',
                                                                          'client_id', 'selected_rate', 'hotel_id',
                                                                          'new_rate', 'room_id', 'final_algo'])

        df_bar_for_show.rename(columns={'selected_rate': 'final_rate', 'creation_date': 'updated_date',
                                        'update_date': 'created_date', 'new_rate': 'push_rate',
                                        'final_algo': 'strategy'}, inplace=True)

        df_bar_for_show = df_bar_for_show[['created_date', 'updated_date', 'checkin_date', 'client_id', 'final_rate',
                                          'hotel_id', 'push_rate', 'room_id', 'strategy']]
        logger.info("--- Deleting the old recommendations_to_show values ---")
        cnx.execute(f"Delete from recommendations_to_show "
                    "where client_id = {} "
                    "and checkin_date >= '{}' ".format(hid, start_date))

        logger.info("--- Inserting the new recommendations_to_show values ---")
        df_bar_for_show.to_sql(con=cnx, name='recommendations_to_show', if_exists='append', index=False, index_label='id')

    except:
        logger.error("--- Failed to insert BAR to display  into recommendations_to_show. Check the database ---")
        logger.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")
        return "-1", "-1"

    try:
        if len(df_bar.index) > 0:
            logger.info("update clients"
                         " set system_today = '{}' where hotel_id ={}"
                         " and id = {} ".format(str(datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S')),
                                                int(hotel_id), int(hid)))
            cnx.execute("update clients"
                        " set system_today = '{}' where hotel_id ={}"
                        " and id = {} ".format(str(datetime.datetime.now().__format__('%Y-%m-%d %H:%M:%S')),
                                               int(hotel_id), int(hid)))
    except:
        logger.error(
            "--- Failed to update system date into client table for BAR Recommendations into "
            "recommendations_final_by_date after considering the overrides. Check the database ---")
        return "-1", "-1"

    logger.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")
    logger.info("--- There are exisitng overrides ---")
    df_bar_all_ord = df_bar_all_algo_for_ord[df_bar_all_algo_for_ord['override_algo'].notnull()]
    df_bar_all_ord = pd.DataFrame(df_bar_all_ord, columns=['hotel_id', 'client_id', 'room_id', 'checkin_date',
                                                           'override_algo', 'override_value', 'selected_rate'])
    if len(df_bar_all_ord.index) > 0:
        df_bar_all_ord['checkin_date'] = pd.to_datetime(df_bar_all_ord['checkin_date'], format="%Y-%m-%d").dt.date
        df_for_ord = pd.DataFrame(df_bar_all_ord)
        logger.debug(df_bar_all_ord.head(n))
        logger.info("~o~o~o~o~o~o~o~ created the data frame with all the final recommendatons ~o~o~o~o~o~o~o~o~o~o~")
        logger.debug(df_for_ord.head(n))

        df_for_ord['override_value'] = np.where((df_for_ord['override_algo']!='other'), df_for_ord['selected_rate'],
                                                df_for_ord['override_value'])

        logger.info("~o~o~o~o~o~o~o~ created the data frame with updated overrides ~o~o~o~o~o~o~o~o~o~o~")

        logger.debug(df_for_ord.head(n))
        df_ord_final = df_for_ord.drop(['selected_rate'],axis = 1)
        df_ord_final['checkin_date'] = pd.to_datetime(df_ord_final['checkin_date'], format="%Y-%m-%d").dt.date

        try:
            logger.info("--- Inserting the New BAR Overrides back into recommendations_override_by_date table after"
                         " considering the Algos ---")
            for row in df_ord_final.itertuples():
                logger.info("update recommendations_override_by_date set override_value = {} "
                             "where hotel_id ={} and client_id = {} and room_id = {} and"
                             " checkin_date = '{}'".format(float(row.override_value), int(row.hotel_id),
                                                           int(row.client_id), int(row.room_id), row.checkin_date))

                cnx.execute("update recommendations_override_by_date set override_value = {} "
                            "where hotel_id ={} and client_id = {} and room_id = {} and "
                            "checkin_date = '{}'".format(float(row.override_value), int(row.hotel_id),
                                                         int(row.client_id), int(row.room_id), row.checkin_date))

        except:
            logger.error("--- Failed to insert BAR Overrides into recommendations_override_by_date after considering"
                          " the Algos. Check the database ---")
            logger.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")
            return "-1", "-1"

    logger.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")
    logger.info("~o~o~o~o~o~o~o~o~o~ Recommendations Updated ~o~o~o~o~o~o~o~o~o~o~")
    logger.info("~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~")

    return len(df_bar_final), date_range


# ~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~
# Applying the psychological factor based
# on the value of the recommendation
# ~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~


def applyPsychologicalFactor(n):
    # print(int(str(n)[-3:]))
    # print(len(str(n)))
    logger.debug("--- Applying Psycological Factor ---")
    if n > 0:
        if (115>int(str(n)[-3:])>100):
            if len(str(n))>4:
                rval =(int((round(n,-1)-100)/100)*100)-1
                logger.debug("Len > 4, Greater than 100 and Smaller than 115 new Value - %s", rval)
            elif len(str(n))<4:
                rval =(int(round(n,-1)/10)*10)-1
                logger.debug("Len < 4, Greater than 100 and Smaller than 115 new Value - %s", rval)
            else:
                rval=(int((round(n,-1)-10)/100)*100)-1
                logger.debug("Greater than 100 and Smaller than 115 new Value - %s", rval)
        elif (15>int(str(n)[-2:])>5):
            rval=(int((round(n, -1))/50)*50)-1
            logger.debug("Greater than 5 and Smaller than 15 new Value - %s", rval)
        else:
            rval =(int(round(n,-1)/10)*10)-1
            logger.debug("Outside the range new Value - %s", rval)
        logger.debug("--- Applied relevant Psycological Factor ---")
    else:
        rval = n
        
    return rval


#
#if __name__ == '__main__':
#    #pid, hCap, rhost, ruser, rpwd, rdb, dbtype
#
#    rhost      = sys.argv[1]
#    ruser      = sys.argv[2]
#    rpwd       = sys.argv[3]
#    rdb        = sys.argv[4]
#    dbtype     = sys.argv[5]
#    pid        = int(sys.argv[6])
#    start_date =sys.argv[7]
#    date_range = int(sys.argv[8])
#    calc_ari   = sys.argv[9]
#    calc_mpi   = sys.argv[10]
#    calc_pqm   = sys.argv[11]
#    getRecommendations(rhost, ruser, rpwd, rdb, dbtype, pid, start_date, date_range, calc_ari, calc_mpi, calc_pqm)
#
