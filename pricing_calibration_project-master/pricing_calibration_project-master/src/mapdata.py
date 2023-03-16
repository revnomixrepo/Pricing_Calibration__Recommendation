# -*- coding: utf-8 -*-
"""
Created on Mon Jan  9 12:05:19 2017

@author: Sameer.Kulkarni
"""

import pandas as pd
import numpy  as np
from scipy.stats import poisson
import logging
logger = logging.getLogger(f'Recomservicelog.{__name__}')
n = 10


def mapdf(asc_dec=True, raw_df=None, pid = None, rcp_df=None, algo="MPI", pqmdf=None, client_id=None):

    logger.info("Calculating the Recommendation using Algo - %s", algo)
    df_name = raw_df
    df_name = df_name.replace('Sold',np.nan, regex=True)
    df_name = df_name.replace('Closed',np.nan, regex=True)
    df_name = df_name.sort_values(['occupancydate','htl_rate'], ascending=[True, True])

    # SAM 21SEP2019: Added this code to restrict the weightage
    # -----------------------------------------------------------
    df_name['htl_rate'] = np.where(df_name['propertydetailsid'] == client_id,
                                   np.where(df_name['htl_rate']/df_name['rcp']<0.7, df_name['rcp']*0.7,
                                             np.where(df_name['htl_rate']/df_name['rcp'] > 1.5, df_name['rcp'] * 1.5,
                                                       df_name['htl_rate'])),
                                   df_name['htl_rate'])
    # -----------------------------------------------------------

    df_name['pp']=(df_name['htl_rate'] - df_name['rcp'])/df_name['rcp']        
    logger.debug(df_name.head(n))
    
    #===============
    logger.info("Calculating the ranks of each of the hotels")
    if algo == "PQM":
        df_name = df_name.merge(pqmdf, on='propertydetailsid', how='left')        
    else:
        df_name['rnk'] = df_name.groupby(['occupancydate'])['htl_rate'].rank(ascending=asc_dec)
    #===============
    
    logger.info("Calculating the weights for each of the hotels")
    df_name['wgt']=np.where((df_name['rnk'] < df_name['pp']),(df_name['pp'] - df_name['rnk']),(df_name['rnk'] - df_name['pp']))

    # df_name.to_csv(
    #     'F:/SamsDrive/OneDrive/Revnomix_backup/GDrive/Rate_Pilots/Test_Results/wgt_' + algo + '.csv', sep=',',
    #     encoding='utf-8')

    # This code is introduced to set the Weight for the hotel with ZERO rate value to ZERO
    # This is essential to set the right Weight for the hotels

    df_name['wgt'] = np.where((df_name['htl_rate'] == 0),0, df_name['wgt'])

    df_name = df_name.fillna(0)
    df_name['wgt'] = np.where((df_name['htl_rate'] > 0),
                            np.where(df_name['wgt'] == 0.0, 1, df_name['wgt']),
                              df_name['wgt'])

    logger.debug("Dataframe with the weights")
    logger.debug(df_name.head(n))
    
    logger.info("Set the Hotel Rate to RCP in place of the market rate")
    df_name['htl_rate'] = np.where(df_name['propertydetailsid'] == pid, df_name['rcp'], df_name['htl_rate']) 

    del df_name['propertydetailsid']

    logger.debug("Final Dataframe to calculate the Market Apropriate Price")
    logger.debug(df_name.head(n))
    
    return df_name


def mpi_ari_pqm(dfin,rcp_df):
    # import datetime
    logger.info("Calculating the weighted average for each of the hotels")
    grouped = dfin.groupby('occupancydate')
    def wavg(group):
        d = group['htl_rate']
        w = group['wgt']
        if w.sum()>0:
             wSum = w.sum() 
        else :
            wSum = 1
        return (d * w).sum() /wSum
    mpir = pd.DataFrame(grouped.apply(wavg))
    mpir.columns=['wavg']
    mpir['wavg'] = pd.to_numeric(mpir['wavg'])
    mpi_df=rcp_df.join(mpir, on='occupancydate', how='left')
    logger.debug("Dataframe with the weighted averages")
    logger.debug(mpi_df.head(n))
    return mpi_df


def optRate(mean):
    mu = mean

    logger.debug("Getting Optimal Rate")
    logger.debug("The Mu is %s ", mu)
    
    if mu <=0 :
        optr = -1
    else:
        rngS = mu / 2
        rngE = 0.5 * np.sqrt(mu)
        bPnt = mu - rngS
        ePnt = mu + rngE
        x = np.arange(bPnt, ePnt, 0.01)
        xv = np.argmax((x * (1 - poisson.cdf(x, mu))))
        
        logger.debug("Print Optimal Rate")
        optr = int(x[xv])
        logger.debug("OptRate is %s ", optr)

    return optr
