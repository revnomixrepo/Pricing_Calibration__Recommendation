# -*- coding: utf-8 -*-
"""
Created on Fri Dec 23 12:26:26 2016

@author: Sameer.Kulkarni
"""

import os
import pandas as pd
import numpy  as np
from scipy.stats import linregress
import logging
logger = logging.getLogger(f'Calbrservicelog.{__name__}')


# Lower Bound and Upper Bound Calculations

def LUBound(adrData):
    
    if len(adrData.index)> 28:    
        q75, q50, q25 = np.percentile(adrData.values, [75,50, 25])
        logger.debug("25th - %s",q25)
        logger.debug("50th - %s",q50)
        logger.debug("75th - %s",q75)
    
        iqr = q75 - q25
        logger.debug("Inter Quartile Range - %s",iqr)
    
        # gap = 2.5
        # ubnd = q50 + (gap * iqr)
        gap = 1.5
        ubnd = q75 + (gap * iqr)
        logger.debug("Upper Bound - %s", ubnd)
        
        # lbnd = q50 - (gap * iqr)
        lbnd = q25 - (gap * iqr)
        logger.debug("Lower Bound - %s", lbnd)
        
        while (lbnd <= 0) :
            gap = gap - 0.01
            # lbnd = q50 - gap * iqr
            lbnd = q25 - (gap * iqr)
            # print(gap, " - ", lbnd)
            if lbnd > 0 :
                break
        logger.debug("Revised Lower Bound - %s at %s", lbnd, gap)
        
    else:
        lbnd = 0
        ubnd = 0
        
    logger.debug("Final Lower Bound - %s", lbnd)
    logger.debug("Final Upper Bound - %s", ubnd)
       
    return lbnd, ubnd


# 95th Percentile and 5th Percentile Calculations
def perTile(adrData,dow,wkDays,wkEnds):

    if len(adrData.index) < 30:        
        logger.info("Insufficient data to calculate the Mu and Sigma")
        p95 = 0
        p05 = 0
        mu = 0
        sigma = 0
    else:        
        WDS = wkDays 
        logger.debug("Weekdays - %s",WDS)
        # ['Monday','Tuesday','Wednesday','Thursday']
        WES = wkEnds
        logger.debug("Weekends - %s",WES)
        # ['Friday','Saturday','Sunday']
        
        if dow == 'WD':
            logger.info("Extracting Weekday data")
            ptDF = adrData[adrData['dow'].isin(WDS)]
        elif dow == 'WE':
            logger.info("Extracting Weekend data")
            ptDF = adrData[adrData['dow'].isin(WES)]
        elif dow == 'All':
            logger.info("Extracting all data")
            ptDF = adrData
        else:
            logger.info("Extracting data for - %s", dow)
            ptDF = pd.DataFrame(adrData[adrData['dow'] == dow])
        
        # mapr=pd.DataFrame(df_rcp,columns = ['occupancydate','rcp'])
        
        # ptDF = ptDF.reindex(columns= 'CheckInDate')
        logger.info("--- Reidexed the DataFrame ---")
        logger.debug(ptDF)
        logger.debug("-- number line in the dataframe %s --",len(ptDF.index))
        
        if len(ptDF.index) < 30:
            p95   = 0
            p05   = 0
            mu    = 0
            sigma = 0
            logger.debug("mu - %s, sigma - %s, p95 - %s, p05 - %s", mu, sigma, p95, p05)
        else:
            p95, p05 = np.percentile(ptDF.ADR, [95, 5])
        
            logger.info("--- Calculated Percentiles ---")
            logger.debug("%s - p05 - %s and p95 - %s", dow, p95, p05)
        
            cleanDF = ptDF[(ptDF['ADR'] >= p05) & (ptDF['ADR'] <= p95)]
            
            logger.info("--- Cleaned Data is ready for calculations ---")
            logger.debug(cleanDF)
            
            arr = np.array(cleanDF['ADR'])
            mu = np.mean(arr)
            sigma = np.std(arr)
    
    logger.info("--- Calculated Mu and Sigma ---")
    logger.debug("Mu - %s", mu)
    logger.debug("sigma - %s", sigma)
    
    return mu, sigma, p95, p05


""" --------------------------------------------------------------------------
    Calculating the slope and intercepts of the linear line
 --------------------------------------------------------------------------"""


def mcVal(x, y):
    
    logger.info("Return slope, intercept of best fit line.")
    min_rate = x.min()
    clean_data = pd.concat([x, y], 1).dropna(0)
    (_, x), (_, y) = clean_data.iteritems()
    slope, intercept, r, p, stderr = linregress(x, y)
    return slope, intercept, min_rate
    # return slope, intercept


""" --------------------------------------------------------------------------
    Calculating the z values and there by fitting the the ADR and Sold numbers 
    on to a linear line
 --------------------------------------------------------------------------"""


def zValues(inDF, DOW, mu, sigma, wdLst, weLst ):
    
    logger.debug("old mu - %s", mu)
    
    vRt1 = 0
    vRt2 = 0
    zRate = 0
    zSale = 0
    
    if mu > 0:
        zVal=[-3, -2.5, -2, -1.5, -1, -0.5, 0, 0.5, 1, 1.5, 2, 2.5, 3]
        zRate=[]
        gap = zVal[0]
        delta = (gap * sigma) + mu
        logger.debug("Original delta value - %s", delta)
        while delta <= 0 :
             gap = (gap + 0.01)
             logger.debug("Gap value - %s", gap)
             delta = (gap * sigma) + mu
             logger.debug("New delta value - %s", delta)
             if delta > 0 :
                 break
             
        if delta != (zVal[0] * sigma) + mu:
             mu = delta - (zVal[0] * sigma)
      
        logger.debug("New mu - %s", mu)
        logger.debug("Sigma - %s", sigma)

        for i in range(len(zVal)):
             zRate.append(zVal[i] * sigma + mu)
             logger.debug('{},{}'.format(i,zRate))
    
        logger.info("--- Calculating volume at each z Value ---")
    
        zSale=[]
        cnt = 0
     
        for i in range(len(zVal)):
            # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
            # This needs to be changed based on the setup
            # ~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~o~
            
             WDS = wdLst
            # ('Monday','Tuesday','Wednesday','Thursday')
             WES = weLst
            # ('Friday','Saturday','Sunday')
         
             vRt1 = zRate[i]
             logger.debug("Rate value is %s at %s",vRt1,i)
             vDow = DOW
             
             if i == 13:
                     if vDow == 'WD':
                         zsDF = inDF.query('(dow in @WDS) &  (ADR >= @vRt1)')
                     elif vDow == 'WE':
                         zsDF = inDF.query('(dow in @WES) &  (ADR >= @vRt1)')
                     elif vDow == 'All':
                         zsDF = inDF.query('(ADR >= @vRt1)')
                     else:
                         zsDF = inDF.query('(dow == @vDow) &  (ADR >= @vRt1)')
                         
                     j = len(zsDF.index) 
                     cnt += j 
                     zSale.append(np.sum(zsDF.Nights))
             else:
                     if i==0 :
                         vRt2 = 0
                     else:
                        vRt2 = zRate[i-1]

                     if vDow == 'WD':
                         zsDF = inDF.query('(dow in @WDS) & (ADR < @vRt1) & (ADR >= @vRt2)')
                     elif vDow == 'WE':
                         zsDF = inDF.query('(dow in @WES) & (ADR < @vRt1) & (ADR >= @vRt2)')
                     elif vDow == 'All':
                         zsDF = inDF.query('(ADR < @vRt1) & (ADR >= @vRt2)')
                     else:
                         zsDF = inDF.query('(dow == @vDow) & (ADR < @vRt1) & (ADR >= @vRt2)')

                     zSale.append(np.sum(zsDF.Nights))        
                     j = len(zsDF.index) 
                     cnt += j    
    else:
        zRate = 0
        zSale = 0
        cnt = 0
        
    logger.info("--- Number of rows - %s ---", cnt)
    logger.debug("vRt1 - %s", vRt1)
    logger.debug("vRt2 - %s", vRt2)
    logger.debug("zRate - %s",zRate)
    logger.debug("zSale - %s",zSale)
    return zRate, zSale, cnt


def direxists(pid):
    wdir = os.getcwd()
    wfl = wdir + "\\" + str(pid)
    logger.debug("Path to save the slope and intercept files - %s", wfl)
    if not os.path.exists(wfl):
        os.makedirs(wfl)
    return wfl


def mcap_percent(mcapData, hcap):
    logger.info("--- Percentiles for max capacity ---")
    if len(mcapData.index) < 30:
        p95 = hcap
        logger.debug("p95 - {}". format(p95))
    else:
        p95 = np.percentile(mcapData.no_of_rooms, 95)

        logger.info("--- Calculated Percentiles for max capacity ---")
        logger.debug("p95 - {}".format(p95))

    return p95
