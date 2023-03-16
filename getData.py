# -*- coding: utf-8 -*-
"""
Created on Fri Dec 23 11:41:59 2016

@author: Sameer.Kulkarni
"""


def getData(myquery, pid, db_confg):
    import sqlalchemy as sa
    import pandas as pd
    import connectdb
    import logging

    logger = logging.getLogger(f'Recomservicelog.{__name__}')
    # logger = logging.getLogger(f'Calbrservicelog.{__name__}')

    n = 10
    logger.info("--- Connecting to the Database ---")
    cnx = connectdb.conectdb(db_confg)

    logger.info("--- Get all raw data ---")
        
    df = pd.read_sql(sa.text(myquery), cnx, params={'prid': pid})
    cnx.close()
    logger.info("--- Got all raw data ---")
    logger.debug(df.head(n))
    
    return df
