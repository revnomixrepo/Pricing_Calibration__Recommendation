# -*- coding: utf-8 -*-
"""
Created on Fri Dec 23 11:41:59 2016

@author: Sameer.Kulkarni
"""


def loadData(df, table_name, db_confg):
    import sqlalchemy as sa
    import pandas as pd
    import connectdb
    import logging

    logger = logging.getLogger(f'Recomservicelog.{__name__}')
    # logger = logging.getLogger(f'Calbrservicelog.{__name__}')

    logger.info("--- Connecting to the Database ---")
    cnx = connectdb.conectdb(db_confg)

    logger.info("--- Loading Data into Database ---")

    df.to_sql(name=table_name, con=cnx, if_exists='append', index=False, index_label='id')
    cnx.close()
    logger.info("--- Loaded Data into Database ---")

    return None
