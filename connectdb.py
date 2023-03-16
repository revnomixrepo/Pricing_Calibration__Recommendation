# -*- coding: utf-8 -*-
"""
Created on Fri Dec 23 11:41:57 2016

@author: Sameer.Kulkarni
"""


def conectdb(db_confg):
    import logging
    import sqlalchemy
    from sqlalchemy import create_engine
    rhost = db_confg['rhost']
    ruser = db_confg['ruser']
    rpwd = db_confg['rpwd']
    rdb = db_confg['rdb']
    dbtype = db_confg['dbtype']
    logger = logging.getLogger(f'Recomservicelog.{__name__}')
    logger = logging.getLogger(f'Calbrservicelog.{__name__}')

    logger.info("--- Connecting to Database ---")
    logger.debug("=== DB Type - %s ===", dbtype)
    if dbtype == 'PGS':        
        # this is the setup for PostgreSQL as the production is on PostgreSQL
        import psycopg2
        constr = 'postgresql+psycopg2://' + ruser + ':' + rpwd + '@' + rhost + '/' + rdb        
    else:
        # this is the setup for MySQL as the research setup is on MySQL
        import mysql.connector
        constr = 'mysql+mysqlconnector://' + ruser + ':' + rpwd + '@' + rhost + '/' + rdb    

    logger.debug(constr)
    try:
        engine = create_engine(constr, echo=False, pool_pre_ping=True)
        cnx = engine.connect()
        logger.info("--- Connected to Database ---")
    except Exception as E:
        cnx = None
        logger.info("--- Failed to Connect to Database ---")
    
    return cnx
