# -*- coding: utf-8 -*-
"""
Created on Sat Apr 15 12:25:34 2017

@author: sameer kulkarni
"""

"""
This is the wraper code to call the getRecommendations 
This wraper is called as RecommendationService
"""

import sys
import os
import logging
import send_revseed_mail as srm
from sqlalchemy import create_engine
from config.log_config import ServiceLog as srl


def updatestatus(id):
    try:
        constr = 'mysql+mysqlconnector://revseed:revseed@123@13.232.233.63:3308/revseed'
        engine = create_engine(constr, echo=False, pool_pre_ping=True)
        cnx = engine.connect()
        cnx.execute('update on_demand_status set allow_run= 0 where client_id in ({})'.format(id))
        return 'update status for :' + str(id)
    except Exception as E:
        return E


def CalibrationService(pid):

    import configparser
    import getData
    import rcpalgo
    import datetime
    
    runDate = str(datetime.date.today().strftime("%d%b%Y"))
    logFileName = "CalibService_" + runDate + "_" + str(pid) + ".log"
    srlog = srl("Calbr", logFileName)
    srlog.closelogger()
    srlog.servicelog()
    logger = logging.getLogger(f"Calbrservicelog.{__name__}")
    
    # logging.basicConfig(level=logging.DEBUG,filename=logFileName)
    # logging.basicConfig(level=logging.INFO,filename=logFileName)
    logger.info("start Calibration")

    configParser = configparser.RawConfigParser()   
    # configFilePath = r'/home/ubuntu/airflow/Pricing_Calibration/setup.ini'
    configFilePath = os.path.join(os.path.dirname(__file__), 'setup.ini')


    try:
        logger.info("Reading setup file")
        configParser.read(configFilePath)
    except:
        logger.error('file not available')
    try:
        dbType = configParser.get('db-config', 'dbtype')
        hostName = configParser.get('db-config', 'rhost')
        userID = configParser.get('db-config', 'ruser')
        pWord = configParser.get('db-config', 'rpwd')
        dbName = configParser.get('db-config', 'rdb')
    except Exception as E:
        logger.error(E)
        logger.error(f'setup.ini file not found in path {configFilePath}')
        return 'Failed'

    config = {'rhost': hostName, 'ruser': userID, 'rpwd': pWord, 'rdb': dbName, 'dbtype': dbType}
    
    myquery = "SELECT capacity FROM clients WHERE id =:prid"
    # myquery = ("SELECT capacity FROM property_details WHERE id =:prid")
    try:
        logger.info("Get the Hotel Details")
        hCapacity = getData.getData(myquery=myquery, pid=pid, db_confg=config)
    except:
        logger.error("--- Unable to fetch Hotel Details ---")

    hCap = hCapacity.loc[0].values[0]
    logger.info("The Hotel Capacity - %s ",hCap)
    
    try:
        logger.info("--- Run Calibration ---")
        rcpalgo.rcpalgo(pid=pid, hCap=hCap,db_config=config)
        return 'Success'
    except Exception as E:
        rcpalgo.print_exception()
        logger.error('check error:{}'.format(E))
        logger.error("--- Unable to Run Calibration ---")
        srm.send_alert_msg(pid, "Unable to Run Calibrations")
        return "Failed"


if __name__ == '__main__':    
    pid = sys.argv[1]        
    CalibrationService(pid)
