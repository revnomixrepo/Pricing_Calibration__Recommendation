"""
Created on Sat Apr 15 12:25:34 2017
@author: sameer kulkarni
This is the wraper code to call the getRecommendations 
This wraper is called as RecommendationService
"""


import requests
import sys
import logging
import requests
import send_revseed_mail as srm
from sqlalchemy import create_engine
import linecache
import os
from config.log_config import ServiceLog as srl


def uploadRate(clientId):
    req = requests.put('https://revseed.revnomix.com/staah/outbound/updateRate?clientId='+str(clientId))

    if (req.status_code != 200):
        raise Exception('Failed uploaded rate to staah: https://revseed.revnomix.com/staah/outbound/updateRate?clientId={}', clientId)


def uploadRatestaah(id):
    uploadrate = requests.put("https://integrations.revnomix.com/staah/outbound/updateRate?clientId="+ str(id))
    status = uploadrate.status_code
    return f"Upload completed for clientId: {id} with status code: {status}"


def uploadRateEzee(id):
    uploadrate = requests.post("https://integrations.revnomix.com/ezee/inbound/updateRate?clientId="+ str(id))
    status = uploadrate.status_code
    return f"Upload completed for clientId: {id} with status code: {status}"


def uploadRatestaah_all(id):
    uploadrate = requests.get("https://integrations.revnomix.com/staahAll/outbound/updateRate?clientId="+ str(id))
    status = uploadrate.status_code
    return f"Upload completed for clientId: {id} with status code: {status}"


def uploadRateCM(id, cm=None):
    try:
        constr = 'mysql+mysqlconnector://revseed:revseed@123@13.232.233.63:3308/revseed'
        engine = create_engine(constr, echo=False, pool_pre_ping=True)
        cnx = engine.connect()
        result = cnx.execute("SELECT channel_manager, rate_push_enable FROM clients WHERE id = {} "
                             "and STATUS='active' AND run_recommendation='YES'".format(id)).fetchone()
    except Exception as E:
        return E
    if result is None:
        return 'rate push disable'
    if cm is None:
        cm = result[0]

    if result[1] == 1:
        if cm == "staah":
            status = uploadRatestaah(id)
        elif cm == "ezee":
            status = uploadRateEzee(id)
        elif cm == "staah All":
            status = uploadRatestaah_all(id)
        else:
            status = f'New cm: {result[1]}'
    else:
        status = 'rate push disable'

    return status


def updatestatus(id):
    try:
        constr = 'mysql+mysqlconnector://revseed:revseed@123@13.232.233.63:3308/revseed'
        engine = create_engine(constr, echo=False, pool_pre_ping=True)
        cnx = engine.connect()
        cnx.execute('update on_demand_status set allow_run= 0 where client_id in ({})'.format(id))
        return 'update status for :' + str(id)
    except Exception as E:
        return E


# if __name__== '__main__':
#     e = updatestatus(3)
#     print(e)


def RecommendationService(pid, start_date=None, date_range=None):

    import configparser
    import getRecommendations
    import datetime

    runDate = str(datetime.date.today().strftime("%d%b%Y"))
    logFileName = "RecomService_" + runDate + "_" + str(pid) + ".log"
    srlog = srl("Recom", logFileName)
    srlog.closelogger()
    srlog.servicelog()
    logger = logging.getLogger(f"Recomservicelog.{__name__}")

    # logging.basicConfig(level=logging.DEBUG,filename=logFileName)
    # logging.basicConfig(level=logging.debug,filename=logFileName)
    logger.info(f"start calculating rate recommendations for {pid}")
    logger.debug(f"start calculating rate recommendations for {pid}")

    configParser = configparser.RawConfigParser()
    # configFilePath = r'F:\SamsDrive\OneDrive\GitHub\Pricing_Calibration\src\setup.ini'
    configFilePath = os.path.join(os.path.dirname(__file__), 'setup.ini')

    try:
        logger.info("Reading setup file")
        configParser.read(configFilePath)
    except:
        logger.error('file not available')

    try:
        logger.info("Got credentials for DB connection")
        dbType = configParser.get('db-config', 'dbtype')
        hostName = configParser.get('db-config', 'rhost')
        userID = configParser.get('db-config', 'ruser')
        pWord = configParser.get('db-config', 'rpwd')
        dbName = configParser.get('db-config', 'rdb')
    except Exception as e:
        logger.error(e)
        logger.error(f'setup.ini file not found in path {configFilePath}')
        return "Failed"

    config = {'rhost': hostName, 'ruser': userID, 'rpwd': pWord, 'rdb': dbName, 'dbtype': dbType}

    if start_date is None:
        start_date = str(datetime.date.today().strftime("%Y-%m-%d"))

    logger.info("Start date to generate - %s", start_date)
    logger.debug("Start date to generate - %s", start_date)

    if date_range is None:
        date_range = 90

    logger.info("Date range to generate recommendation - %s", date_range)
    logger.debug(f"Date range to generate recommendation - {date_range}")

    try:
        logger.info("Generate the recommendations from %s for next %s ",start_date, date_range)
        update_len = getRecommendations.getRecommendations(config, pid=pid, start_date=start_date)
        return {"massage": 'Success', 'days': update_len}
    except Exception as E:
        # print(E)
        getRecommendations.print_exception()
        logger.exception("--- Unable to generate the recommendations --- error:{}".format(E))
        srm.send_alert_msg(pid, "Unable to generate the recommendations")
        return "Failed"

#
# if __name__ == '__main__':
#     pid = int(sys.argv[1])
#     start_date = sys.argv[2]
#     date_range = int(sys.argv[3])
#     RecommendationService(pid, start_date, date_range)
