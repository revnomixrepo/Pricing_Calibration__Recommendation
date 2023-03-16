#from log_config import logger
import logging.config

from flask import Flask
from flask_restful import Api
from gevent.pywsgi import WSGIServer

import config
from controller.calibration import CalibrationController
from controller.home import Home
from controller.recommendation import RecommendationController
from controller.revseed_hnf import HNFController

logging.config.fileConfig(fname='config/log.conf', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


logger.info(f'Starting app in {config.APP_ENV} environment')

app = Flask(__name__)
app.config.from_object('config')
api = Api(app)
api.add_resource(HNFController,"/api/HNF")
api.add_resource(RecommendationController,"/api/recommendation/<int:pid>")
api.add_resource(CalibrationController,"/api/calibration/<int:pid>")
api.add_resource(Home, "/")

if __name__ == "__main__":
    logger.info("App is running............")
    # app.run(port=config.PORT, debug=config.DEBUG, host='0.0.0.0')
    http_server = WSGIServer(('0.0.0.0', 5000), app)
    http_server.serve_forever()
    logger.info("Server is started ..............")
