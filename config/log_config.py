import logging

# Create a custom logger
logger = logging.getLogger(__name__)

# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('logs/observer.log')
c_handler.setLevel(logging.WARNING)
f_handler.setLevel(logging.ERROR)

# Create formatters and add it to handlers
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)


class ServiceLog:
    def __init__(self, service, filename):
        self.filename = filename
        self.service = service

    def servicelog(self):
        # Create a custom logger
        servicelogger = logging.getLogger(f'{self.service}servicelog')
        servicelogger.setLevel(logging.INFO)

        # Create handlers
        sl_handler = logging.FileHandler(f'logs/{self.filename}')
        f_handler.setLevel(logging.INFO)

        # Create formatters and add it to handlers
        sl_format = logging.Formatter(f'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        sl_handler.setFormatter(sl_format)

        # Add handlers to the logger
        servicelogger.addHandler(sl_handler)

    def closelogger(self):
        log = logging.getLogger(f'{self.service}servicelog')
        x = list(log.handlers)
        for i in x:
            log.removeHandler(i)
            i.flush()
            i.close()



