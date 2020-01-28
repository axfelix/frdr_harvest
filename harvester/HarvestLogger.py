import os
import logging
import sys
from logging.handlers import RotatingFileHandler
from harvester.BufferingSMTPHandler import BufferingSMTPHandler


class HarvestLogger:
    def __init__(self, params):
        self.logdir = os.path.dirname(params['filename'])
        if not os.path.exists(self.logdir):
            os.makedirs(self.logdir)

        self.handler = RotatingFileHandler(
            params.get('filename', 'logs/log.txt'),
            maxBytes=int(params.get('maxbytes', 10485760)),
            backupCount=int(params.get('keep', 7))
        )
        logFormatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        self.handler.setFormatter(logFormatter)
        self.logger = logging.getLogger("Rotating Log")
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.DEBUG)
        if 'level' in params:
            if (params['level'].upper() == "INFO"):
                self.logger.setLevel(logging.INFO)
            if (params['level'].upper() == "ERROR"):
                self.logger.setLevel(logging.ERROR)
        if 'console' in params and (params['console'].upper() == "TRUE"):
            self.logger.addHandler(logging.StreamHandler(sys.stdout))

        self.copyerrorstoemail = False
        self.previouserrorstate = False
        self.mailto = False
        self.mailfrom = False
        if 'copyerrorstoemail' in params and params.get("copyerrorstoemail").upper() == "TRUE":
            self.mailto = params.get("mailtoaddr")
            self.mailfrom = params.get("mailfromaddr")
            self.mailhost = params.get("mailhost", "localhost")
            self.mailsubject = params.get("mailsubject", "Error log")
            if self.mailto != "" and self.mailfrom != "":
                self.copyerrorstoemail = True
                self.previouserrorstate = True
                self.mailusessl = False
                if 'mailusessl' in params and params.get("mailusessl").upper() == "TRUE":
                    self.mailusessl = True
                self.mailauthuser = params.get("mailauthuser")
                self.mailauthpass = params.get("mailauthpass")
                self.mailHandler = BufferingSMTPHandler(self.mailhost, self.mailusessl, self.mailauthuser,
                                                        self.mailauthpass,
                                                        self.mailfrom, self.mailto, self.mailsubject, 200 * 1024,
                                                        logFormatter, self.logger)
                self.mailLogger = logging.getLogger("Email log")
                self.mailLogger.addHandler(self.mailHandler)
                self.mailLogger.setLevel(logging.ERROR)

    def setErrorsToEmail(self, newState):
        self.previouserrorstate = self.copyerrorstoemail
        self.copyerrorstoemail = newState

    def restoreErrorsToEmail(self):
        self.copyerrorstoemail = self.previouserrorstate

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)
        if self.copyerrorstoemail:
            self.mailLogger.error(message)
