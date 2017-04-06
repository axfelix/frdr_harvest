import os
import logging
import sys
from logging.handlers import TimedRotatingFileHandler

class HarvestLogger:

	def __init__(self, params):
		self.logdir = os.path.dirname(params['filename'])
		if not os.path.exists(self.logdir):
			os.makedirs(self.logdir)

		self.handler = TimedRotatingFileHandler(
			params['filename'],
			when="D",
			interval=int(params['daysperfile']),
			backupCount=int(params['keep'])
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

	def debug(self, message):
		self.logger.debug(message)

	def info(self, message):
		self.logger.info(message)

	def error(self, message):
		self.logger.error(message)