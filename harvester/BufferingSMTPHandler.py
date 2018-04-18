import logging
from logging.handlers import BufferingHandler
import smtplib
from email.mime.text import MIMEText

class BufferingSMTPHandler(logging.handlers.BufferingHandler):
	def __init__(self, mailhost, usessl, authuser, authpass, fromaddr, toaddrs, subject, capacity, logFormatter, alt_logger):
		logging.handlers.BufferingHandler.__init__(self, capacity)
		self.mailhost   = mailhost
		self.usessl     = usessl
		self.authuser   = authuser
		self.authpass   = authpass
		self.fromaddr   = fromaddr
		self.toaddrs    = toaddrs
		self.subject    = subject
		self.alt_logger = alt_logger
		self.setFormatter(logFormatter)

	def flush(self):
		if len(self.buffer) > 0:
			try:
				if isinstance(self.toaddrs, list):  # If toaddrs is a list, then join them as a string
					toaddrs = ','.join(self.toaddrs)
				else:
					toaddrs = self.toaddrs
				msg = ""
				for record in self.buffer:
					s = self.format(record)
					msg = msg + s + "\r\n"
				mimeMessage = MIMEText(msg.encode('utf-8'), _charset='utf-8')
				mimeMessage['Subject'] = self.subject
				mimeMessage['From'] = self.fromaddr
				mimeMessage['To'] = toaddrs

				port = 25
				if self.usessl:
					port = 587
				smtp = smtplib.SMTP(self.mailhost, port)
				smtp.ehlo()
				if self.usessl:
					smtp.starttls()
					smtp.ehlo()
				if self.authuser and self.authpass:
					smtp.login(self.authuser, self.authpass)
				smtp.sendmail(self.fromaddr, toaddrs, mimeMessage.as_string())
				smtp.quit()

			except Exception as e:
				self.alt_logger.error("Trying to send log via email: {}".format(e))
				#self.handleError(None)
			self.buffer = []