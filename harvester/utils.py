class DBInterface:
	def __init__(self, params):
		self.dbtype = params.get('type', None)
		self.filename = params.get('filename', None)
		self.host = params.get('host', None)
		self.schema = params.get('schema', None)
		self.user = params.get('user', None)
		self.password = params.get('pass', None)
		self.connection = None
		if self.dbtype == "sqlite":
			self.sqlite3 = __import__('sqlite3')

	def getConnection(self):
		if self.dbtype == "sqlite":
			self.connection = self.sqlite3.connect(self.filename)
			return self.connection

	def getRow(self):
		if self.dbtype == "sqlite":
			return self.sqlite3.Row

class TimeFormatter:
	def __init__(self):
		self.nothing = None

	def humanize(self, amount):
		INTERVALS = [ 1, 60, 3600, 86400, 604800, 2629800, 31557600 ]
		NAMES = [('second', 'seconds'),	('minute', 'minutes'), ('hour', 'hours'),
			('day', 'days'), ('week', 'weeks'), ('month', 'months'), ('year', 'years')]
		result = ""
		amount = int(amount)

		for i in range(len(NAMES)-1, -1, -1):
			a = amount // INTERVALS[i]
			if a > 0: 
				result = result + str(a) + " " + str(NAMES[i][1 % a]) + " "
				amount -= a * INTERVALS[i]

		result = str.strip(result)
		if result == "":
			result = "0 seconds"
		return result