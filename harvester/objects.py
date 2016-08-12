class HarvestRepo:
	time = __import__('time')

	def __init__(self, params):
		self.url = params.get('url', None)
		self.type = params.get('type', 'oai')
		self.name = params.get('name', None)
		self.set = params.get('set', None)
		self.thumbnail = params.get('thumbnail', None)
		self.update_log_after_numitems = params.get('update_log_after_numitems', None)
		self.item_url_pattern = params.get('item_url_pattern', None)
		self.enabled = params.get('enabled', True)
		self.db = None
		self.enabled = False

	def setDefaults(self, defaults):
		for k,v in defaults.items():
			if isinstance(k, str) or isinstance(k, unicode):
				if k in self.__dict__:
					if not self.__dict__[k]:
						self.__dict__[k] = v
				else:
					self.__dict__[k] = v

	def setLogger(self, l):
		self.logger = l

	def setDatabase(self, d):
		self.db = d

	def setFormatter(self, f):
		self.formatter = f

	def get_data_from_db(self, column):
		returnvalue = False
		c = self.db.getConnection()
		with c:
			c.row_factory = self.db.getRow()
			litecur = c.cursor()
			records = litecur.execute("select " + column + " from repositories where repository_url = ?",(self.url,) ).fetchall()
			for record in records:
				returnvalue = record[column]
		return returnvalue

	def crawl(self):
		self.tstart = self.time.time()
		self.last_crawl = self.get_data_from_db("last_crawl_timestamp")

		if self.last_crawl == 0:
			self.logger.info("Repo: " + self.name + " (last harvested: never)" )
		else:
			self.logger.info("Repo: " + self.name + " (last harvested: %s ago)", self.formatter.humanize(self.tstart - self.last_crawl ) )

		if (self.enabled):
			if (self.last_crawl + self.repo_refresh_days*86400) < self.tstart:
				if self.type == "oai":
					self.oai_harvest_with_thumbnails()
				elif self.type == "ckan":
					self.ckan_get_package_list(repository)
				update_repo_last_crawl(repository)
			else:
				self.logger.info("This repo is not yet due to be harvested")
		else:
			self.logger.info("This repo is not enabled for harvesting")
