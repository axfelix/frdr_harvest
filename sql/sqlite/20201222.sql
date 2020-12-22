BEGIN TRANSACTION;

CREATE TABLE if NOT EXISTS _temp (
	record_id INTEGER PRIMARY KEY NOT NULL,
	repository_id INTEGER NOT NULL,
	title TEXT,pub_date TEXT,
	modified_timestamp INTEGER DEFAULT 0,
	source_url TEXT,
	deleted INTEGER DEFAULT 0,
	local_identifier TEXT,
	series TEXT,
	item_url TEXT,
	title_fr TEXT,
	upstream_modified_timestamp INTEGER DEFAULT 0,
	geodisy_harvested INTEGER DEFAULT 0);

INSERT INTO _temp SELECT record_id,
	repository_id,
	title,
	pub_date,
	modified_timestamp,
	source_url,
	deleted,
	local_identifier,
	series,
	item_url,
	title_fr,
	upstream_modified_timestamp,
	geodisy_harvested
	FROM records;

DROP TABLE records;

ALTER TABLE _temp RENAME TO records;

DROP INDEX IF EXISTS records_by_modified_timestamp;
CREATE INDEX IF NOT EXISTS records_by_modified_timestamp ON records (modified_timestamp);

COMMIT;