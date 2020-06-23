BEGIN TRANSACTION;

CREATE TABLE if NOT EXISTS temp (
	record_id INTEGER PRIMARY KEY NOT NULL,
	repository_id INTEGER NOT NULL,
	title TEXT,pub_date TEXT,
	modified_timestamp INTEGER DEFAULT 0,
	source_url TEXT,
	deleted NUMERIC DEFAULT 0,
	local_identifier TEXT,
	series TEXT,
	item_url TEXT,
	title_fr TEXT);

INSERT INTO temp SELECT record_id,
	repository_id,
	title,
	pub_date,
	modified_timestamp,
	source_url,
	deleted,
	local_identifier,
	series,
	item_url,
	title_fr
	FROM records;

DROP TABLE records;

ALTER TABLE temp RENAME TO records;

COMMIT;