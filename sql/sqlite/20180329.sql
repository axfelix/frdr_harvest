CREATE TABLE descriptions2 (
	description_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL, 
	description TEXT, 
	language TEXT);

INSERT INTO descriptions2 (record_id, description, language)
	SELECT record_id, description, language
	FROM descriptions;

DROP TABLE descriptions;

CREATE TABLE descriptions (
	description_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL, 
	description TEXT,
	description_hash TEXT, 
	language TEXT);

INSERT INTO descriptions (description_id, record_id, description, language)
	SELECT description_id, record_id, description, language
	FROM descriptions2;

DROP TABLE descriptions2;

CREATE INDEX descriptions_by_description_hash on descriptions(description_hash);

vacuum;