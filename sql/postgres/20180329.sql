CREATE TABLE descriptions2 (
	description_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL, 
	description TEXT, 
	language TEXT);

ALTER SEQUENCE descriptions_id_sequence restart;
ALTER TABLE descriptions2 ALTER description_id SET DEFAULT NEXTVAL('descriptions_id_sequence');

INSERT INTO descriptions2 (record_id, description, language)
	SELECT record_id, description, language
	FROM descriptions;

DROP TABLE descriptions;

CREATE TABLE descriptions (
	description_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL, 
	description TEXT,
	description_hash VARCHAR(100), 
	language VARCHAR(5));

INSERT INTO descriptions (description_id, record_id, description, language)
	SELECT description_id, record_id, description, language
	FROM descriptions2;

ALTER TABLE descriptions ALTER description_id SET DEFAULT NEXTVAL('descriptions_id_sequence');

DROP TABLE descriptions2;

CREATE INDEX descriptions_by_description_hash on descriptions(description_hash);

