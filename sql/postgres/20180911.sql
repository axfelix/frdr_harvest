CREATE TABLE records_x_affiliations (
	records_x_affiliations_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	affiliation_id INTEGER NOT NULL);

CREATE SEQUENCE IF NOT EXISTS records_x_affiliations_seq;
CREATE SEQUENCE IF NOT EXISTS affiliations_id_sequence;
ALTER TABLE records_x_affiliations ALTER records_x_affiliations_id SET DEFAULT NEXTVAL('records_x_affiliations_seq');

CREATE TABLE affiliations (
  affiliation_id INTEGER PRIMARY KEY NOT NULL,
  affiliation TEXT);

ALTER TABLE affiliations ALTER affiliation_id SET DEFAULT NEXTVAL('affiliations_id_sequence');
CREATE INDEX affiliations_by_affiliation on affiliations(affiliation);
CREATE INDEX records_x_affiliations_by_record on records_x_affiliations(record_id);
CREATE INDEX records_x_affiliations_by_affiliation on records_x_affiliations(affiliation_id);
