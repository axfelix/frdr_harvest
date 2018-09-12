CREATE TABLE affiliations (
	affiliation_id INTEGER PRIMARY KEY NOT NULL,
	affiliation TEXT);

CREATE TABLE records_x_affiliations (
	records_x_affiliations_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	affiliation_id INTEGER NOT NULL);

CREATE INDEX affiliations_by_affiliation on affiliations(affiliation);
CREATE INDEX records_x_affiliations_by_record on records_x_affiliations(record_id);
CREATE INDEX records_x_affiliations_by_affiliation on records_x_affiliations(affiliation_id);

