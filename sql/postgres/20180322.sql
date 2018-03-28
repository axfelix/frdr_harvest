CREATE TABLE subjects2 (
	subject_id INTEGER PRIMARY KEY NOT NULL,
	subject TEXT);

ALTER SEQUENCE subjects_id_sequence restart;
ALTER TABLE subjects2 ALTER subject_id SET DEFAULT NEXTVAL('subjects_id_sequence');

INSERT INTO subjects2 (subject)
	SELECT Distinct o.subject
	FROM subjects as o
	WHERE o.subject Is Not Null
	AND o.subject Not In (SELECT subject from subjects2);

CREATE TABLE records_x_subjects (
	records_x_subjects_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	subject_id INTEGER NOT NULL);

CREATE SEQUENCE IF NOT EXISTS records_x_subjects_seq;
ALTER TABLE records_x_subjects ALTER records_x_subjects_id SET DEFAULT NEXTVAL('records_x_subjects_seq');

INSERT INTO records_x_subjects (record_id, subject_id)
	SELECT distinct record_id, subjects2.subject_id
	FROM subjects
	JOIN subjects2 ON subjects.subject = subjects2.subject
	WHERE subjects.subject IS NOT NULL;

DROP TABLE subjects;

CREATE TABLE subjects (
	subject_id INTEGER PRIMARY KEY NOT NULL,
	subject TEXT);

INSERT INTO subjects (subject_id, subject)
	SELECT subject_id, subject
	FROM subjects2;

ALTER TABLE subjects ALTER subject_id SET DEFAULT NEXTVAL('subjects_id_sequence');

DROP TABLE subjects2;

CREATE INDEX subjects_by_subject on subjects(subject);
CREATE INDEX records_x_subjects_by_record on records_x_subjects(record_id);
CREATE INDEX records_x_subjects_by_subject on records_x_subjects(subject_id);

DELETE from records_x_subjects where record_id not in (select record_id from records);
DELETE from subjects where subject_id not in (select subject_id from records_x_subjects);

CREATE TABLE creators2 (
	creator_id INTEGER PRIMARY KEY NOT NULL,
	creator TEXT);

ALTER SEQUENCE creators_id_sequence restart;
ALTER TABLE creators2 ALTER creator_id SET DEFAULT NEXTVAL('creators_id_sequence');

INSERT INTO creators2 (creator)
	SELECT Distinct o.creator
	FROM creators as o
	WHERE o.creator IS NOT NULL
	AND o.creator Not In (SELECT creator from creators2);

CREATE TABLE records_x_creators (
	records_x_creators_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	creator_id INTEGER NOT NULL,
	is_contributor INTEGER NOT NULL);

CREATE SEQUENCE IF NOT EXISTS records_x_creators_seq;
ALTER TABLE records_x_creators ALTER records_x_creators_id SET DEFAULT NEXTVAL('records_x_creators_seq');

INSERT INTO records_x_creators (record_id, creator_id, is_contributor)
	SELECT distinct record_id, creators2.creator_id, is_contributor
	FROM creators
	JOIN creators2 ON creators.creator = creators2.creator
	WHERE creators.creator IS NOT NULL;

DROP TABLE creators;

CREATE TABLE creators (
	creator_id INTEGER PRIMARY KEY NOT NULL,
	creator TEXT);

INSERT INTO creators (creator_id, creator)
	SELECT creator_id, creator
	FROM creators2;

ALTER TABLE creators ALTER creator_id SET DEFAULT NEXTVAL('creators_id_sequence');

DROP TABLE creators2;

CREATE INDEX creators_by_creator on creators(creator);
CREATE INDEX records_x_creators_by_record on records_x_creators(record_id);
CREATE INDEX records_x_creators_by_creator on records_x_creators(creator_id);

DELETE from records_x_creators where record_id not in (select record_id from records);
DELETE from creators where creator_id not in (select creator_id from records_x_creators);

CREATE TABLE publishers2 (
	publisher_id INTEGER PRIMARY KEY NOT NULL,
	publisher TEXT);

ALTER SEQUENCE publishers_id_sequence restart;
ALTER TABLE publishers2 ALTER publisher_id SET DEFAULT NEXTVAL('publishers_id_sequence');

INSERT INTO publishers2 (publisher)
	SELECT Distinct o.publisher
	FROM publishers as o
	WHERE o.publisher Is Not Null
	AND o.publisher Not In (SELECT publisher from publishers2);

CREATE TABLE records_x_publishers (
	records_x_publishers_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	publisher_id INTEGER NOT NULL);

CREATE SEQUENCE IF NOT EXISTS records_x_publishers_seq;
ALTER TABLE records_x_publishers ALTER records_x_publishers_id SET DEFAULT NEXTVAL('records_x_publishers_seq');

INSERT INTO records_x_publishers (record_id, publisher_id)
	SELECT distinct record_id, publishers2.publisher_id
	FROM publishers
	JOIN publishers2 ON publishers.publisher = publishers2.publisher
	WHERE publishers.publisher IS NOT NULL;

DROP TABLE publishers;

CREATE TABLE publishers (
	publisher_id INTEGER PRIMARY KEY NOT NULL,
	publisher TEXT);

INSERT INTO publishers (publisher_id, publisher)
	SELECT publisher_id, publisher
	FROM publishers2;

ALTER TABLE publishers ALTER publisher_id SET DEFAULT NEXTVAL('publishers_id_sequence');

DROP TABLE publishers2;

CREATE INDEX publishers_by_publisher on publishers(publisher);
CREATE INDEX records_x_publishers_by_record on records_x_publishers(record_id);
CREATE INDEX records_x_publishers_by_publisher on records_x_publishers(publisher_id);

DELETE from records_x_publishers where record_id not in (select record_id from records);
DELETE from publishers where publisher_id not in (select publisher_id from records_x_publishers);

CREATE TABLE access2 (
	access_id INTEGER PRIMARY KEY NOT NULL,
	access TEXT);

ALTER SEQUENCE access_id_sequence restart;
ALTER TABLE access2 ALTER access_id SET DEFAULT NEXTVAL('access_id_sequence');

INSERT INTO access2 (access)
	SELECT Distinct o.access
	FROM access as o
	WHERE o.access Is Not Null
	AND o.access Not In (SELECT access from access2);

CREATE TABLE records_x_access (
	records_x_access_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	access_id INTEGER NOT NULL);

CREATE SEQUENCE IF NOT EXISTS records_x_access_seq;
ALTER TABLE records_x_access ALTER records_x_access_id SET DEFAULT NEXTVAL('records_x_access_seq');

INSERT INTO records_x_access (record_id, access_id)
	SELECT distinct record_id, access2.access_id
	FROM access
	JOIN access2 ON access.access = access2.access
	WHERE access.access IS NOT NULL;

DROP TABLE access;

CREATE TABLE access (
	access_id INTEGER PRIMARY KEY NOT NULL,
	access TEXT);

INSERT INTO access (access_id, access)
	SELECT access_id, access
	FROM access2;

ALTER TABLE access ALTER access_id SET DEFAULT NEXTVAL('access_id_sequence');

DROP TABLE access2;

CREATE INDEX access_by_access on access(access);
CREATE INDEX records_x_access_by_record on records_x_access(record_id);
CREATE INDEX records_x_access_by_access on records_x_access(access_id);

DELETE from records_x_access where record_id not in (select record_id from records);
DELETE from access where access_id not in (select access_id from records_x_access);

CREATE TABLE tags2 (
	tag_id INTEGER PRIMARY KEY NOT NULL,
	tag TEXT, 
	language TEXT);

ALTER SEQUENCE tags_id_sequence restart;
ALTER TABLE tags2 ALTER tag_id SET DEFAULT NEXTVAL('tags_id_sequence');

INSERT INTO tags2 (tag, language)
	SELECT Distinct o.tag, o.language
	FROM tags as o
	WHERE o.tag Is Not Null
	AND o.tag Not In (SELECT tag from tags2);

CREATE TABLE records_x_tags (
	records_x_tags_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	tag_id INTEGER NOT NULL);

CREATE SEQUENCE IF NOT EXISTS records_x_tags_seq;
ALTER TABLE records_x_tags ALTER records_x_tags_id SET DEFAULT NEXTVAL('records_x_tags_seq');

INSERT INTO records_x_tags (record_id, tag_id)
	SELECT distinct record_id, tags2.tag_id
	FROM tags
	JOIN tags2 ON tags.tag = tags2.tag AND tags.language = tags2.language
	WHERE tags.tag IS NOT NULL;

DROP TABLE tags;

CREATE TABLE tags (
	tag_id INTEGER PRIMARY KEY NOT NULL,
	tag TEXT, 
	language TEXT);

INSERT INTO tags (tag_id, tag, language)
	SELECT tag_id, tag, language
	FROM tags2;

ALTER TABLE tags ALTER tag_id SET DEFAULT NEXTVAL('tags_id_sequence');

DROP TABLE tags2;

CREATE INDEX tags_by_tag on tags(tag);
CREATE INDEX records_x_tags_by_record on records_x_tags(record_id);
CREATE INDEX records_x_tags_by_tag on records_x_tags(tag_id);

DELETE from records_x_tags where record_id not in (select record_id from records);
DELETE from tags where tag_id not in (select tag_id from records_x_tags);

CREATE TABLE rights2 (
	rights_id INTEGER PRIMARY KEY NOT NULL,
	rights TEXT);

ALTER SEQUENCE rights_id_sequence restart;
ALTER TABLE rights2 ALTER rights_id SET DEFAULT NEXTVAL('rights_id_sequence');

INSERT INTO rights2 (rights)
	SELECT Distinct o.rights
	FROM rights as o
	WHERE o.rights Is Not Null
	AND o.rights Not In (SELECT rights from rights2);

CREATE TABLE records_x_rights (
	records_x_rights_id INTEGER PRIMARY KEY NOT NULL,
	record_id INTEGER NOT NULL,
	rights_id INTEGER NOT NULL);

CREATE SEQUENCE IF NOT EXISTS records_x_rights_seq;
ALTER TABLE records_x_rights ALTER records_x_rights_id SET DEFAULT NEXTVAL('records_x_rights_seq');

INSERT INTO records_x_rights (record_id, rights_id)
	SELECT distinct record_id, rights2.rights_id
	FROM rights
	JOIN rights2 ON rights.rights = rights2.rights
	WHERE rights.rights IS NOT NULL;

DROP TABLE rights;

CREATE TABLE rights (
	rights_id INTEGER PRIMARY KEY NOT NULL,
	rights TEXT,
	rights_hash VARCHAR(100));

INSERT INTO rights (rights_id, rights)
	SELECT rights_id, rights
	FROM rights2;

ALTER TABLE rights ALTER rights_id SET DEFAULT NEXTVAL('rights_id_sequence');

DROP TABLE rights2;

CREATE INDEX rights_by_right_hash on rights(rights_hash);
CREATE INDEX records_x_rights_by_record on records_x_rights(record_id);
CREATE INDEX records_x_rights_by_right on records_x_rights(rights_id);

DELETE from records_x_rights where record_id not in (select record_id from records);
DELETE from rights where rights_id not in (select rights_id from records_x_rights);

