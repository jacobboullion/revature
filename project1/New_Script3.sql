--HQL Hive Query Language

CREATE DATABASE student_db;

SHOW DATABASES;

USE student_db;

CREATE TABLE STUDENT (
	ssn STRING,
	first_name STRING,
	last_name STRING,
	age INT,
	state STRING,
	house STRING)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	TBLPROPERTIES("skip.header.line.count"="1");
	
DESCRIBE STUDENT;

LOAD DATA LOCAL INPATH '/home/jacob/student-house.csv' INTO TABLE STUDENT;

SELECT * FROM STUDENT;

DROP TABLE STUDENT;

--external table 

CREATE EXTERNAL TABLE STUDENT (
	ssn STRING,
	first_name STRING,
	last_name STRING,
	age INT,
	state STRING,
	house STRING)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ','
	LOCATION '/user/jacob/mydata'
	TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA INPATH '/user/jacob/student/student-house.csv' INTO TABLE STUDENT;

SELECT * FROM STUDENT
ORDER BY last_name DESC 
LIMIT 50;

SHOW TABLES;


INSERT OVERWRITE DIRECTORY '/user/hive/output'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '\t'
SELECT house, COUNT(house) FROM STUDENT 
GROUP BY house;



SELECT first_name, last_name, state FROM STUDENT 
WHERE UPPER(state) = 'VIRGINIA' OR UPPER(state) = 'CALIFORNIA';



SELECT first_name, ssn FROM STUDENT 
WHERE house = 'Hufflepuff' AND first_name LIKE 'C%';

SELECT house, ROUND(AVG(AGE), 2) FROM student 
GROUP BY house;


--managed tables 
CREATE TABLE student_state 
AS SELECT COUNT(*) AS Num_Students, state FROM student 
GROUP BY state;

SELECT * FROM student_state;


SELECT * FROM student;
CREATE TABLE STUDENT_AGE (
	ssn STRING,
	first_name STRING,
	last_name STRING,
	state STRING,
	house STRING
) PARTITIONED BY (age INT)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',';


INSERT INTO TABLE STUDENT_AGE PARTITION(age=33)
SELECT ssn, first_name, last_name, state, house FROM STUDENT WHERE age=33;


INSERT INTO TABLE STUDENT_AGE PARTITION(age=37)
SELECT ssn, first_name, last_name, state, house FROM STUDENT WHERE age=37;

INSERT INTO TABLE STUDENT_AGE PARTITION(age=23)
SELECT ssn, first_name, last_name, state, house FROM STUDENT WHERE age=23;


SELECT * FROM STUDENT_AGE;

SELECT * FROM STUDENT


SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;


INSERT INTO TABLE STUDENT_AGE PARTITION(age)
SELECT ssn, first_name, last_name, state, house, age FROM STUDENT;


ALTER TABLE STUDENT_AGE DROP PARTITION(age=32);


CREATE TABLE student_house (
	ssn STRING,
	first_name STRING,
	last_name STRING,
	age INT,
	state STRING
)
PARTITIONED BY (house STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
TBLPROPERTIES("skip.header.line.count"="1");


--LOAD DATA INPATH '/user/jacob/mydata/student-house.csv' INTO TABLE student_house;

INSERT INTO TABLE student_house PARTITION(house)
SELECT ssn, first_name, last_name, age, state, house FROM student;

SELECT * FROM student_house;


--bucketing 
SET hive.enforce.bucketing = true;


CREATE TABLE student_age_buckets (
	ssn STRING,
	first_name STRING,
	last_name STRING,
	age INT,
	state STRING,
	house STRING 
)
CLUSTERED BY (age) INTO 4 BUCKETS 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',';

INSERT INTO student_age_buckets
SELECT ssn, first_name, last_name, age, state, house FROM student;


CREATE TABLE student_partition_buckets (
	ssn STRING,
	first_name STRING,
	last_name STRING,
	age INT,
	state STRING
)
PARTITIONED BY (house STRING)
CLUSTERED BY (age) INTO 4 BUCKETS 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',';

INSERT INTO student_partition_buckets PARTITION(house)
SELECT ssn, first_name, last_name, age, state, house FROM student;


CREATE TABLE house_buckets (
	ssn STRING,
	first_name STRING,
	last_name STRING,
	age INT,
	state STRING,
	house STRING
)
CLUSTERED BY (house) INTO 3 BUCKETS 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',';


INSERT INTO house_buckets
SELECT ssn, first_name, last_name, age, state, house FROM student;


---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
--Project 1----------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------

USE testdb;

--Question 1
CREATE TABLE PAGEVIEW (
	domain_code STRING,
	page_title STRING,
	count_views INT,
	total_response_size INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
TBLPROPERTIES("skip.header.line.count"="1");


LOAD DATA LOCAL INPATH '/home/jacob/pageview' INTO TABLE PAGEVIEW;

SELECT page_title, SUM(count_views) AS views 
FROM PAGEVIEW
GROUP BY page_title
ORDER BY views DESC
LIMIT 20;


DROP TABLE clickstream;


--Question 2
CREATE TABLE CLICKSTREAM (
	prev STRING,
	curr STRING,
	type STRING,
	n INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/jacob/dec-clickstream' INTO TABLE CLICKSTREAM;

SELECT * FROM clickstream;


SELECT prev, SUM(n) AS clicks
FROM clickstream
WHERE type='link'
GROUP BY prev
ORDER BY clicks DESC;

SELECT type, SUM(n)
FROM clickstream 
WHERE prev = '2020_United_States_presidential_election'
GROUP BY type;

SELECT page_title, SUM(count_views)*31 AS views 
FROM PAGEVIEW
WHERE page_title = '2020_United_States_presidential_election'
GROUP BY page_title
ORDER BY views DESC;


--Question 3

--total page views 
SELECT page_title, SUM(count_views)*31 AS views 
FROM PAGEVIEW
WHERE page_title = 'Hotel_California' OR page_title = 'Hotel_California_(Eagles_album)' 
OR page_title = 'Don_Felder' OR page_title = 'Eagles_(band)' OR page_title = 'Don_Henley'  
GROUP BY page_title
ORDER BY views DESC;

SELECT page_title, SUM(count_views)*31 AS views 
FROM PAGEVIEW
WHERE page_title = 'The_Long_Run_(album)' OR page_title = 'Bernie_Leadon' 
OR page_title = 'Lois_Chiles' OR page_title = 'Eagles_(band)' OR page_title = 'Don_Henley' OR page_title = 'Glenn_Frey'   
GROUP BY page_title
ORDER BY views DESC;

--The_Long_Run_(album), Bernie_Leadon, Glenn_Frey or Don_Henley, Eagles_(band) or Lois_Chiles

SELECT SUM(n)
FROM clickstream 
WHERE curr = 'Hotel_California';

--starting at hotel California
SELECT * FROM clickstream
WHERE type='link' AND prev='Don_Henley'
ORDER BY n DESC
limit 5;

SELECT * FROM clickstream
WHERE prev='Hotel_California_(Eagles_album)'
ORDER BY n DESC;

-- All links from third fraction
SELECT prev, SUM(n)
FROM clickstream 
WHERE type = 'link' AND (prev = 'The_Long_Run_(album)' OR prev = 'Bernie_Leadon' 
OR prev = 'Lois_Chiles' OR prev = 'Eagles_(band)' OR prev = 'Don_Henley' OR prev = 'Glenn_Frey')  
GROUP BY prev;


--Question 4
--The USA
CREATE TABLE PAGEVIEW_8_to_9 (
	domain_code STRING,
	page_title STRING,
	count_views INT,
	total_response_size INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/jacob/pageview_us' INTO TABLE pageview_8_to_9;

-- Other Countries
CREATE TABLE PAGEVIEW_ELSEWHERE (
	domain_code STRING,
	page_title STRING,
	count_views INT,
	total_response_size INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/jacob/pageview_other' INTO TABLE pageview_elsewhere;

DROP TABLE PAGEVIEW_10_to_7;


SELECT page_title, SUM(count_views) AS views_us
FROM pageview_8_to_9 
GROUP BY page_title
ORDER BY views_us DESC
LIMIT 20;

SELECT page_title, SUM(count_views) AS views_other
FROM pageview_elsewhere 
GROUP BY page_title
ORDER BY views_other DESC
LIMIT 20;


--Question 5
CREATE TABLE REVISION (
	wiki_db	string,
	event_entity string,	
	event_type	string,	
	event_timestamp	string,
	event_comment	string,
	event_user_id	bigint,
	event_user_text_historical	string,	
	event_user_text	string,
	event_user_blocks_historical string,
	event_user_blocks string,	
	event_user_groups_historical string,
	event_user_groups	string,	
	event_user_is_bot_by_historical	string,	
	event_user_is_bot_by string,	
	event_user_is_created_by_self boolean,	
	event_user_is_created_by_system	boolean,	
	event_user_is_created_by_peer boolean,
	event_user_is_anonymous	boolean,
	event_user_registration_timestamp string,
	event_user_creation_timestamp string,
	event_user_first_edit_timestamp	string,
	event_user_revision_count bigint,	
	event_user_seconds_since_previous_revision bigint,
	page_id	bigint,	
	page_title_historical string,	
	page_title string,	
	page_namespace_historical int,	
	page_namespace_is_content_historical boolean,	
	page_namespace int,	
	page_namespace_is_content boolean,
	page_is_redirect	boolean,	
	page_is_deleted	boolean,	
	page_creation_timestamp	string,	
	page_first_edit_timestamp	string,	 
	page_revision_count	bigint,	
	page_seconds_since_previous_revision bigint,	
	user_id	bigint,	
	user_text_historical string,	
	user_text string,
	user_blocks_historical	string,	
	user_blocks	string,
	user_groups_historical	string,	
	user_groups	string,	
	user_is_bot_by_historical	string,
	user_is_bot_by	string,
	user_is_created_by_self	boolean,	
	user_is_created_by_system	boolean,	
	user_is_created_by_peer	boolean,	
	user_is_anonymous	boolean,	
	user_registration_timestamp	string,	
	user_creation_timestamp	string,
	user_first_edit_timestamp	string,
	revision_id	bigint,	
	revision_parent_id	bigint,	
	revision_minor_edit	boolean,	
	revision_deleted_parts	string,	
	revision_deleted_parts_are_suppressed	boolean,	
	revision_text_bytes	bigint,	
	revision_text_bytes_diff	bigint,	
	revision_text_sha1	string,	
	revision_content_model	string,	
	revision_content_format	string,	
	revision_is_deleted_by_page_deletion boolean,
	revision_deleted_by_page_deletion_timestamp	string,	
	revision_is_identity_reverted	boolean,	
	revision_first_identity_reverting_revision_id	bigint,	
	revision_seconds_to_identity_revert	bigint,	
	revision_is_identity_revert	boolean,	
	revision_is_from_before_page_creation boolean,
	revision_tags string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';



LOAD DATA LOCAL INPATH '/home/jacob/revision/2020-12.enwiki.2020-12.tsv' INTO TABLE revision;

DROP TABLE revision;

SELECT * FROM revision;

--testing table 
SELECT event_entity, event_type, event_timestamp, event_user_id, page_title, page_revision_count,
ROUND(page_seconds_since_previous_revision/60, 2) AS min_since_prev_revision,
revision_is_identity_reverted, ROUND(revision_seconds_to_identity_revert/60, 2) AS minutes_to_revert
FROM revision
--WHERE page_title = 'Cher'
WHERE event_entity = 'revision' AND revision_is_identity_reverted = true
ORDER BY page_revision_count DESC;

--get the total number of times a page was revised
SELECT page_title, COUNT(page_title) AS count_title , ROUND(SUM(revision_seconds_to_identity_revert)/60, 2) as minutes_to_revert
FROM revision
WHERE revision_is_identity_reverted = true
GROUP BY page_title 
ORDER BY count_title DESC;

--individual time to revert page
SELECT page_title, event_timestamp, ROUND(revision_seconds_to_identity_revert/60, 2)
FROM revision
WHERE revision_is_identity_reverted = true AND (page_title = 'Teahouse' OR page_title = 'Sandbox' OR page_title = 'Elliot_Page')


SELECT page_title, SUM(count_views) AS views 
FROM PAGEVIEW
WHERE page_title = 'Teahouse' OR page_title = 'Sandbox' OR page_title = 'Elliot_Page'
GROUP BY page_title;


--question 6: what was the most viewed article this past month that started with A and ended with A

SELECT page_title, SUM(count_views) AS views 
FROM PAGEVIEW
WHERE page_title LIKE 'a%a' OR page_title LIKE 'A%a'
GROUP BY page_title
ORDER BY views DESC
LIMIT 5;





