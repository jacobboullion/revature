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


DROP TABLE pageview_usa;


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

SELECT * FROM clickstream
WHERE type='link'
ORDER BY n DESC;

--Question 3

SELECT * FROM clickstream
WHERE type='link' AND prev='Hotel_California'
ORDER BY n DESC;

SELECT * FROM clickstream
WHERE prev='Hotel_California'
ORDER BY n DESC;


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

