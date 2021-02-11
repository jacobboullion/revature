# PROJECT 1

## Project Description

This project takes in large amounts of wikipedia page view and clickstream data in order to answer some questions and do analysis based on this data.It does this by using HQL and other technologies like MapReduce to effienctly read through the data. 

## Technologies Used

* Hive 
* YARN
* MapReduce
* HDFS
* DBeaver

## Features

List of features ready and TODOs for future development
* Loads data from local directory
* Makes tables for the csv data collected locally 
* Queries run on these tables and get information

To-do list:
* Take data from hdfs
* Simplify number of Queries

## Getting Started
   
(include git clone command)
(include all environment setup steps)
* git clone the repo
* install hdfs, yarn, and hive
* start hive server with command: hiveserver2
* You can connect to the server using a program like DBeaver or in beeline using: beeline -u jdbc:hive2://localhost:10000
* Once your connected you can start using the HQL queries in the provided script 
* The Wikipedia analytics information that is used as input to the programs can be found at: https://dumps.wikimedia.org/other/analytics/

## Usage

> You can run the queries like this (SELECT page_title, SUM(count_views) AS views FROM PAGEVIEW GROUP BY page_title ORDER BY VIEWS DESC LIMIT 20) after forming the tables and analyze the results from the tables stored in the hive warehouse.

## Contributors

## License
> No lincense
