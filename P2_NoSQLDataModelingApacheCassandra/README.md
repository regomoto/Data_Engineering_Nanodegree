## OVERVIEW

This project required building an ETL pipleine that models user acticity data into Apache Cassandra as non-relational tables. These non-relational tables were designed to increase the performance of the database when running specific analytic queries. 

## Files in repository

__1) Project_1B_ Project_Template.ipynb:__ Notebook that executes the ETL pipeline. Also contains the analytic queries that these tables were based on. These queries were executed in order to verify that the data model and data pipeline were designed and executed correctly.

__2) event_data folder:__ Contains user log data for a single month in csv format. Each csv file in the folder represents a day of user activitiy. These files will be used to create an aggregated denormalized dataset that aggregates all of the days' data.

__3) event_data_new.csv:__ Aggregated dataset that was created from processing the log files in the event_data folder. This file is used to create the NoSQL tables.


