## PROJECT SUMAMRY
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.



## FILE STRUCTURE

__1) dags:__ This folder contains the dag file that defines imports, tasks, and task dependencies and a set of SQL queries used to create tables. 
- _udac_example_dag.py_: File containst DAG definition with imports, tasks, and task dependencies.
- _create_tables.sql_: Containts SQL code that will be read using a Postgres Operator

__2) plugins:__ This folder contains operators that create and load necessary tables, stage data in Redshift, and run data quality checks. It also contains supporting SQL code needed to accomplish these operations.
- _StageToRedshiftOperator (stage_redshift.py):_ Copies data from S3 and loads them into Redshift staging tables
- _LoadFactOperator (load_fact.py):_ Loads data into Redshift fact table from the staged tables
- _LoadDimensionsOperator (load_dimension.py):_ Loads data into Redshift dimension tables from the staged tables
- _DataQualityOperator (data_quality.py):_ Takes user defined data quality tests (as a SQL query) and checks the output from the data pipeline using a user-defined expected value. The data quality tests are in the sql_quality_tests.py file in the SqLQualityTests class. Users can update the SqlQualitiyTests class with any data quality tests they want to run and the expected value of the test. Currently there are 2 data quality tests:
 - Check if the tables have greater than one row. The expected value for the pipleine is 0.
 - Check if the start_time column has any null values. The expected value for the pipeline is 0. 



## HOW TO RUN

Step 1: Set up the Airflow environment. See 'Airflow Setup' section below for details

Step 2: Update the 'sql_quality_tests.py' with SQL queries you want to run as data quality tests and the expected value of these tests. After making these changes, also update the test_list variable. test_list is a list of dictionaries that contains all data quality tests and their respective expected values.

#### Airflow Setup

My project was run in a Udacity workspace with a pre-loaded Airflow environment. However, you can also run Airflow locally. Running Airflow locally requires the following steps:

1) Install Airflow, create variable AIRFLOW_HOME and AIRFLOW_CONFIG with the appropiate paths, and place dags and plugins on airflow_home directory.

2) Initialize Airflow data base with airflow initdb, and open webserver with airflow webserver

3) Access the server http://localhost:8080 and create AWS and Redshift connections

AWS Connection: 

- Conn Id: Enter 'aws_credentials'
- Conn Type: Enter 'Amazon Web Services'
- Login: Enter your Access key ID from the AWS IAM user credentials
- Password: Enter your Secret access key from the AWS IAM credentials

Redshift Connection:
- Conn Id: Enter 'redshift'
- Conn Type: Enter 'Postgres'
- Host: Enter the endpoint of your redshift cluster
- Schema: Redshift database
- Login: awsuser (or the masterusername used in your AWS account)
- Password: Password created when launching Redshift cluster
- Port: 5439