### PROJECT SUMMARY

A music streaming startup, Sparkify, has grown their user base and song database. Now they want to move their data warehouse to a data lake. 

We will build an ELT pipelines which will extract the JSON files from S3, process the data using Spark, and load the data back into S3 as a set of dimensional tables in parquet format.

We can use this new data lake infrastructure to perform analytics queries on a cloud platform and using a big data technology like Spark to process more information at a much faster rate.


### FILES IN REPOSITORY

__1) dl.cfg:__ Contains AWS credentials. Need to fill in values for 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY" with your own access key and secret key from AWS.

__2) etl.py:__ ETL script that extracts the data from S3, processes the data in Spark, and loads the transformed tables back into S3 as parquet files

__3) test.ipynb:__ Python notebook to test pipeline on a smaller subset of data prior to executing on the entire dataset.


### HOW TO RUN THE PROGRAM:

__Step 1:__ When running in local mode, create a config file with your own amazon access key and secret key in the root of the project. (If you are running this in an EMR cluster, this is not needed)  

__Step 2:__ In AWS, create an S3 bucket. Make sure the correct level of access is attached so that you can write your data to the bucket.  

__Step 3:__ Open a command line interface and run the etl script by entering the following command:  'python etl.py'



