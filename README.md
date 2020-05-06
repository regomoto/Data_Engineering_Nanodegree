### PROJECT SUMMARY

Sparkify is a music streaming company that wants to enable their analytics team to continue finding insights in what songs their users are listening to.  

The user activity and songs metadata data from the app resides in JSON files in S3. 
The ETL pipeline will extract their data from S3, stage the data in Redshift, and transform data into a set of dimensional tables that the analytics team can query for insights. 
Putting this data in the cloud and utlizing a Redshift data warehosue structure allows for a scalable solution that will result in no onsite hosting cost and fast query times. 


### FILES IN REPOSITORY

__1) dwh.cfg:__  Contains file paths for the user activity and song data files in S3. Also contains information related to the cluster you will be running in AWS. 
Prior to running, the user should fill in their own information from their AWS accounts under the [IAM_role] and [Cluster] sections.

__2) create_tables.py:__ Creates fact and dimension tables for the star schema in Redshift.  

__3) etl.py:__ Extract data from the S3 bucket where the log and song files are located. Insert data into the dimension tables.  

__4) sql_queries:__ Contains SQL statements used in etl.py and create_tables.py.


### HOW TO RUN PYTHON SCRIPTS  

__Step 1:__ Fill empty variables in the dwh.cfg file with information from your own AWS account. Save the file.   

__Step 2:__ Open a terminal window. Run the create_tables.py script. Verify that script was run without errors.  

__Step3:__ Run the etl.py script.  