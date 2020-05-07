from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from boto3 import Session

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    #make certain keys templatable. use context variables to render
    #these templates. needs to render out this key before being passed
    #into operator
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT as json '{}'
        ;
        """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region = "",
                 copy_json_option="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.copy_json_option = copy_json_option 
        

    def execute(self, context):
        """
        The execute function will copy data from S3 and move it into staging tables
        """
        #get credentials and setup connection
        
        #self.s3 = S3Hook(aws_conn_id=self.aws_credentials, verify=False)
        #credentials = self.s3.get_credentials()
        
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        #session = Session()
        #credentials = session.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Connected with " + self.redshift_conn_id)
        
        #delete table if exists
        self.log.info("Clearing data from destination table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        #copy data from S3
        self.log.info("Copying data from S3")
        #pass in context to to s3_key to render it
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        #move into staging tables
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.copy_json_option
        )
        redshift.run(formatted_sql)
        
        #log a successful copy
        self.log.info(f"Successfully copied {self.table} to Redshift")
  


