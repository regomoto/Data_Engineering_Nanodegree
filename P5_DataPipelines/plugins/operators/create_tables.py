from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):
    """
    This will read the create tables files
    and create tables needed for the data pipeline.
    """
    ui_color = '#358140'
    sql_code = 'create_tables.sql'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials='',
                 *args, **kwargs):
        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
    def execute(self,context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        file = open(CreateTablesOperator.sql_code, 'r')
        sql_file = file.read()
        
        #sql commands split with semi-colon
        sqlCommands = sqlFile.split(';')
        
        for command in sqlCommands:
            redshift.run(command)
            