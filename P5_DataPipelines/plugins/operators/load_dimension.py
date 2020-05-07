from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_stmt = """
    INSERT INTO {}
    {}
    ;
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 aws_credentials_id='',
                 sql_query = '',
                 table='',
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql_query = sql_query
        self.table = table
        self.truncate = truncate
        
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #truncate table if table already exists
        if self.truncate:
            redshift_hook.run('TRUNCATE TABLE {}'.format(self.table))
        
        #format and execute query
        formatted_sql = self.insert_stmt.format(
            self.table,
            self.sql_query
            )
        redshift_hook.run(formatted_sql)