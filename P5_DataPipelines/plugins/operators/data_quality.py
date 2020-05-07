from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Receive One or More SQL based tests and compare with 
    each of the test's expected results
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 aws_credentials_id='',
                 test_list = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.test_list = test_list

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id),
        
        """
        take in a list of data quality checks to test the data output
        user can insert many queries for this step in sql_quality_tests.py
        """
        #counter for while loop
        x = 0
        
        while x < len(self.test_list):
            #get the nth list item to test to get the first set of key value pairs
            input_test = test_list[x] 
            
            #get the sql query from the input list. This gets the query as a string
            quality_test = list({key:value for (key,value) in input_test.items()}.values())[0]
            
            #set up connections with redshift
            redshift_hook = PostgresHook(self.redshift_conn_id)
            
            #use the sql query string from line 41 and execute query
            quality_test_value = redshift_hook.get_records(quality_test)
            
            #get the expected value from the dictionary in the input list
            expected_val = list({key:value for (key,value) in input_test.items()}.values())[1]
            
            #compare the expected value against the result of the query
            if quality_test != expected_val:
            #raise error if data quality check fails
            #will let user know which number test failed and the details of the SQL code that raised the data quality failure
                raise ValueError('Data quality check failed. Check the data quality test ' + str(x+1) + ': ' + quality_test)
             
            x = x + 1