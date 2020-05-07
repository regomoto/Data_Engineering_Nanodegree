class SqlQualityTests:
    """
    test_list will be passed into the data quality test step in the DAG.
    test_list is a list of dictionaries. 
    You must update the list with new dictionaries when adding a data quality SQL query
    
    When creating dictionaries to pass into the list, there should be two key value pairs:
    - The first is the test value based on a SQL test query
    - The second is the expected value
    
    
    After creating these values, put them into a dictionary with the following format (where N is the nth test entered in this file):
    
    test_dictN = {
        'testN' : test variable name,
        'expectedN' : expected variable name
     }
     
     
    Then update test_list with test_dictN
     
    
    """
    
    """
    Test 1: Tests start_time columns for any null values. The expected resuls should be 0
    """
    #tests the start_time column for any null values
    time_quality_test = ("""
        SELECT COUNT(*)
        FROM time
        WHERE start_time IS NULL;   
    """)
    
    #expect no null values in the start_time column
    time_quality_test_expected = 0
    
    test_dict1 = {
        'test1' : time_quality_test,
        'expected1' : time_quality_test_expected
    }
    
    """
    Test 2: tests time table for greater than 0 rows
        
    """
    time_rows_test = ("""
        SELECT COUNT(*)
        FROM songplays
        WHERE songplay_id IS NULL;   
    """)
    
    #expect no null values in the songplay_id column
    time_rows_test_expected = 0
    
    test_dict2 = {
        'test2' : time_rows_test,
        'expected2' : time_rows_test_expected
     }
    
    #update this list with queries you want to test with. follow format above
    test_list = [test_dict1, test_dict2]
