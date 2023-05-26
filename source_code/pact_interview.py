import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas 
import pandas as pd

# Fetch data from Stack Exchange API
def fetch_data_from_stack_overflow(api_key, query_params):
    base_url = "https://api.stackexchange.com/2.3/"
    endpoint = "questions"  
    
   
    common_params = {
        "site": "stackoverflow",
        "key": api_key
    }
    
  
    query_params.update(common_params)
    
    try:
        response = requests.get(base_url + endpoint, params=query_params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print("Error fetching data from Stack Overflow API:", str(e))
        return None

# Store data in Snowflake table
def store_data_in_snowflake(data, snowflake_config):
    conn = snowflake.connector.connect(**snowflake_config)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stack_overflow_data (
        question_id INTEGER,
        title STRING,
        tags ARRAY,
        creation_date TIMESTAMP
    )
    """
    cursor.execute(create_table_query)

    # Insert data into the table
    insert_values = [
        (
            item['question_id'],
            item['title'],
            item['tags'],
            item['creation_date']
        )
        for item in data['items']
    ]
    df=pd.DataFrame(insert_values,columns=['QUESTION_ID', 'TITLE', 'TAGS', 'CREATION_DATE'],index=None)
    print(df)
    success, nchunks, nrows, _= write_pandas(
        conn,
        df,
        'STACK_OVERFLOW_DATA')

    conn.commit()
    cursor.close()
    conn.close()
    print("Data stored in Snowflake table successfully.")


api_key = "your api key"
query_params = {
    "tagged": "python",
    "order": "desc",
    "sort": "creation"
}

response_data = fetch_data_from_stack_overflow(api_key, query_params)
if response_data:
    snowflake_config = {
        'user': 'username',
        'password': 'password',
        'account': 'account details',
        'warehouse': 'COMPUTE_WH',
        'database': 'SAMPLE_ANSH',
        'schema': 'pact'
    }
    #print(response_data)
    store_data_in_snowflake(response_data, snowflake_config)
