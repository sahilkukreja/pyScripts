from google.cloud import storage
from google.cloud import bigquery as bq
import json
import pandas as pd
import os

def get_enviroment_variable(param):
    """ get enviroment variable

    Args:
        param ([string]): [environment variable name]

    Returns:
        [type]: [description]
    """
    original_string = os.environ.get(param,None)
    if original_string is not None:
        original_string = original_string.replace("'", "")

    return original_string

src_bucket = get_enviroment_variable('SOURCE_BUCKET_ID')
dataset_id = get_enviroment_variable('BQ_SETTINGS_DATASET_ID')
table_id='project_billing_info'
storage_client = storage.Client()
bucket = storage_client.get_bucket(src_bucket)
client = bq.Client()
dataset = client.dataset(dataset_id)
client.delete_table('cudinator_stg.project_billing_info', not_found_ok=True)  
for file in storage_client.list_blobs(bucket):
    if ('/mapping.json' in file.name):
        blob = bucket.get_blob(file.name)
        data =blob.download_as_string()
        dict_str = data.decode("UTF-8")
        data=eval(dict_str)

        project_details=[]
        billing_accounts=data.get("billing_accounts")
        # ETL WORK TO ADD BILLING NUMBER TO EACH PROJECT 
        for billingAccounts in billing_accounts:
            billing_Accounts_Projects=billingAccounts.get("projects")
            name=billingAccounts.get("name")
            for billing_Accounts_Project_Id in billing_Accounts_Projects:
                billing_Accounts_Project_Id.update( {'billing_Account_Id' : name.rsplit('/',1)[1]} )
                project_details.append(billing_Accounts_Project_Id)                 
        df_dict = project_details
        df = pd.DataFrame(df_dict)
        table_ref = dataset.table(table_id)
        client.load_table_from_dataframe(df, table_ref).result()
