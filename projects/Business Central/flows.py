from prefect import flow
from tasks import get_records, update_table_with_dicts

import sys
import os
import json
import dotenv

from src.business_central_api_client import BusinessCentralAPIClient
from src.sql_engine import SQLServerEngine

current_dir = os.path.dirname(os.path.abspath(__file__))

project_dir = os.abspath(os.join(os.getcwd(), './'))

sys.path.insert(0, project_dir)


@flow(name='actualizacion BC_PROD SQL',log_prints=True)

def update_bc_prod_db(tenant_id,environment,company,client_id,client_secret,server,database):

    APIClient = BusinessCentralAPIClient(tenant_id,
                         environment,
                         company,
                         client_id,
                         client_secret,
                         )

    Session = SQLServerEngine(server=server,
                             database=database)
    
    
    json_path = os.path.join(current_dir,'db_schema.json')

    with open(json_path,'r') as file:

        db_schema = json.load(file)

    for table, table_config in db_schema['tables'].items():
        
        endpoint = table_config['endpoint']

        fields_attr = table_config['fields']

        primary_key = [k for k, v in fields_attr.items() if v.get('primary_key')]

        allowed_ops = table_config['allowed_operations']

        data = get_records.submit(Session,APIClient,endpoint,table,fields_attr,allowed_ops)

        update_table_with_dicts.submit(Session,data,table,primary_key,wait_for=[data])


if __name__ == '__main__':
        
    dotenv.load_dotenv('config/.env')

    update_bc_prod_db(tenant_id = os.getenv('TENANT_ID'),
                       environment = os.getenv('BC_ENVIRONMENT'),
                       company = os.getenv('BC_COMPANY'),
                       client_id = os.getenv('AZURE_CLIENT_ID'),
                       client_secret = os.getenv('AZURE_CLIENT_SECRET'),
                       server= os.getenv('SERVER'),
                       database= os.getenv('BUSINESS_CENTRAL_DATABASE'))





