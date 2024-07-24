import sys
import os 
import dotenv 

project_dir = os.abspath(os.join(os.getcwd(), './'))
sys.path.insert(0, project_dir)

from src.sql_engine import SQLServerEngine
from prefect import flow
from tasks import retrieve_event_form_responses
from utils.shared_tasks import update_table

from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient



@flow(name='actualizar_eventos_DKT_SCHOOL')

def update_events_table(tenant_id,client_id,client_secret,server,database):

    credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret

    )

    scopes = ['https://graph.microsoft.com/.default']


    Client = GraphServiceClient(credential,scopes)
    SQLSession = SQLServerEngine(server=server,database=database)

    df = retrieve_event_form_responses(GraphClient=Client,SQLSession=SQLSession,table='Eventos',primary_key='itemId')

    update_table(SQLSession=SQLSession,dfs=df,table='Eventos',primary_key='itemId')

if __name__ == '__main__':
    
    dotenv.load_dotenv('config/.env')

    update_events_table(tenant_id = os.getenv('TENANT_ID'),
                         client_id = os.getenv('AZURE_CLIENT_ID'),
                           client_secret = os.getenv('AZURE_CLIENT_SECRET'),
                             server=os.getenv('SERVER'),
                               database= 'DKT_SCHOOL')
    







