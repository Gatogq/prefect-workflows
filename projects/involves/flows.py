from tasks import get_pointofsale_data,get_employee_data,get_visit_data
from tasks import get_channel_data,get_chain_data,get_region_data,update_table
from prefect import flow
from utils.webhooks import success_hook,failure_hook
from src.involves_api_client import InvolvesAPIClient
from src.sql_engine import SQLServerEngine

@flow(name='integracion_SQL_involves_clinical',
      log_prints=True,
      on_completion=[success_hook],
      on_failure=[failure_hook]
      )

def update_involves_clinical_db(environment,domain,username,password,engine_type,database,server):

    Client = InvolvesAPIClient(environment,domain,username,password)
    SQLSession = SQLServerEngine(engine_type,server,database)

    pos_fields = [

        'id',
        'pointOfSaleBaseId',
        'name',
        'code',
        'enabled',
        'region_id',
        'chain_id',
        'pointOfSaleChannel_id',
        'address_zipCode',
        'address_city_name',
        'address_city_state_name',
        'address_latitude',
        'address_longitude',
        'deleted'
    ]

    employee_fields = [

        'id',
        'name',
        'nationalIdCard2',
        'userGroup_name',
        'email',
        'enabled',
        'fieldTeam'

    ] 

    visit_fields = [

        'visit_date',
        'customer_id',
        'employee_name',
        'visit_status',
        'check_in',
        'check_out'

    ]



    pos = get_pointofsale_data.submit(Client=Client,SQLSession=SQLSession,fields=pos_fields,table='PointOfSale',primary_key='id')

    employees = get_employee_data.submit(Client=Client,SQLSession=SQLSession,fields=employee_fields,table='Employee',primary_key='id')

    visits = get_visit_data.submit(SQLSession,username,password,environment,domain,fields=visit_fields,table='Visit')  

    channels = get_channel_data.submit(Client=Client,SQLSession=SQLSession,table='Channel',primary_key='id')

    chains = get_chain_data.submit(Client=Client,SQLSession=SQLSession,fields=['id','name'],table='Chain',primary_key='id')

    regions = get_region_data.submit(Client=Client,SQLSession=SQLSession,fields=['id','name'],table='Region',primary_key='id')


    update_table.map(SQLSession,
                     dfs=[pos,employees,channels,chains,regions,visits],
                     table=['PointOfSale','Employee','Channel','Chain','Region','Visit'],
                     primary_key=['id','id','id','id','id',['visit_date','customer_id']]
                     )


@flow(name='integracion_SQL_involves_dkt',
      log_prints=True,
      on_completion=[success_hook],
      on_failure=[failure_hook]
      )

def update_involves_dkt_db(environment,domain,username,password,engine_type,database,server):

    Client = InvolvesAPIClient(environment,domain,username,password)
    SQLSession = SQLServerEngine(engine_type,server,database)

    pos_fields = [
          
         'id',
         'name',
         'pointOfSaleBaseId',
         'code',
         'enabled',
         'region_id',
         'chain_id',
         'pointOfSaleType_id',
         'pointOfSaleProfile_id',
         'pointOfSaleChannel_id',
         'address_zipCode',
         'address_city_name',
         'address_city_state_name',
         'address_latitude',
         'address_longitude',
         'deleted',
         'storeNumber'
    
    
    ]

    employee_fields = [

        'id',
        'name',
        'userGroup_name',
        'employeeEnvironmentLeader_name',
        'enabled'

    ] 

    visit_fields = [

        'visit_date',
        'customer_id',
        'employee_name',
        'visit_status',
        'check_in',
        'check_out',
        'surveys',
        'justification'

    ]

    pos = get_pointofsale_data.submit(Client=Client,SQLSession=SQLSession,fields=pos_fields,table='PointOfSale2',primary_key='id')

    employees = get_employee_data.submit(Client=Client,SQLSession=SQLSession,fields=employee_fields,table='Employee',primary_key='id')

    visits = get_visit_data.submit(SQLSession,username,password,environment,domain,fields=visit_fields,table='Visit')  

    channels = get_channel_data.submit(Client=Client,SQLSession=SQLSession,table='Channel',primary_key='id')

    chains = get_chain_data.submit(Client=Client,SQLSession=SQLSession,fields=['id','name'],table='Chain',primary_key='id')

    regions = get_region_data.submit(Client=Client,SQLSession=SQLSession,fields=['id','name','macroregional_id'],table='Region',primary_key='id')


    update_table.map(SQLSession,
                     dfs=[pos,employees,channels,chains,regions,visits],
                     table=['PointOfSale2','Employee','Channel','Chain','Region','Visit'],
                     primary_key=['id','id','id','id','id',['visit_date','customer_id']]
                     )


if __name__ == '__main__':

   from dotenv import load_dotenv
   import os

   env_vars_path = env = os.path.abspath(os.path.join(os.path.dirname(__file__),'..','..','config','.env'))

   load_dotenv(env_vars_path)

   

   environment = 1
   domain = os.getenv('INVOLVES_DOMAIN')
   username = os.getenv('INVOLVES_USERNAME')
   password = os.getenv('INVOLVES_PASSWORD')
   database = 'Involves_DKT'
   server = os.getenv('SERVER')

   update_involves_dkt_db(environment,domain,username,password,'mssql',database,server)

