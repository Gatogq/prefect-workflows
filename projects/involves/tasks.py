import sys
from os.path import abspath, join
from os import getcwd
project_dir = abspath(join(getcwd(), './'))
sys.path.insert(0, project_dir)

from pandas import DataFrame
from datetime import datetime
from numpy import nan
from prefect import task
from utils.utilities import download_visits, set_null_values
from utils.retry_handlers import visit_bot_retry_fn
from prefect import task
from prefect.artifacts import create_markdown_artifact

@task(name='actualizar tabla SQL',log_prints=True)
def update_table(SQLSession,dfs,table,primary_key):

    if 'insert' in dfs:

        df = dfs['insert']

        if not df.empty:

            SQLSession.bulk_insert_from_df(table,df)
            create_markdown_artifact(markdown=df.to_markdown(index=False),description='registros nuevos')

        else:
            print(f'No se encontraron registros nuevos para añadir a la tabla {table}')
        
        if 'update' in dfs:

            df = dfs['update']
     
            if not df.empty:
               
               SQLSession.update_records_from_df(table,df,primary_key)
               create_markdown_artifact(markdown=df.to_markdown(index=False),description='Registros actualizados')

            else:
               print(f'No se encontraron registros modificados para actualizar en la tabla {table}')

        if 'delete' in dfs:
           
           df = dfs['delete']
           
           if not df.empty:
                #---
                create_markdown_artifact(markdown=df.to_markdown(index=False),description='Registros eliminados')
               
           else:
               print(f'No se encontraron registros eliminados en la tabla {table}')




@task(name='descarga puntos de venta',log_prints=True,retries=2)
def get_pointofsale_data(Client,SQLSession,fields,table,primary_key):

    timestamp = int(datetime.now().timestamp()*1000)

    last_update_timestamp = SQLSession.get_last_update(table,time_column='updated_at')

    rows = Client.get_pointsofsale(select=fields,updatedAtMillis=last_update_timestamp)

    if rows:
            
        df = DataFrame(rows).assign(updated_at=timestamp)
        
        df = df.map(set_null_values).replace({nan:None})

        ids = set(SQLSession.select_values(table,columns=[primary_key]))

        df_to_insert = df[~df[primary_key].isin(ids)]

        df_to_update = df[df[primary_key].isin(ids)]

    
    else:

        df_to_insert = DataFrame({})
        df_to_update = DataFrame({})

        print('No se añadieron o modificaron registros desde la última actualización')


    return {
         
          'insert':df_to_insert,
          'update':df_to_update
     } 

@task(name='descarga empleados',log_prints=True,retries=2)
def get_employee_data(Client,SQLSession,fields,table,primary_key):

     timestamp = int(datetime.now().timestamp()*1000)

     last_update_timestamp = SQLSession.get_last_update(table,time_column='updated_at')

     rows = Client.get_employees(select=fields,updatedAtMillis=last_update_timestamp)

     if rows:
            
            df = DataFrame(rows).assign(updated_at=timestamp).map(set_null_values).replace({nan:None})

            ids = set(SQLSession.select_values(table,columns=[primary_key]))

            df_to_insert = df[~df[primary_key].isin(ids)]
      
            df_to_update = df[df[primary_key].isin(ids)]
            
     else:

            df_to_insert = DataFrame({})
            df_to_update = DataFrame({})

            print('No se añadieron o modificaron registros desde la última actualización')


     return {
          
          'insert': df_to_insert,
          'update':df_to_update
          }

     
@task(name='descarga visitas',log_prints=True,retries=3,retry_condition_fn=visit_bot_retry_fn)
def get_visit_data(SQLSession,username,password,environment,domain,fields,table):

     date = SQLSession.get_last_visit_date(table,column='visit_date')

     df = download_visits(username,password,date,environment,domain)

     df.columns = fields

     ids = set(SQLSession.select_values(table,columns=['visit_date','customer_id']))

     df_to_insert = df[~df.apply(lambda row: (row['visit_date'],row['customer_id']) in ids, axis=1)].replace({nan:None})
     df_to_update = df[df.apply(lambda row: (row['visit_date'],row['customer_id']) in ids, axis=1)].replace({nan:None})

     return {

          'insert' : df_to_insert,
          'update' : df_to_update
     }


@task(name='descarga canales PDV',log_prints=True)
def get_channel_data(Client,SQLSession,table,primary_key):
     
     rows = Client.get_channels()

     if rows:
            
            df = DataFrame(rows).map(set_null_values).replace({nan:None})

            ids = set(SQLSession.select_values(table,columns=[primary_key]))

            df_to_insert = df[~df[primary_key].isin(ids)]

            df_to_update = df[df[primary_key].isin(ids)]
            
     else:

            df_to_insert = DataFrame({})
            df_to_update = DataFrame({})

            print('No se añadieron o modificaron registros desde la última actualización')


     return {
          
          'insert': df_to_insert,
          'update':df_to_update
          }

@task(name='descarga cadenas PDV',log_prints=True)
def get_chain_data(Client,SQLSession,table,fields,primary_key):
      
      rows = Client.get_chains(select=fields)

      if rows:
            
            df = DataFrame(rows).map(set_null_values).replace({nan:None})

            ids = set(SQLSession.select_values(table,columns=[primary_key]))

            df_to_insert = df[~df[primary_key].isin(ids)]

            df_to_update = df[df[primary_key].isin(ids)]
            
      else:

            df_to_insert = DataFrame({})
            df_to_update = DataFrame({})

            print('No se añadieron o modificaron registros desde la última actualización')


      return {
          
          'insert': df_to_insert,
          'update':df_to_update
          }
      
            
@task(name='descarga regiones',log_prints=True)
def get_region_data(Client,SQLSession,table,fields,primary_key):
            
      rows = Client.get_regions(select=fields)

      if rows:
            
            df = DataFrame(rows).map(set_null_values).replace({nan:None})

            ids = set(SQLSession.select_values(table,columns=[primary_key]))

            df_to_insert = df[~df[primary_key].isin(ids)]

            df_to_update = df[df[primary_key].isin(ids)]
            
      else:

            df_to_insert = DataFrame({})
            df_to_update = DataFrame({})

            print('No se añadieron o modificaron registros desde la última actualización')


      return {
          
          'insert': df_to_insert,
          'update':df_to_update
          }
