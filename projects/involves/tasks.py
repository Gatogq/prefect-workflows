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
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from src.involves_api_client import InvolvesAPIClient
from src.sql_engine import SQLServerEngine

@task(name='actualizar tabla SQL',log_prints=True)
def update_table(SQLSession,dfs,table,primary_key):

    if 'insert' in dfs:

        df = dfs['insert']

        table_data = {
             
             table : df
        }

        if not df.empty:

            SQLSession.bulk_insert_from_df(table_data)
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


@task(name='insertar registros SQL',log_prints=True)
def insert_records_into_table(SQLSession:SQLServerEngine,tables:dict):
        

        SQLSession.bulk_insert_from_df(tables)

        for table_name, df in tables.items():

            if not df.empty:
             
                create_markdown_artifact(markdown=df.to_markdown(index=False),description=f'registros nuevos tabla {table_name}')



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


     rows = Client.get_employees(select=fields)

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

@task(name='descarga macroregionales',log_prints=True)
def get_macroregional_data(Client,SQLSession,ids,table,primary_key):
     
     
    rows = list()

    for _ in ids:

        try:  
            macroregion = Client.get_macroregional_by_id(_)
            rows.append(macroregion)
            
        except:
             continue

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


@task(name='descarga ausencias empleados',log_prints=True)
def get_employee_absence_data(Client,SQLSession,table,fields,primary_key):
     
    rows = Client.get_employee_absences(select=fields)

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
          'update': df_to_update

          }

@task(name='descarga formularios',log_prints=True)       
def get_form_data(Client,SQLSession,table,primary_key):
         
    rows = Client.get_activated_forms()

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
          'update': df_to_update

          }
     
@task(name='descarga campos formularios',log_prints=True)
def get_form_fields_data(Client,SQLSession,ids,table,primary_key):
     
    rows = list()

    for _ in ids:
          
        form_fields = Client.get_fields_by_form_id(_)

        for element in form_fields:

            row = {
                 
                'form_id' : _,
                'id' : element.get('id'),
                'question' : element.get('question')

                }
        
            rows.append(row)

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
          'update': df_to_update
          }

@task(name='descarga encuestas',log_prints=True)
def get_survey_data(Client:InvolvesAPIClient,SQLSession:SQLServerEngine,table,primary_key,formIds:list):


    if Client.environment == 1:
         
        limits = {
              
              16 : 1804758,
              111: 1804637
         }
    
    else:
         
         limits = {}

        
    valid_ids = set()

    for _ in formIds:
        
        response = Client.get_answered_surveys(status='ANSWERED',formIds=_,select=[primary_key])

        if _ in limits:
            
            lower_limit = limits[_]
            
            response_ids = {r['id'] for r in response if r['id'] >= lower_limit}

        else:

             response_ids = {r['id'] for r in response}  

        valid_ids.update(response_ids)  

    existing_ids = set(SQLSession.select_values(table,columns=[primary_key]))

    ids = valid_ids - existing_ids

    header = []
    detail = []

    for _ in ids:
         
         row = Client.get_survey_response_by_id(_,select=['form_id','id','responseDate','ownerId','pointOfSaleId','answers'])

         survey_header = row.copy()

         survey_header.pop('answers')

         header.append(survey_header)

         for i in row['answers']:
              
              survey_detail = {
                   
                   'id' : _,
                   'item_id' : i['item']['id'] if i['item'] else None,
                   'topic_id' : i['item']['topic'] if i['item'] else None,
                   'question_id' : i['question']['id'],
                   'answer_value' : i['value']
              }


              detail.append(survey_detail)

    if header:
         
         header = DataFrame(header).map(set_null_values).replace({nan:None})
         detail = DataFrame(detail).map(set_null_values).replace({nan:None})


    else:
         
         header = DataFrame({})
         detail = DataFrame({})

         print('No se añadieron o modificaron registros desde la última actualización')
    

    return header, detail

     
    
    
          

     
     
     