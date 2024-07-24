import sys
import os
from pandas import DataFrame
from prefect import task
from prefect.artifacts import create_markdown_artifact

current_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.abspath(os.path.join(os.getcwd(), './'))

sys.path.insert(0, project_dir)

from src.business_central_api_client import BusinessCentralAPIClient
from src.sql_engine import SQLServerEngine


@task(log_prints=True, task_run_name='GET - {table}')
def get_records(Session : SQLServerEngine,APIClient : BusinessCentralAPIClient,endpoint : str,table : str,
                fields : dict,allowed_operations : dict) -> dict:
    
    #obtain last created record datetime and last modified record datetime from sql table, formated as ISO 8601 standard
    
    last_created_dt = Session.get_last_update(table,'systemCreatedAt').strftime('%Y-%m-%dT%H:%M:%S.%f')+'Z'

    last_modified_dt = Session.get_last_update(table,'systemModifiedAt').strftime('%Y-%m-%dT%H:%M:%S.%f')+'Z'

    #fields to retrieve from the api call, as a list

    columns = [_['endpoint_field'] for _ in fields.values()]

    inserted_records = [] 
    
    modified_records = []

    #make api calls using the following query parameters: $select to retrieve only required columns and 
    # $filter to fetch only records created and modified after last database update.

    if allowed_operations['insert']:

        data =  APIClient.get_with_odata_parameters(api_page=endpoint,createdAt=last_created_dt,select=columns)

        #set column names and apply custom null logic as specified on db_schema.json

        inserted_records = [transform_record(_, fields) for _ in data]

    if allowed_operations['update']:


        data =  APIClient.get_with_odata_parameters(api_page=endpoint,createdAt=last_modified_dt,select=columns)

        #set column names and apply custom null logic as specified on db_schema.json

        modified_records = [transform_record(_, fields) for _ in data]

        #filter out modified records that are also new records

        modified_records = [_ for _ in modified_records if _ not in inserted_records]


        #return a dictionary containing the list of dictionaries to insert and to update on sql table.

    result = {'insert' : inserted_records, 'update' : modified_records}

    return result


@task(name='actualizar tabla SQL',log_prints=True)
def update_table_with_dicts(SQLSession : SQLServerEngine,dicts : dict,table : str,primary_key : list):

    #only if 'insert' key contains a non empty list, perform bulk insertion of new records.
 
    if 'insert' in dicts:

        rows = dicts['insert']

        if rows:

            SQLSession.bulk_insert_from_pydicts(pydict=rows,table_name=table)

            markdown_table = DataFrame(rows).to_markdown(index=True)

            description = f'{len(rows)} registros nuevos'

            #create a markdown table of the inserted rows to show on the prefect UI
            
            create_markdown_artifact(markdown=markdown_table,description=description)

        else:

            print(f'No se encontraron registros nuevos para añadir a la tabla {table}')

    
    #only if 'update' key contains a non empty list, perform update of modified records based on its primary key.
       
    if 'update' in dicts:

        rows = dicts['update']
     
        if rows:
               
            SQLSession.update_records_from_pydicts(table,rows,primary_key)

            markdown_table = DataFrame(rows).to_markdown(index=True)

            description = f'{len(rows)} registros actualizados'

            #create a markdown table of the modified rows to show on the prefect UI

            create_markdown_artifact(markdown=markdown_table,description=description)

        else:
               
            print(f'No se encontraron registros modificados para actualizar en la tabla {table}')



def transform_record(row : dict,fields : dict):

    #for each dictionary which represents a row on the api response, rename, mapping the api fields to the sql table fields.

    column_map = {v['endpoint_field']: k for k, v in fields.items()}

    #custom mapping dictionaries, for certain columns as specified on db_schema.json, mapping empty values to None
    # which is compatible with SQL NULL value

    str_null_map = {'' : None,' ' : None}

    int_null_map = {0 : None}

    date_null_map = { '0001-01-01' : None}

    row = {column_map.get(k, k): v for k, v in row.items()}

    #custom logic that iterates over dictionary values to set None for empty values.

    for f, attr in fields.items():

        if attr['apply_null_format']:

            match attr['type']:
                
                case 'str':

                    row[f] = str_null_map.get(row[f], row[f])

                case 'int':

                    row[f] = int_null_map.get(row[f], row[f])

                case 'date':

                    row[f] = date_null_map.get(row[f],row[f])

    return row
