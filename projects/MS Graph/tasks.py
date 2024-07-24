import sys
from os.path import abspath, join
from os import getcwd
project_dir = abspath(join(getcwd(), './'))
sys.path.insert(0, project_dir)


import asyncio
from prefect import task
from pandas import DataFrame
from utils.utilities import set_null_values
from numpy import nan

from src.sql_engine import SQLServerEngine
from msgraph import GraphServiceClient

from msgraph.generated.sites.item.lists.item.list_item_request_builder import ListItemRequestBuilder
from kiota_abstractions.base_request_configuration import RequestConfiguration


#credential = ClientSecretCredential(
#    tenant_id=tenant_id,
#    client_id=client_id,
#    client_secret=client_secret
#)

#Client = GraphServiceClient(credential,scopes)


async def get_list_items(GraphClient,site_id,list_id):

    params = ListItemRequestBuilder.ListItemRequestBuilderGetQueryParameters(

    expand = ["fields"]

    )   

    request_config = RequestConfiguration(
        query_parameters=params
    )

    list_items = await GraphClient.sites.by_site_id(f'{site_id}').lists.by_list_id(f'{list_id}').items.get(request_configuration=request_config)

    if list_items:

        data = list_items.value
    
    while list_items is not None and list_items.odata_next_link is not None: 

        list_items = await GraphClient.sites.by_site_id(f'{site_id}').lists.by_list_id(f'{list_id}').items.with_url(list_items.odata_next_link).get(request_configuration=request_config)
    
        data.extend(list_items.value)
    
    return data


@task(name='descarga respuestas eventos',log_prints=True)
def retrieve_event_form_responses(GraphClient : GraphServiceClient,SQLSession : SQLServerEngine, table, primary_key):
    
    site_id = '61085889-2859-43b4-9e56-5edc109aa0ac,41bc9dfa-8920-4971-bffc-7ec6b41dacfb'
    list_id = '74913d76-843e-488b-9f5a-960fc6dcc986'

    data = asyncio.run(
        get_list_items(GraphClient,site_id,list_id)
        )

    main_list = []
    
    
    for ListItem in data:

        
        fields = ListItem.fields.additional_data
        
        items_d = {

            'itemId' : ListItem.id,
            'responseId' : fields.get('ID_x002d_form'),
            'email' : fields.get('Email'),
            'planCorrespondiente' : fields.get('PlanCorrespondienteATuReporte'),
            'aQueRegionPerteneces' : fields.get('AQueRegionPerteneces'),
            'estadoEvento' : fields.get('EstadoDondeRealizasteEvento'),
            'fechaEvento' : fields.get('FechaDelEvento'),
            'tipoEvento' : fields.get('TipoEvento'),
            'nombreEvento' : fields.get('NombreEvento'),
            'nivelEducativo' : fields.get('SeleccionaNivelEducativo'),
            'personaVoluntaria' : fields.get('PersonaVoluntariaQueAcompa_x00f1'),
            'personasImpactadas' : fields.get('PersonasImpactadas'),
            'condonesRepartidos' : fields.get('CondonesRepartidos'),
            'evidencia' : fields.get('Evidencias'),
            'comentarios' : fields.get('ComentariosEvento'),
            'materialesRepartidos' : fields.get('MaterialesRepartidos'),
            'nombreTaller' : fields.get('NombreDelTaller'),
            'comunidadVisitada' : fields.get('NombreComunidadVisitada'),
            'createdAt' : fields.get('Created')         
            }
        
        main_list.append(items_d)

    
    ids = set(SQLSession.select_values(table,columns=[primary_key]))

    df = DataFrame(main_list).map(set_null_values).replace({nan:None})

    df_to_insert = df[~df[primary_key].isin(ids)]

    return {

            'insert' : df_to_insert
        }
