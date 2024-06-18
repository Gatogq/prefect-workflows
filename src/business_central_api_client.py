import requests
import urllib.parse
from msal import ConfidentialClientApplication
import json

class BusinessCentralAPIClient(requests.Session):
    """
    A class  that inherits from requests.Session class for interacting with Dynamics 365 Business Central API.
    
    This class is intended to provide methods for common operations on Business Central tables and handles OAuth2.0 Authentication.

    Attributes:

        tenant_id (str) : Azure tenant ID.
        environment (str) : name of the Business Central Environment.
        company (str) : name of the company within the environment.
        client_id (str) : the client id of your registered Azure App.
        client_secret (str) : the client secret of your registered Azure App.
        scopes (list) : API scopes, set by default to business central API scopes but including other Microsoft REST APIS are allowed.
        base_url (str) : the base URL of the Business Central API.
        authority (str) : the authority URL required to obtain oauth token.
        access_token (str) : OAuth2.0 access token for the client, automatically retrieved when an object is initialized.
        token_type (str) : token type, automatically retrieved when an object is initialized.
        headers (dict) : the base headers for every request to the Business Central API.
        CUSTOMER_TABLE_ENDPOINT (str) : name of the custom API page entity which exposes data from Customer Table (ID 18)
            Reference to Microsoft Documentation: https://learn.microsoft.com/en-us/dynamics365/business-central/application/base-application/table/microsoft.sales.customer.customer
        PRODUCT_TABLE_ENDPOINT (str) : name of the custom API page entity which exposes data from Item Table (ID 27)
            Reference to Microsoft Documentation: https://learn.microsoft.com/en-us/dynamics365/business-central/application/base-application/table/microsoft.inventory.item.item
    """

    CUSTOMER_TABLE_ENDPOINT = 'SQLCustomer'
    PRODUCT_TABLE_ENDPOINT = 'SQLProduct'

    def __init__(self, 
                 tenant_id, 
                 environment, 
                 company,
                 client_id,
                 client_secret,
                 scopes=['https://api.businesscentral.dynamics.com/.default']
                 ):
        """
        Initializes the API Client with the necessary credentials and base URL.

        Args:
            tenant_id (str): Azure tenant ID.
            environment (str): Name of the Business Central Environment.
            company (str): Name of the company within the environment.
            client_id (str): The client ID of your registered Azure App.
            client_secret (str): The client secret of your registered Azure App.
        """
        super().__init__()

        self.tenant_id = tenant_id
        self.environment = environment
        self.company = company
        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = scopes
        self.base_url = f"https://api.businesscentral.dynamics.com/v2.0/{self.tenant_id}/{self.environment}/ODataV4/Company('{self.company}')/"
        self.authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        self.access_token = None
        self.token_type = None
        self.get_oauth_token()
        self.headers.update(
            {
                'Authorization': f'{self.token_type} {self.access_token}',
                'Accept': 'application/json',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
        )

    def get_oauth_token(self):


        """
        Obtains an OAuth2.0 access token from Azure AD using msal library ConfidentialClientApplication class, as recommended by Microsoft.

        This method handles the OAuth2.0 authentication process and retrieves an access token
        required for making the API requests.
        """

        auth_client = ConfidentialClientApplication(
        client_id=self.client_id,
        client_credential=self.client_secret,
        authority=self.authority
        )   
    
        token_response = auth_client.acquire_token_for_client(scopes=self.scopes)

        if 'access_token' in token_response:

            self.access_token = token_response['access_token']   
            self.token_type = token_response['token_type'] 

        else:

            raise Exception(f'''Unable to obtain access token with parameters provided.
                             client_id : {self.client_id}
                             client_secret : {self.client_secret}
                             authority : {self.authority}''')
        
    def refresh_oauth_token(self):

        """
        Obtains a new OAuth2.0 access token once the current session token expired.
        """

        self.get_oauth_token()

    
    def request(self, url, method, params=None):

        """
        Extended version of request method from request.Session class, modified to handle automatic OAuth2.0 token refresh and
        pagination for 'GET' requests using @OData.nextLink annotation as per OData standard.
        """

        endpoint = urllib.parse.urljoin(self.base_url,url)

        response = super().request(url=endpoint,method=method,headers=self.headers,params=params)

        if response.status_code == 401:

            self.refresh_oauth_token()
            
            response = super().request(url=endpoint,method=method,headers=self.headers,params=params)

        
        response.raise_for_status()

        if method == 'GET':

            nextLink = response.json().get('@odata.nextLink')

            while nextLink:

                nextLink_response = super().request(url=nextLink,method=method,headers=self.headers)
                nextLink_response.raise_for_status()

                response.json()['value'].extend(nextLink_response.json()['value'])

                nextLink = nextLink_response.json().get('@odata.nextLink')
            
            return response.json().get('value')

        return response.json()
    
    def create_parameters(self,createdAt,modifiedAt,orderBy,select,offset,limit,filterExpression):
        """
        Constructs the parameters dictionary of the specific request, 
        using OData standard query options such as $filter, $select, $orderby, $top and $skip.

        Args:
            createdAt (datetime): The value to filter records created after a specific timestamp. 
            (it is required that systemCreatedAt field is included on the API page to use this parameter).
            modifiedAt (datetime): The value to filter records modified after a specific timestamp.
            (it is required that systemModifiedAt field is included on the API page to use this parameter).
            orderBy (str): The field of the API response to order by.
            select (str): The fields to include in the API response.
            offset (int): The number of records to skip in the API response.
            limit (int): The maximum number of records to return in the API response.
            filterExpression (str): A custom filter expression to apply to the request.
        """
        self.params = {}

        if createdAt:

            self.params.update(
                {
                    '$filter' : f'systemCreatedAt gt {createdAt}'
                }
            )
        
        if modifiedAt:

            if '$filter' in self.params:

                self.params['$filter'] =+ f' and ()systemModifiedAt gt {modifiedAt})'

            else:
                
                self.params.update(
                    {
                        '$filter' : f'systemModifiedAt gt {modifiedAt}'
                    }
                )

        if orderBy:

            self.params.update(
                {
                    '$orderby' : f'{orderBy}'
                }
            )

        if select:
            
            self.params.update(
                {
                    '$select' : f'{select}'
                }
            )

        if offset:

            self.params.update(
                {
                    '$skip' : f'{offset}'
                }
            )
        
        if limit:

            self.params.update(
                {
                    '$top' : f'{limit}'
                }

            )
        
        if filterExpression:

            if '$filter' in self.params:

                self.params['$filter'] =+ f' and ({filterExpression})'

            else:

                self.params.update(
                    {
                        '$filter':f'{filterExpression}'
                    }
                )
            
    
    def get_customers(self,createdAt=None,modifiedAt=None,orderBy=None,select=None,offset=None,limit=None,filterExpression=None):

        """get a list of customers for the specific company.
            
            Args:
            createdAt (datetime): retrieve records created after a specific timestamp. (optional)
            (it is required that systemCreatedAt field is included on the API page to use this parameter).
            modifiedAt (datetime): retrieve records modified after a specific timestamp (optional)
            (it is required that systemModifiedAt field is included on the API page to use this parameter).
            orderBy (str): order results by a specific field (optional)
            select (str): specify the fields to include on the api response (optional)
            offset (int): the number of records to skip in the API response. (optional)
            limit (int): the maximum number of records to return in the API response. (optional)
            filterExpression (str): a custom filter expression to apply to the request. (optional)

            Returns:
            A list of customers from the Customer Table Entity in json format. 
        """

        self.create_parameters(createdAt,modifiedAt,orderBy,select,offset,limit,filterExpression)

        return self.request(url=self.CUSTOMER_TABLE_ENDPOINT,method='GET',params=self.params)
    
    def get_products(self,createdAt=None,modifiedAt=None,orderBy=None,select=None,offset=None,limit=None,filterExpression=None):
            
        """get a list of products for the specific company.
            
            Args:
            createdAt (datetime): retrieve records created after a specific timestamp. (optional)
            (it is required that systemCreatedAt field is included on the API page to use this parameter).
            modifiedAt (datetime): retrieve records modified after a specific timestamp (optional)
            (it is required that systemModifiedAt field is included on the API page to use this parameter).
            orderBy (str): order results by a specific field (optional)
            select (str): specify the fields to include on the api response (optional)
            offset (int): the number of records to skip in the API response. (optional)
            limit (int): the maximum number of records to return in the API response. (optional)
            filterExpression (str): a custom filter expression to apply to the request. (optional)

            Returns:
            A list of products  on the Item Table Entity in json format. 
        """

        self.create_parameters(createdAt,modifiedAt,orderBy,select,offset,limit,filterExpression)

        return self.request(url=self.PRODUCT_TABLE_ENDPOINT,method='GET',params=self.params)
    
    def get_customer(self,customerId):

        """get information about a specific customer.
            
            Args:
            customerId (str): the unique identifier for the specific customer, it is found on the 'no' field of the API Page. (required)

            Returns:

            A single record representing the information for the specific customer in json format.
        
        """

        return self.get_customers(filterExpression=f"no eq '{customerId}'")
    
    def get_product(self,productId):

        """get information about a specific product.
            
            Args:
            productId (str): the unique identifier for the specific product, it is found on the 'no' field of the API Page. (required)

            Returns:

            A single record representing the information for the specific product in json format.
        
        """

        return self.get_products(filterExpression=f"no eq '{productId}'")

    


    