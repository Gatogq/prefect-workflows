from sqlalchemy import create_engine, MetaData, Table, select, and_
from datetime import datetime
from sqlalchemy.exc import IntegrityError,OperationalError,SQLAlchemyError
from sqlalchemy.orm import sessionmaker

class SQLServerEngine:

    def __init__(self,engine_type='mssql',server=None,database=None):

        self.engine_type = engine_type
        self.server = server
        self.database = database
        
        if self.engine_type == 'mssql':
            self.connection_url = f'mssql+pyodbc://{self.server}/{self.database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
        elif self.engine_type == 'sqlite':
            self.connection_url = f'sqlite:///{database}'

        self.engine = create_engine(self.connection_url)
        self.Session = sessionmaker(bind=self.engine)


    def execute_query(self,query):
        
        with self.engine.connect() as c:

            r = c.execute(query)

            try:
                return r.fetchall()
            
            except:
                
                print('La consulta se ejecutó correctamente sin devolver filas.')

                return None
    
    def get_set_of_unique_values(self,table,column):

        r = self.execute_query(f'''
                                  SELECT DISTINCT
                                  {column} FROM {table}''')
        
        return set([row[0] for row in r])
    
    def bulk_insert_from_df(self,table_data):

        try:

            with self.Session() as session:

                for table_name,df in table_data.items():

                    if not df.empty:

                        d = df.to_dict(orient='records')

                        metadata = MetaData()
                        table = Table(table_name, metadata, autoload=True, autoload_with=self.engine)
                
                        session.execute(table.insert(), d)

                session.commit()

                for table_name,df in table_data.items():

                    print(f'Se insertaron correctamente los nuevos registros en la tabla {table_name}. Se añadieron {len(df)} registros')

        except Exception as e:

            session.rollback()

            tables = ', '.join(list(table_data.keys()))

            raise SQLAlchemyError(f'Error al intentar actualizar las tablas {tables}: {e}')

        
    
    def update_records_from_df(self, table_name, df, primary_key):

        update_data = df.to_dict(orient='records')

        try:

            with self.Session() as session:
                    
                metadata = MetaData()
                table = Table(table_name, metadata, autoload=True, autoload_with=self.engine)

                    
                for data in update_data:

                    if isinstance(primary_key,str):

                        condition = table.c[primary_key] == data[primary_key]
                        
                    elif isinstance(primary_key,(list,tuple)):

                        cds = [getattr(table.c,k) == data[k] for k in primary_key]

                        condition = and_(*cds)

                    else:
                        raise ValueError('Invalid data type for parameter primary key: it must be a str or a tuple/list')
                        
                    statement = table.update().where(condition).values(data)

                    session.execute(statement)
                
                session.commit()

                print(f'Se actualizaron correctamente los registros de la tabla {table_name}. Se modificaron {len(df)} registros.')

        except Exception as e:

            session.rollback()
            raise SQLAlchemyError(f'Error al intentar actualizar registros en la tabla {table_name}: {e}')
        

        
    def get_columns_from_table(self, table):

        if self.engine_type == 'mssql':

            r = self.execute_query(f'''SELECT COLUMN_NAME
                                FROM INFORMATION_SCHEMA.COLUMNS
                               WHERE TABLE_NAME = '{table}'
                                 ''')
            return [row[0] for row in r]
            
        elif self.engine_type == 'sqlite':

            r = f'''
                PRAGMA table_info({table}
            '''
            return [row['name'] for row in r]
    
    def get_last_update(self, table,time_column):

        r = self.execute_query(f"SELECT MAX({time_column}) FROM {table}")
        if r:
            return r[0][0] or 0
        else:
            return 0

    def get_last_visit_date(self,table,column):

        subquery = f'SELECT MAX({column}) FROM {table}'

        query = f'{subquery} WHERE {column} < ({subquery})' 

        r = self.execute_query(query)

        if self.engine_type == 'mssql':

            if r:
                return r[0][0] or datetime(2023,11,1)
        
            else:
                return datetime(2023,11,1)
        
        if self.engine_type == 'sqlite':

            if r:
                return datetime.strptime(r[0][0], '%Y-%m-%d') or datetime(2023,11,1)
            
            else:
                return datetime(2023,11,1)
    
    def select_values(self,table,columns):

        metadata = MetaData()
        table = Table(table,metadata,autoload=True,autoload_with=self.engine)

        query = select([table.c[column] for column in columns])
        
        r = self.execute_query(query)

        if len(columns) > 1:

            return [tuple(row) for row in r]
        else:

            return [row[0] for row in r]