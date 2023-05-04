#%%
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect, text
import pandas as pd
class DatabaseConnector:
        def read_db_creds(self,yaml_file):
            with open(yaml_file, 'r') as f:
                credentials_dict = yaml.safe_load(f)
                return credentials_dict
        
        def init_db_engine(self):
            credentials_dict = self.read_db_creds('db_creds.yaml')
            RDS_HOST= credentials_dict['instance']['RDS_HOST']
            RDS_PASSWORD = credentials_dict['instance']['RDS_PASSWORD']
            RDS_USER = credentials_dict['instance']['RDS_USER']
            RDS_DATABASE = credentials_dict['instance']['RDS_DATABASE']
            DATABASE_TYPE = credentials_dict['instance']['DATABASE_TYPE']
            RDS_PORT = credentials_dict['instance']['RDS_PORT']
            engine = create_engine(f'{DATABASE_TYPE}://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}')
            engine = engine.connect()
            return engine
        
        def list_db_tables(self):
            engine = self.init_db_engine()
            inspector = inspect(engine)
            inspector_2 = inspector.get_table_names()
            table_name = inspector_2[1]
            return table_name
      
        def read_rds_database(self,engine,table_name):
            engine = self.init_db_engine()
            table_name = self.list_db_tables()
            legacy_users = pd.read_sql_table(table_name,engine)
            legacy_users = legacy_users.head(10)
            return legacy_users
            print(legacy_users)


if __name__ == '__main__':
    object = DatabaseConnector()
    object_1 = object.read_db_creds('db_creds.yaml')
    object_2 = object.init_db_engine()
    object_3 = object.list_db_tables()
    object_4 = object.read_rds_database(object_2,object_3)
    