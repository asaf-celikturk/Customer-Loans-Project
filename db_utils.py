import yaml
from sqlalchemy import create_engine
import pandas as pd


class RDSDatabaseConnector:
    
    # Step 4
    def __init__(self):
        
        self.loadedDatabase = self.loadDatabase()
        self.engine = self.initialiseEngine()   
        self.extract = self.extractData()

    # Step 3
    def loadDatabase(self):
        with open('credentials.yaml', 'r') as file:
            data = yaml.safe_load(file)

        return data

    # Step 5
    def initialiseEngine(self):
        # Creating an SQLAlchemy engine using the extracted credentials
        creds = self.loadedDatabase
        
        USERNAME = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD']
        HOST = creds['RDS_HOST']
        DATABASE = creds['RDS_DATABASE']
        PORT = creds['RDS_PORT']
        
        connection_string = f"postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        engine = create_engine(connection_string)
        
        return engine
    
    # Step 6
    def extractData(self):
        df = pd.read_sql_table('loan_payments', self.engine)
        return df
    
    # Step 7
    def saveData(self):
        df = pd.DataFrame(self.extract)
        df.to_csv('/mnt/c/users/Work/aicore-files/customer-loans-finance/customer-loans.csv', index = False)
        return df




object1 = RDSDatabaseConnector()
print(object1.saveData())