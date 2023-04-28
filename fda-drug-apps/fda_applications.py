import pandas as pd
from sklearn import preprocessing as skp
import sqlalchemy
import sqlite3


def read_data(path:str) -> pd.DataFrame:
    """ Read in FDA dataset, split into applicants df and drug application df
    
    Args:
        path(str): location of dataset

    Returns:
        applicants(pd.DataFrame): pandas applicants df
        drug(pd.DataFrame): pandas drugs df
    
    """
    df = pd.read_csv(path)
    applicants = pd.DataFrame(df['applicant'], columns=['applicant'])
    
    # Encode applicant values to numerical ids
    le = skp.LabelEncoder()
    df['applicant_id'] = df.apply(le.fit_transform)['applicant']+1  
    applicants['id'] = df['applicant_id']
    drug = df[['applicant_id','bla_nda_number','is_biologic','proprietary_name','proper_name','approval_type',
               'ref_proper_name','ref_proprietary_name','supplement_no','license_no','exclusivity_date',
               'is_deleted','created_at','updated_at']]

    return applicants, drug


def make_db():
    """
        Create SQLite database and empty tables
        
    """
    
    sqliteconn = sqlite3.connect('FDA-apps.db')
    cursor = sqliteconn.cursor()
    
    # Company table with unique applicant names
    createcomp = '''
    CREATE TABLE IF NOT EXISTS company (
        id INTEGER PRIMARY KEY NOT NULL,
        applicant VARCHAR(250) UNIQUE NOT NULL
    );
    '''
    
    # Application table with drug applicantion info
    createapp = '''
    CREATE TABLE IF NOT EXISTS application (
        bla_nda_number INTEGER PRIMARY KEY,
        is_biologic BOOLEAN,
        proprietary_name VARCHAR(250),
        proper_name VARCHAR(250),
        approval_type VARCHAR(250),
        ref_proper_name VARCHAR(250),
        ref_proprietary_name VARCHAR(250),
        supplement_no INTEGER,
        license_no INTEGER,
        exclusivity_date TIMESTAMP,
        is_deleted BOOLEAN,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        applicant_id INTEGER,
        FOREIGN KEY(applicant_id) REFERENCES bridge(self_id)
    );
    '''
    
    # Bridge table to connect application applicant id with company id
    createbridge = '''
    CREATE TABLE IF NOT EXISTS bridge (
        self_id INTEGER,
        parent_id INTEGER,
        FOREIGN KEY(parent_id) REFERENCES company(id)
    );
    '''
    
    cursor.execute(createcomp)
    cursor.execute(createapp)
    cursor.execute(createbridge)
    cursor.close()
    
    
def make_bridge(dfc: pd.DataFrame) -> pd.DataFrame:
    """ Create bridge df for deduplication
    
    Args:
        dfc(pd.DataFrame): applicants df, corresponds to company table
        
    Returns:
        applicants(pd.DataFrame): pandas bridge df
    
    """
    bridgedf = pd.DataFrame(columns=['parent_id', 'self_id'])
    bridgedf['parent_id'] = dfc['id']
    bridgedf['self_id'] = dfc['id']

    for i, app1 in dfc.iterrows():
        for j, app2 in dfc.iloc[i+1:].iterrows():
            if app1['applicant'] in app2['applicant']:
                bridgedf['parent_id'][j] = dfc['id'][i]
            elif app2['applicant'] in app1['applicant']:
                bridgedf['parent_id'][i] = dfc['id'][j]

    return bridgedf


def append_db(dfc: pd.DataFrame, dfa: pd.DataFrame, dfb: pd.DataFrame):
    """ Append data from dfs to corresponding db tables
    
    Args:
        dfc(pd.DataFrame): applicants df, corresponds to company table
        dfa(pd.DataFrame): drug df, corresponds to application table
        dfb(pd.DataFrame): bridge df, corresponds to bridge table

    """ 
    # Create SQLAlchemy engine that connects to companies db
    engine = sqlalchemy.create_engine('sqlite:///FDA-apps.db') 

    # Append df data to corresponding SQL db table
    dfc.to_sql('company', engine, index=False, if_exists='append')
    dfa.to_sql('application', engine, index=False, if_exists='append')
    dfb.to_sql('bridge', engine, index=False, if_exists='append')


if __name__ == '__main__':
    """ Simple relational database for FDA drug applications """
    # Create database and read in FDA data to pandas dfs
    make_db() 
    compdf, appdf = read_data('fda_purple_orange_books.csv')
    
    # Basic cleanup of companies df
    compdf = compdf.drop_duplicates()
    compdf.sort_values(by=['id'], inplace=True)
    compdf.reset_index(inplace=True, drop=True)
    
    # Create bridge df from companies df
    bridge = make_bridge(compdf)
    
    # Append data from pandas df to SQL db
    append_db(compdf, appdf, bridge)