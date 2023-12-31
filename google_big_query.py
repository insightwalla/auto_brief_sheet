from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st
import datetime
import pandas as pd

class GoogleBigQuery:
    def __init__(self, key_path):
        self.key_path = key_path

    def connect(self):
        self.credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)
        return self.client
    
    def query(self, query = str, as_dataframe = True):
        with self.connect() as client:
            query_job = self.client.query(query)
            results = query_job.result()
            if as_dataframe:
                results = results.to_dataframe()
            return results

            
@st.cache_data
def _get_data(query):
    connector = GoogleBigQuery('key.json')
    results = connector.query(query)
    return results

def get_data_from_big_query():
    data_start = st.sidebar.date_input('Start Date', value=None, min_value=None, max_value=None, key='start')
    if data_start:
        # transform date to datetime
        data_start = datetime.datetime.combine(data_start, datetime.datetime.min.time())
        data_end = data_start + datetime.timedelta(days=1)
        # keep only dates
        data_start = data_start.date()
        data_end = data_end.date()
        # as string
        data_start = data_start.strftime('%Y-%m-%d')
        data_end = data_end.strftime('%Y-%m-%d')


        query_forecast = '''
        SELECT *
            FROM `rota_ready_integration.shifts`
            WHERE start BETWEEN '{}' AND '{}'
        '''.format(data_start, data_end)
        
        data = _get_data(query_forecast)

        # create a new column called Venue taking the word betweeen the parenthesis in column account_entity_name
        data['Venue'] = data['accounting_entity_name'].str.extract(r"\((.*?)\)", expand=False)

        # unique venues
        venues = data['Venue'].unique()
        #st.write(venues)

        res_to_rename = {
            'Covent Garden': 'D1',
            'Shoreditch': 'D2',
            'Kings Cross': 'D3',
            'Carnaby': 'D4',
            'Edinburgh': 'D5',
            'Kensington': 'D6',
            'Manchester': 'D7',
            'Birmingham': 'D8',
            'Canary Wharf': 'D9'
        }

        data['Venue'] = data['Venue'].replace(res_to_rename)
        data = data[data['Venue'].isin(res_to_rename.values())]

        # create a filter for the venue
        venues_updated = res_to_rename.values()
        venue = st.sidebar.selectbox('Select Venue', venues_updated)
        if venue:
            data = data[data['Venue'] == venue]
        # change shift_type_concatenated_name to type
        data.rename(columns={'shift_type_concatenated_name': 'type', 'user_name': 'user', 'work_entity_name': 'group'}, inplace=True)
        # create a new column with the date from start
        data['date'] = data['start'].apply(lambda x: pd.to_datetime(x).date())

        # workDepartment is the same as group
        data['workDepartment'] = data['group']
        # break is 0
        data['break'] = 0 
        # keep only start time and end time in columns
        data['start'] = data['start'].apply(lambda x: pd.to_datetime(x).time())
        data['end'] = data['end'].apply(lambda x: pd.to_datetime(x).time())
        data['start'] = data['start'].apply(lambda x: (datetime.datetime.combine(datetime.date(1,1,1), x) + datetime.timedelta(hours=1)).time())
        data['end'] = data['end'].apply(lambda x: (datetime.datetime.combine(datetime.date(1,1,1), x) + datetime.timedelta(hours=1)).time())

        # change the date to d m y
        data['date'] = data['date'].apply(lambda x: x.strftime('%d/%m/%Y'))

        # drop user if == 'Unassigned'
        data = data[data['user'] != 'Unassigned']
        columns_to_keep = ['group','user','date','start','end','type','break','workDepartment']
        data = data[columns_to_keep]

        return data

data_ready = get_data_from_big_query()