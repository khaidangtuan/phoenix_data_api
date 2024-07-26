from typing import Union, List
from fastapi import FastAPI
from pydantic import BaseModel, TypeAdapter
from datetime import datetime
import sqlalchemy as sa
from sqlalchemy import text
import psycopg2
import pandas as pd
from pandas.tseries.offsets import DateOffset
from utils import curr_convert
from postgres_config import conn_str, table_name

class Price(BaseModel):
    price: float
    currency: str
    whatsapp: str
    whatsappName: str
    updated_at: str

# initiate engine
engine = sa.create_engine(conn_str)    

# get static currency due to posibility of time-out request to VCB
curr_df = pd.read_pickle('curr_data.pkl')

app = FastAPI()

@app.get('/getPrice/')
def getPrice(model: str, curr: str, color: str = '') -> Price:
    # create query
    query = f'''
    SELECT "price", "groupName", "senderName","senderPhone","updated_at"
    FROM public."{table_name}"
    WHERE model = '{model}' 
    AND color = '{color}';
    '''
    
    with engine.connect() as conn:
        results = pd.read_sql(text(query), conn)
    
    if results.shape[0] != 1:
        raise ValueError('Return more than 1 record!')
         
    results['price'] = results['price'].apply(lambda x: curr_convert(x, curr_df, curr.lower()))
    
    response = results.to_dict('records')[0]
    print(response)
    
    return Price(price=response['price'],
                 currency=curr.upper(),
                 whatsapp=response['senderPhone'],
                 whatsappName=response['senderName'],
                 updated_at=str(response['updated_at']),
                 )

@app.get('/sortPrice/')
def sortPrice(sort: str, curr: str):
    # create query
    query = f'''
    SELECT "model", "price"
    FROM public."{table_name}"
    ORDER BY "price" {sort.upper()}
    LIMIT 250;
    '''
    
    with engine.connect() as conn:
        results = pd.read_sql(text(query), conn)
    
    results['price'] = results['price'].apply(lambda x: curr_convert(x, curr_df, curr.lower()))
    results['currency'] = curr.upper()
    
    return results.to_dict('records')