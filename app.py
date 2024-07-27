from typing import List
from fastapi import FastAPI
from pydantic import BaseModel, TypeAdapter
import sqlalchemy as sa
from sqlalchemy import text
import psycopg2
import pandas as pd
from utils import curr_convert
from postgres_config import conn_str, table_name

class Price(BaseModel):
    price: float
    currency: str
    whatsapp: str
    whatsappName: str
    updated_at: str
    
class Value(BaseModel):
    code: str
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
    SELECT "price", "senderName","senderPhone","updated_at"
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
    
    return Price(price=response['price'],
                 currency=curr.upper(),
                 whatsapp=response['senderPhone'],
                 whatsappName=response['senderName'],
                 updated_at=str(response['updated_at']),
                 )

@app.get('/sortPrice/')
def sortPrice(sort: str, curr: str, order: str, type: int):
    
    if order == "price":
        sorted_col = order
    elif order == "created_at_time":
        sorted_col = "updated_at"
    
    # create query
    query = f'''
    SELECT "model", "price", "senderName","senderPhone","updated_at"
    FROM public."{table_name}"
    ORDER BY "{sorted_col}" {sort.upper()}
    LIMIT 250;
    '''
    
    with engine.connect() as conn:
        results = pd.read_sql(text(query), conn)
    
    results['price'] = results['price'].apply(lambda x: curr_convert(x, curr_df, curr.lower()))
    results['currency'] = curr.upper()
    results.rename(columns={'model':'code',
                            'senderPhone':'whatsapp',
                            'senderName':'whatsappName'}, inplace=True)
    
    results['updated_at'] = results['updated_at'].astype(str)
    
    ta = TypeAdapter(List[Value])
    values = ta.validate_python(results.to_dict('records'))
    
    return values