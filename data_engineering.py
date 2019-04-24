#!/usr/bin/env python
# coding: utf-8

# In[112]:


import pandas as pd
from sodapy import Socrata

app_token = "6mndJ6dQB2dsu0CXE1RFz9AGA"


# In[113]:


##EXTRACT
url="https://data.cityofnewyork.us/resource/qiz3-axqb.json"


# In[114]:


df=pd.read_json(url)


# In[115]:


#shows first 5 rows of the dataset
df.head()


# In[116]:


#provides list of columns in dataset
df.columns


# In[117]:


##TRANSFORM

#concatenates date and time into one column
df['datetime']=pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str), errors='coerce')


# In[118]:


#removes whitespace from dataframe
df=df.apply(lambda x: x.str.strip() if x.dtype =='object' else x)
df.head()


# In[119]:


#prints first val of street name col
df['on_street_name'][0]


# In[120]:


#subsets columns of interest
columns_i_want = ['unique_key', 'latitude', 'longitude', 'on_street_name', 'cross_street_name',
                 'number_of_persons_injured','contributing_factor_vehicle_1','vehicle_type_code1', 'vehicle_type_code2','datetime']


# In[121]:


#creates new dataframe from subsetted columns
df=df[columns_i_want]
df.head()


# In[122]:


#reduces vehicle types to unique values
df['vehicle_type_code1'].unique()


# In[123]:


#reduces vehicle types to unique values
df['vehicle_type_code2'].unique()


# In[124]:


#removes duplicates from 'on_street_name' and 'cross_street'
street_names=set(list(df['on_street_name'].unique())+list(df['cross_street_name'].unique()))


# In[125]:


#removes duplicate values from contributing factor
contributing_factor=list(df['contributing_factor_vehicle_1'].unique())


# In[126]:


#removes duplicate values between vehicle type1 and 2
vehicle_types=set(list(df['vehicle_type_code1'].unique())+list(df['vehicle_type_code2'].unique()))


# In[127]:


#creates function to create dictionary out of col values
def create_table_dict(table):
    table_ids=list(range(1,len(table)+1))
    table_types=list(table)
    table_dictionary=dict(zip(table_ids, table_types))
    return table_dictionary

new_vehicle_dict=create_table_dict(vehicle_types)
new_street_names_dict=create_table_dict(street_names)
new_contributing_factor_dict=create_table_dict(contributing_factor)


# In[128]:


#creates dataframe from dictionary
vehicle_df=pd.DataFrame.from_dict(new_vehicle_dict, orient="index").reset_index()
vehicle_df.columns=['vehicle_id','vehicle_type']


# In[129]:


#creates dataframe from dictionary
street_df=pd.DataFrame.from_dict(new_street_names_dict, orient="index").reset_index()
street_df.columns=['street_id','street_name']


# In[130]:


#creates dataframe from dictionary
contributing_factor_df=pd.DataFrame.from_dict(new_contributing_factor_dict, orient="index").reset_index()
contributing_factor_df.columns=['contributing_factor_id','contributing_factor']


# In[131]:


vehicle_map = {y:x for x,y in new_vehicle_dict.items()}
df["vehicle_type_code1"].replace(vehicle_map, inplace=True)
df["vehicle_type_code2"].replace(vehicle_map, inplace=True)


# In[132]:


street_replace = {y:x for x,y in new_street_names_dict.items()}
df["on_street_name"].replace(street_replace, inplace=True)
df["cross_street_name"].replace(street_replace, inplace=True)


# In[133]:


factor_replace = {y:x for x,y in new_contributing_factor_dict.items()}
df["contributing_factor_vehicle_1"].replace(factor_replace, inplace=True)


# In[134]:


#rename variables
collision_df=df.rename(columns={
     "on_street_name" :"on_street_name_id",
     "cross_street_name": "cross_street_name_id",
     "contributing_factor_vehicle_1": "contributing_factor_vehicle_1_id",
     "vehicle_type_code1": "vehicle_type_code1_id",
     "vehicle_type_code2": "vehicle_type_code2_id",
    })


# In[135]:


##LOAD

import sqlalchemy
from sqlalchemy import *

#creates SQL engine and loads data into database
engine = create_engine('sqlite:///data_engineering_workshop.db')

# Load to database (replace table if it exists)
vehicle_df.to_sql(name = 'vehicles', con = engine, if_exists = 'replace',index = False)
street_df.to_sql(name = 'streets', con = engine, if_exists = 'replace',index = False)
contributing_factor_df.to_sql(name = 'factors', con = engine, if_exists = 'replace',index = False)
collision_df.to_sql(name='collisions', con=engine, if_exists='append',index=False)


# In[136]:


#begin querying the dataset
connection=engine.connect()

#SELECTS street id, name and number of persons injured orders greatest to least
#JOIN collisions table to the streets table to link the ID's

query="SELECT on_street_name_id, streets.street_name,number_of_persons_injured FROM collisions LEFT JOIN streets ON collisions.on_street_name_id = streets.street_id ORDER BY number_of_persons_injured DESC"


# In[137]:


results=connection.execute(query).fetchall()


# In[139]:


results[0:15]


# In[ ]:




