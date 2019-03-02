
# coding: utf-8

# In[9]:


import pandas as pd
from sodapy import Socrata


app_token = "6mndJ6dQB2dsu0CXE1RFz9AGA"


# In[10]:


url="https://data.cityofnewyork.us/resource/qiz3-axqb.json"


# In[13]:


df=pd.read_json(url)


# In[14]:


df.head()


# In[16]:


df.columns


# In[27]:


df=df.astype(str)

df["datetime"]=pd.to_datetime(df['date']+' '+df['time'], errors='coerce')

df['on_sreet_name'][0]
df.columns
df.head()


# In[24]:


df['datetime']


# In[28]:


df=df.apply(lambda x: x.str.strip() if x.dtype =='object' else x)
df.head()


# In[31]:


columns_i_want = ['datetime', 'unique_key', 'latitude', 'longitude', 'on_street_name', 'cross_street_name',
                 'number_of_persons_injured','contributing_factor_vehicle_1','vehicle_type_code1', 'vehicle_type_code2']

df=df[columns_i_want]
df.head()


# In[33]:


df['vehicle_type_code1'].unique()


# In[34]:


df['vehicle_type_code2'].unique()


# In[93]:



street_names=set(list(df['on_street_name'].unique())+list(df['cross_street_name'].unique()))

contributing_factor=list(df['contributing_factor_vehicle_1'].unique())
        


# In[94]:


vehicle_type_ids=list(range(1,len(vehicle_types)+1))
vehicle_types=set(list(df['vehicle_type_code1'].unique())+list(df['vehicle_type_code2'].unique()))
vehicle_type_dictionary=dict(list(zip(vehicle_type_ids,vehicle_types)))

street_name_ids=list(range(1,len(street_names)+1))
street_names_dictionary=dict(list(zip(street_name_ids,street_names)))

contributing_factor_ids=list(range(1,len(contributing_factor)+1))
contributing_factor_dictionary=dict(list(zip(contributing_factor_ids,contributing_factor)))


# In[85]:


vehicle_df=pd.DataFrame.from_dict(vehicle_type_dictionary, orient="index").reset_index()
#vehicle_df=pd.DataFrame(vehicle_type_dictionary, index=[0])
vehicle_df.columns=['vehicle_id','vehicle_type']
vehicle_df.head()

street_df=pd.DataFrame.from_dict(street_names_dictionary, orient="index").reset_index()
#vehicle_df=pd.DataFrame(vehicle_type_dictionary, index=[0])
street_df.columns=['street_id','street_name']
street_df.head()

contributing_factor_df=pd.DataFrame.from_dict(contributing_factor_dictionary, orient="index").reset_index()
#vehicle_df=pd.DataFrame(vehicle_type_dictionary, index=[0])
contributing_factor_df.columns=['contributing_factor_id','contributing_factor']
contributing_factor_df.head()


# In[78]:


df


# In[98]:


df=df.applymap(str) #forces everything to be a string
street_replace={y:x for x,y in street_names_dictionary.items()}
df['on_street_name'].replace(street_replace, inplace=True)
df['cross_street_name'].replace(street_replace, inplace=True)
df.head()


# In[101]:


import sqlalchemy
from sqlalchemy import *

engine = create_engine('sqlite:///data_engineering_workshop.db')
#Base=declarative_base()

df.to_sql(name='collisions', con=engine, if_exists='append',index=False)

