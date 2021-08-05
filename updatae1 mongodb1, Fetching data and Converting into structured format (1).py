#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pymongo 
import pandas as pd


# In[4]:


clien=pymongo.MongoClient("mongodb+srv://bharath_23:nanda8189N23@cluster0-l5wpk.mongodb.net/test?retryWrites=true&w=majority")

mydb = clien["Newyorkcabs"]
collect = mydb["Ride_details"]


# In[5]:


x = collect.find_one()
print(x)


# In[11]:


# Fields with values as 1 will# only appear in the result 
x = collect.find({},{'_id': 0, 'fare_amount': 1, 'trip_distance': 1, 'total_amount': 1, 'tip_amount':1}) 

for data in x: 
    print(data)


# In[19]:



samples=collect.find().sort("_id",1)
df=pd.DataFrame(samples)

df.head()


# In[ ]:





# In[ ]:





# In[8]:


df2=df.to_csv(r'C:/Users/91893/Desktop/SRH project works module 1/fetchmongodata.csv',index=False)


# In[ ]:





# In[ ]:





# In[ ]:




