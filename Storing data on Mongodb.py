#!/usr/bin/env python
# coding: utf-8

# In[1]:


from kafka import KafkaConsumer
from json import loads
import json
from pykafka import KafkaClient
from pymongo import MongoClient
import pymongo


# In[6]:


#consumer
client=KafkaClient(hosts="localhost:9093")
client.topics
topic = client.topics['datapipe']


# In[7]:


clien=pymongo.MongoClient("mongodb+srv://bharath_23:nanda8189N23@cluster0-l5wpk.mongodb.net/test?retryWrites=true&w=majority")


# In[8]:


mydb = clien["Newyorkcabs"]
collect = mydb["Ride_details"]


# In[ ]:


consumer = topic.get_simple_consumer()
for message in consumer:
    message_json=json.loads(message.value.decode("utf-8"))
    print(collect.insert_one(message_json).inserted_id)
    


# In[ ]:




