#!/usr/bin/env python
# coding: utf-8

# In[1]:


from kafka import KafkaProducer
import pandas as pd
import json
import csv
from time import sleep


# In[2]:


df = pd.read_csv (r'C:/Users/bharath desktop files/updated newyrk.csv',dtype=str)
df1=df.to_json (r'C:/Users/91893/Desktop/SRH project works module 1/baywtchjs.json')
input_file=open('C:/Users/91893/Desktop/SRH project works module 1/baywtchjs.json')
json_array=json.load(input_file)


# In[3]:


bootstrap_servers1=['localhost:9092','localhost:9093','localhost:9094']
producer = KafkaProducer(bootstrap_servers=bootstrap_servers1,
                         acks=1,retries = 5,
                         retry_backoff_ms=200,
                         max_in_flight_requests_per_connection=1,
                         buffer_memory=33554432,
                         compression_type='gzip',
                         batch_size=17000,linger_ms=20,
                         api_version=(0, 10, 1),
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# In[5]:


producer.send('datapipe',json_array)


# In[8]:


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
# handle exception

# produce asynchronously with callbacks
producer.send('datapipe',json_array).add_callback(on_send_success).add_errback(on_send_error)

producer.flush()


# In[28]:





# In[ ]:





# In[ ]:




