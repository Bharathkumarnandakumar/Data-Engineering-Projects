#!/usr/bin/env python
# coding: utf-8

# In[1]:


from kafka import KafkaConsumer
from json import loads
import json
from pykafka import KafkaClient


# In[2]:


#consumer
client=KafkaClient(hosts="localhost:9093")
client.topics
topic = client.topics['datapipe']


# In[6]:


#consumer
consumer = topic.get_simple_consumer(consumer_timeout_ms=1000,auto_offset_reset='earliest')
for message in consumer:
    if message is not None:
        print (message.offset, message.value,message.partition)


# In[ ]:




