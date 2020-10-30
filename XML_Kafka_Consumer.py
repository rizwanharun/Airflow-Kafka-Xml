#!/usr/bin/env python
# coding: utf-8

# In[1]:


from kafka import KafkaConsumer
from json import loads


# In[ ]:


xmlmessage = KafkaConsumer(
    'xmldata',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))


# In[ ]:


for message in xmlmessage:
    message = message.value
    print('{} added'.format(message))

