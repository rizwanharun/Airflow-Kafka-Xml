#!/usr/bin/env python
# coding: utf-8

# In[4]:


from time import sleep
from json import dumps
from kafka import KafkaProducer


# In[6]:


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))


# In[26]:


import xml.etree.ElementTree as ET
tree = ET.parse('d:\\xmls\\simple.xml')
root = tree.getroot()
for elem in root:
    for subelem in elem:
        producer.send('xmldata', subelem.text)
        print(subelem.text)
        sleep(5)


# In[ ]:




