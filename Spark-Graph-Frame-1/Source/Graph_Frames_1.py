#!/usr/bin/env python
# coding: utf-8

# In[ ]:


pip install graphframes


# In[ ]:


pip install pyspark==2.3.0


# In[1]:


from pyspark import SparkContext


# In[2]:


from graphframes import *


# In[3]:


from pyspark import *
from pyspark.sql import *


# In[ ]:


pip install findspark


# In[ ]:


import findspark
import os
os.getcwd()


# In[ ]:


get_ipython().system('apt-get install openjdk-8-jdk-headless -qq > /dev/null')
get_ipython().system('wget -q http://www-eu.apache.org/dist/spark/spark-2.3.4/spark-2.3.4-bin-hadoop2.7.tgz')
get_ipython().system('tar xf spark-2.3.4-bin-hadoop2.7.tgz')
get_ipython().system('pip install -q findspark')


# In[ ]:


get_ipython().system(' ls /usr/lib/jvm/')


# In[ ]:


pip install -U pyarrow


# In[ ]:


import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.3.4-bin-hadoop2.7"


# In[4]:


from graphframes import *


# In[5]:


import pyspark


# In[6]:


from pyspark import *
from pyspark.sql import *


# In[ ]:


import findspark
findspark.init("/content/spark-2.3.4-bin-hadoop2.7")


# In[11]:


sc=SparkContext().getOrCreate()


# In[12]:


sqlcontext=SQLContext(sc)


# In[14]:


df_station=sqlcontext.read.format("csv").option("header","true").csv("201508_station_data.csv")


# In[15]:


df_trip=sqlcontext.read.format("csv").option("header","true").csv("201508_trip_data.csv")


# In[ ]:


df_station.show


# In[16]:


df_station.registerTempTable("station")
df_trip.registerTempTable("trip")


# In[17]:


sqlcontext.sql('select * from station').show()


# In[18]:


sqlcontext.sql('select * from trip').show()


# In[19]:


sqlcontext.sql('select concat(cast(lat as string)," ",cast(long as String)) as lat_long from station').show()


# In[20]:


sqlcontext.sql('select station_id, name as id, lat,long,dockcount,landmark,installation from station group by station_id,name,lat,long,dockcount,landmark,installation').show()


# In[35]:


vertices=sqlcontext.sql('select name as id, concat(cast(lat as string)," ",cast(long as String)) as lat_long, dockcount,installation from station group by station_id,name,lat,long,dockcount,landmark,installation')


# In[36]:


vertices.show()


# In[23]:


edges=sqlcontext.sql('select `Start Station` as src,`End Station` as dst, count(*) as cnt from trip group by `Start Station`,`End Station`')


# In[24]:


edges.show()


# In[25]:



from graphframes import *


# In[ ]:


get_ipython().system('curl -L -o "/usr/local/lib/python3.6/dist-packages/pyspark/jars/graphframes-0.6.0-spark2.3-s_2.11.jar" http://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.6.0-spark2.3-s_2.11/graphframes-0.6.0-spark2.3-s_2.11.jar')


# In[37]:


graph=GraphFrame(vertices,edges)


# In[38]:


graph.vertices.show()


# In[ ]:




