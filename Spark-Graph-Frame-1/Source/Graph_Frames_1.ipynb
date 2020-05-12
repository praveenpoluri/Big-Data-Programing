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


# In[4]:


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


# In[5]:


from graphframes import *


# In[6]:


import pyspark


# In[7]:


from pyspark import *
from pyspark.sql import *


# In[8]:


import findspark
findspark.init("/content/spark-2.3.4-bin-hadoop2.7")


# In[9]:


sc=SparkContext().getOrCreate()


# In[42]:


sqlcontext=SQLContext(sc)


# In[43]:


df_station=sqlcontext.read.format("csv").option("header","true").csv("201508_station_data.csv")


# In[44]:


df_trip=sqlcontext.read.format("csv").option("header","true").csv("201508_trip_data.csv")


# In[45]:


df_station.show


# In[46]:


df_station.registerTempTable("station")
df_trip.registerTempTable("trip")


# In[40]:


sqlcontext.sql('select * from station').show()


# In[41]:


sqlcontext.sql('select * from trip').show()


# In[47]:


sqlcontext.sql('select concat(cast(lat as string)," ",cast(long as String)) as lat_long from station').show()


# In[48]:


sqlcontext.sql('select station_id, name as id, lat,long,dockcount,landmark,installation from station group by station_id,name,lat,long,dockcount,landmark,installation').show()


# In[49]:


vertices=sqlcontext.sql('select name as id, concat(cast(lat as string)," ",cast(long as String)) as lat_long, dockcount,installation from station group by station_id,name,lat,long,dockcount,landmark,installation')


# In[50]:


vertices.show()


# In[51]:


edges=sqlcontext.sql('select `Start Station` as src,`End Station` as dst, count(*) as cnt from trip group by `Start Station`,`End Station`')


# In[24]:


edges.show()


# In[52]:



from graphframes import *


# In[ ]:


get_ipython().system('curl -L -o "/usr/local/lib/python3.6/dist-packages/pyspark/jars/graphframes-0.6.0-spark2.3-s_2.11.jar" http://dl.bintray.com/spark-packages/maven/graphframes/graphframes/0.6.0-spark2.3-s_2.11/graphframes-0.6.0-spark2.3-s_2.11.jar')


# In[53]:


graph=GraphFrame(vertices,edges)


# In[54]:


graph.vertices.show()


# In[55]:


graph.edges.show()


# In[56]:


graph.inDegrees.show()


# In[57]:


graph.outDegrees.show()


# In[58]:


graph.find("(a)-[e]->(b);(b)-[e1]->(c)").show(truncate=False)


# In[34]:


sqlcontext.sql("select * from inDegrees order by inDegrees desc").show()

