import datetime

from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')

import logging
logging.basicConfig(filename=datetime.datetime.now().strftime("%Y-%m-%d") + ".log", 
                    level=logging.DEBUG)

class DataBaseProxy (object):
    
    def __init__ (self):
        
        self.db = client['UMAP']

    def log_message (self, record, scope, status):
        
        return '[{}] -> {} {}: {} {}'\
                    .format(datetime.datetime.now(),\
                            record["provider"],\
                            record["city"],\
                            scope,\
                            status)

    def insert (self, collection, record):
    
        collection = self.db[collection]
        try:
            collection.insert_one(record)
            logging.debug(self.log_message(record, "insert", "success"))
        except:
            logging.debug(self.log_message(record, "insert", "error"))
            
    def query (self, collection, query):

        return self.db[collection].find(query)

    def find_last (self, collection, query):
        
        return self.db[collection].find \
                    (query).sort([("_id", -1)]).limit(1)
    
    
dbp = DataBaseProxy()

import datetime
import pandas as pd
import numpy as np

import matplotlib
import matplotlib.pyplot as plt
matplotlib.style.use('ggplot')

#cursor = dbp.query("bookings", {"city":"vancouver"})
#df = pd.concat([pd.DataFrame(doc["bookings"]).T for doc in cursor])
#df["car_id"] = df.index.values
#df = df.set_index("start_time")

start = datetime.datetime(2017, 10, 31)
cursor = dbp.query("bookings", {"city":"madrid", "timestamp":{"$gt": start}})
df = pd.concat([pd.DataFrame(doc["bookings"]).T for doc in cursor])
df["car_id"] = df.index.values
df["hour"] = df["start_time"].apply(lambda d: d.hour)
df = df.set_index("start_time")
df["fuel_consumption"] = df.end_fuel - df.start_fuel

plt.figure()
df.loc[df.fuel_consumption > 0].groupby("hour").fuel_consumption.count().plot(marker="o")

plt.figure()
df.loc[df.fuel_consumption <= 0].groupby("hour").fuel_consumption.count().plot(marker="o")  
  
plt.figure()
df.loc[df.fuel_consumption > 0].groupby("hour").fuel_consumption.sum().plot(marker="o")
df.loc[df.fuel_consumption <= 0].groupby("hour").fuel_consumption.sum().apply(np.abs).plot(marker="o")

plt.figure()
df.loc[df.fuel_consumption > 0].groupby("hour").fuel_consumption.apply(np.mean).plot(marker="o")
df.loc[df.fuel_consumption <= 0].groupby("hour").fuel_consumption.apply(np.mean).apply(np.abs).plot(marker="o")

plt.figure()
df.loc[df.fuel_consumption > 0].groupby("hour").fuel_consumption.apply(np.sum).hist(bins=50, cumulative=True)

plt.figure()
df.loc[df.fuel_consumption <= 0].groupby("hour").fuel_consumption.apply(np.sum).hist(bins=50, cumulative=True)

plt.figure()
df.loc[df.fuel_consumption > 0].groupby("hour").fuel_consumption.apply(np.mean).hist(bins=50, cumulative=True)

plt.figure()
df.loc[df.fuel_consumption <= 0].groupby("hour").fuel_consumption.apply(np.mean).hist(bins=50, cumulative=True)
