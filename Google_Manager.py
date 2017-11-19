"""

TO BE IMPLEMENTED

"""

import sys

def print_exception ():
    exc_type, exc_obj, exc_tb = sys.exc_info()
    print exc_type
    print exc_obj
    print exc_tb.tb_lineno   
    
from threading import Thread

import datetime
import time

import googlemaps

#import logging
#logging.basicConfig(filename= "./Logs/" + datetime.datetime.now().strftime("%Y-%m-%d") + ".log", 
#                    level=logging.DEBUG)        

import requests
import json

from DataBaseProxy import dbp

import pandas as pd

class Google_Manager(Thread):
    
    def __init__ (self, city):
        
        Thread.__init__(self)
        self.daemon = True
        
        self.name = "google"
        self.city = city
        
        self.keys = pd.Series(["key1","key2"])
 
        self.current_key = 0
        self.start_session()

        doc = dbp.find_last("bookings",
                            {"city":self.city}).next()
        self.last_time = doc["timestamp"]
        self.last = pd.DataFrame(doc["bookings"])
       
    def log_message(self, scope, status):
        return '[{}] -> {} {}: {} {}'\
                    .format(datetime.datetime.now(),\
                            self.name,\
                            self.city,\
                            scope,\
                            status)  
   
    def start_session (self):
        print self.keys.iloc[self.current_key]
        try:
            self.gmaps = googlemaps.Client(key=self.keys.iloc[self.current_key])
            self.session_start_time = datetime.datetime.now()
            message = self.log_message("session","success")
        except:
            message = self.log_message("session","error")
        print message
 
            
    def get_directions (self, booking):

        try:
            directions_result = self.gmaps.directions\
                ([booking['start_latitude'], booking['start_longitude']],
                  [booking['end_latitude'], booking['end_longitude']],
                  mode="transit",
                  departure_time = datetime.datetime.now())
            message = self.log_message("direction_transit","success")
        except:
            message = self.log_message("direction_transit","error")
        print message
                
    def to_DB (self):
    
        dbp.insert("snapshots", 
                   {
                     "timestamp": datetime.datetime.now(),
                     "provider": self.name,
                     "city": self.city,
                     "snapshot": self.current_feed
                     }
                    )

    def run (self):
        
        while True:

            doc = dbp.find_last("bookings",
                                {"city":self.city}).next()
            self.current_time = doc["timestamp"]
            self.current = pd.DataFrame(doc["bookings"])

            if not self.current.equals(self.last):
                print self.current

            self.last = self.current
            time.sleep(10)

#Google_Manager("torino").start()