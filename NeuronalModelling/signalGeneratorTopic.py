#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 27 17:09:00 2020

@author: rajeevgangal
"""

import pandas as pd
import paho.mqtt.client as mqtt



inputFile = "/home/rajeevgangal/myProjects/NeuronalModelling/csv1.csv"

def on_message(client, userdata, message):   # print received messages , topic and payload
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
   # print("message qos=",message.qos)
    #print("message retain flag=",message.retain)
    
def readFileCreateDF(str):
   
    df= pd.read_csv(str, header='infer')   
    
    return df

def splitColumnsSendSignal(newdf): #split input csv , form a Topic hierarchy and sendSignal
        client.loop_start() #star
        
        
        for colname in newdf.columns:
            currentCol= newdf[[colname]]
            for myrow in currentCol.index:    
                myval=currentCol[colname][myrow]
                TopicString = type(myval).__name__ + "/" + colname + "/Row" + str(myrow) #form mqtt Topic Hierarchy
                print(TopicString)
                print("Publishing message to topic", TopicString)
                client.publish(TopicString,myval,2)
                
         
        client.on_message=on_message  #callback function
        
def CreateTopicSubscribe(newdf):   #Form same dynamc topic hierarchy and subcrive
     for colname in newdf.columns:
            currentCol= newdf[[colname]]
            for myrow in currentCol.index:    
                myval=currentCol[colname][myrow]
                TopicString = type(myval).__name__ + "/" + colname + "/Row" + str(myrow) 
                print(TopicString)
                print("Subscribing to topic",TopicString)
                client.subscribe(TopicString)

                
                
broker_address="test.mosquitto.org"
client = mqtt.Client("imacLinux") #create new instance
print("connecting to broker")
client.connect(broker_address) #connect to broker

newdf= readFileCreateDF(inputFile)
CreateTopicSubscribe(newdf)
splitColumnsSendSignal(newdf)
  
client.loop_stop() #stop 
#    
#    
