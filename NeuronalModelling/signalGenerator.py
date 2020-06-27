import pandas as pd
import paho.mqtt.client as mqtt
import time
import array as arr

inputFile = "/home/rajeevgangal/myProjects/NeuronalModelling/csv1.csv"

def on_message(client, userdata, message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)
    
def readFileCreateDF(str):
   
    df= pd.read_csv(str, header='infer')   
    
    return df

def splitColumns(newdf):
        client.loop_start() #star
        
        
        for colname in newdf.columns:
            currentCol= newdf[[colname]]
            for myrow in currentCol.index:    
                myval=currentCol[colname][myrow]
                print("Publishing message to topic","house/bulbs/bulb1")
                client.publish("house/bulbs/bulb1",myval,2)
                
                 
        client.on_message=on_message  
        

broker_address="test.mosquitto.org"
client = mqtt.Client("imacLinux") #create new instance
print("connecting to broker")
client.connect(broker_address) #connect to broker
print("Subscribing to topic","house/bulbs/bulb1")
client.subscribe("house/bulbs/bulb1")
time.sleep(3)
  
newdf= readFileCreateDF(inputFile)
splitColumns(newdf)
client.loop_stop() #stop 
#    
#    
