import paho.mqtt.client as mqtt
import time
import array as arr
############
def on_message(client, userdata, message):
    print("message received " ,str(message.payload.decode("utf-8")))
    print("message topic=",message.topic)
    print("message qos=",message.qos)
    print("message retain flag=",message.retain)
########################################
broker_address="test.mosquitto.org"
#broker_address="iot.eclipse.org"
print("creating new instance")
signal1 = arr.array('d',[3,11,4,13, 7])
client = mqtt.Client("imacLinux") #create new instance
client.on_message=on_message #attach function to callback
print("connecting to broker")
client.connect(broker_address) #connect to broker
for i in signal1:
    client.loop_start() #start the loop
    print("Subscribing to topic","house/bulbs/bulb1")
    client.subscribe("house/bulbs/bulb1")
    print("Publishing message to topic","house/bulbs/bulb1")
    client.publish("house/bulbs/bulb1","On")
    time.sleep(i) # wait
    client.loop_stop() #stop the loop
