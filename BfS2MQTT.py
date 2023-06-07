# python 3.6
from tendo import singleton
import random
import time
from paho.mqtt import client as mqtt_client
import requests
import json

broker = 'localhost'
port = 1883
client_id = 'BfS2MQTT'
username = ''
password = '' 
str_bfsURL = "https://www.imis.bfs.de/ogc/opendata/ows?service=WFS&version=1.1.0&request=GetFeature&typeName=opendata:odlinfo_timeseries_odl_1h&outputFormat=application/json&viewparams=kenn:"
str_bfsKennung = "057700040"
str_bfsFilter = "&sortBy=end_measure+D&maxFeatures=1"
prefix = "/home/data/bfs/"
me = singleton.SingleInstance()
def myping(host):
    resp = ping(host)

    if resp == False:
        return False
    else:
        return True

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client):
        str_req = str_bfsURL + str_bfsKennung + str_bfsFilter
        print(str_req)
        r = requests.get(str_req)
        datei = open('BfS2MQTT.json','w')
        datei.write(r.text)
        datei.close()
        with open("BfS2MQTT.json") as jsonFile:
            jsonObject = json.load(jsonFile)
            jsonFile.close()
            name = jsonObject["features"][0]["properties"]["name"]
            id = jsonObject["features"][0]["properties"]["id"]
            kenn = jsonObject["features"][0]["properties"]["kenn"]
            start_measure = jsonObject["features"][0]["properties"]["start_measure"]
            end_measure = jsonObject["features"][0]["properties"]["end_measure"]
            value = jsonObject["features"][0]["properties"]["value"]
            unit = jsonObject["features"][0]["properties"]["unit"]
            print(name)
            print(id)
            print(kenn)
            print(start_measure)
            print(end_measure)
            print(value)
            print(unit)
            str_name = str(prefix) + str(id) + "/name"
            str_id = str(prefix) + str(id) + "/id"
            str_kenn = str(prefix) + str(id) + "/kenn"
            str_start = str(prefix) + str(id) + "/start_measure"
            str_end = str(prefix) + str(id) + "/end_measure"
            str_value = str(prefix) + str(id) + "/value"
            str_unit = str(prefix) + str(id) + "/unit"
        client.publish(str_name, name)
        client.publish(str_id, id)
        client.publish(str_kenn, kenn)
        client.publish(str_kenn, start_measure)
        client.publish(str_end, end_measure)
        client.publish(str_value, value)
        client.publish(str_unit, unit)
        time.sleep(20) 

def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)

if __name__ == '__main__':
    run()
