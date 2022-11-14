import requests
import json
from paho.mqtt import client as mqtt_client


def data_injector():
    # Target URL
    url = "http://uoweb3.ncl.ac.uk/api/v1.1/sensors/PER_AIRMON_MONITOR1135100"\
        "/data/json/?starttime=20220601&endtime=20220831"

    # Request data from Urban Observatory Platform
    resp = requests.get(url)

    # Convert response(Json) to dictionary format
    raw_data_dict = resp.json()
    filter_data_dict = {}
    filter_data_dict["PM2.5"] = []
    for raw_data in raw_data_dict["sensors"][0]["data"]["PM2.5"]:
        filter_data = {}
        filter_data["Timestamp"] = raw_data["Timestamp"]
        filter_data["Value"] = raw_data["Value"]
        filter_data_dict["PM2.5"].append(filter_data)

    data = json.dumps(filter_data_dict)
    with open("./task1.json", 'w') as f:
        f.write(data)
    return data


def MQTT_publisher(msg):
    mqtt_ip = "localhost"
    mqtt_port = 1883
    topic = "CSC8112"
    # Create a mqtt client object
    client = mqtt_client.Client()

    # Callback function for MQTT connection
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT OK!")
        else:
            print("Failed to connect, return code %d\n", rc)
    # Connect to MQTT service
    client.on_connect = on_connect
    client.connect(mqtt_ip, mqtt_port)
    client.publish(topic, msg)


if __name__ == '__main__':
    msg = data_injector()
    MQTT_publisher(msg)
