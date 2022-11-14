import json
import pandas as pd
import time
from paho.mqtt import client as mqtt_client
import pika


def step2_filter_out(data):
    data = data["PM2.5"]
    print("After filter out data:", flush=True)
    for item in data:
        if int(item['Value']) > 50:
            print("{}".format(item), flush=True)
    data_pd = pd.DataFrame(data)
    data_pd = data_pd[data_pd['Value'] < 50]
    return data_pd


def step3_calculate_average(data_pd):
    data_pd["Time"] = data_pd["Timestamp"].apply(
        lambda x: time.strftime("%Y-%m-%d", time.localtime(x / 1000))
    )
    data_pd_mean = pd.DataFrame(data_pd.groupby("Time").mean())
    data_pd_mean.reset_index(inplace=True)
    data = data_pd_mean.loc[:, ["Time", "Value"]]
    print(" averaging value of PM2.5 data:", flush=True)
    print(data, flush=True)
    return data


def step4_transfer_data_to_rabbitmq(msg):
    rabbitmq_ip = "192.168.0.100"
    rabbitmq_port = 5672
    # Queue name
    rabbitmq_queque = "CSC8112"

    # Connect to RabbitMQ service
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_ip, port=rabbitmq_port))
    channel = connection.channel()

    # Declare a queue
    channel.queue_declare(queue=rabbitmq_queque)

    # Produce message
    channel.basic_publish(exchange='',
                          routing_key=rabbitmq_queque,
                          body=json.dumps(msg))
    connection.close()


def step1_grab_data_from_MQTT():
    mqtt_ip = "localhost"
    mqtt_port = 1883
    topic = "CSC8112"
    # Create a mqtt client object
    client = mqtt_client.Client()

    # Callback function for MQTT connection
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT OK!", flush=True)
        else:
            print("Failed to connect, return code %d\n", rc, flush=True)

    # Connect to MQTT service
    client.on_connect = on_connect
    client.connect(mqtt_ip, mqtt_port)

    # Callback function will be triggered
    def on_message(client, userdata, msg):
        raw_data_dict = json.loads(msg.payload)
        print("Get raw message from publisher:", flush=True)
        print(raw_data_dict["PM2.5"], flush=True)
        data_filter_out_pd = step2_filter_out(raw_data_dict)
        data_mean_pd = step3_calculate_average(data_filter_out_pd)
        step4_transfer_data_to_rabbitmq(data_mean_pd.to_dict())

    # Subscribe MQTT topic
    client.subscribe(topic)
    client.on_message = on_message

    # Start a thread to monitor message from publisher
    client.loop_forever()


if __name__ == '__main__':
    step1_grab_data_from_MQTT()
