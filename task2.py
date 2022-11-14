import json
from paho.mqtt import client as mqtt_client
import pika

def step2_filter_out(data):
    pass

def step3_calculate_average(data):
    pass

def step4_transfer_data_to_rabbitmq(msg):
    rabbitmq_ip = "localhost"
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
                          body=msg)
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
            print("Connected to MQTT OK!")
        else:
            print("Failed to connect, return code %d\n", rc)

    # Connect to MQTT service
    client.on_connect = on_connect
    client.connect(mqtt_ip, mqtt_port)

    # Callback function will be triggered
    def on_message(client, userdata, msg):
        raw_data_dict=json.loads(msg.payload)
        print(f"Get raw message from publisher:\n {raw_data_dict}")
        step2_filter_out(raw_data_dict)
        step3_calculate_average(raw_data_dict)
        step4_transfer_data_to_rabbitmq(msg.payload)
        # with open("task2.json",'wb+') as f:
        #     f.write(msg.payload) 

    # Subscribe MQTT topic
    client.subscribe(topic)
    client.on_message = on_message

    # Start a thread to monitor message from publisher
    client.loop_forever()


if __name__ == '__main__':
    step1_grab_data_from_MQTT()