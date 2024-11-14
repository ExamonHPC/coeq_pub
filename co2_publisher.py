#!/home/examon/.venv/bin/python
import paho.mqtt.client as mqtt
import time
import socket
from coe_calculator import get_COE

hostname = socket.gethostname()
BROKER = '127.0.0.1'
PORT = 1883
TOPIC = f'org/unibo/cluster/hifive/node/{hostname}/plugin/coe_calulator/chnl/data/carbon_intensity'

def get_value():
    value = "{:.2f}".format(get_COE())
    return value
def publish_message(client,value):
    current_timestamp = time.time()
    client.publish(TOPIC,f"{value};{current_timestamp}")
    print(f"Published {value};{current_timestamp} to topic {TOPIC}")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.connect(BROKER, PORT)

try:
    while True:
        value = get_value()
        for _ in range(2):
            publish_message(client,value)
            time.sleep(1800)  
except KeyboardInterrupt:
    print("Script stopped by user.")
finally:
    client.disconnect()
