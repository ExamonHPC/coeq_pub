#!/home/examon/.venv/bin/python

import paho.mqtt.client as mqtt
import time
import socket
from coe_calculator import get_COE
import logging
import sys
import configparser


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Load configuration from co2_pub.conf
config = configparser.ConfigParser()
config.read('co2_pub.conf')


hostname = socket.gethostname()
BROKER = config.get('MQTT', 'BROKER', fallback='127.0.0.1')
PORT = config.getint('MQTT', 'PORT', fallback=1883)
TOPIC_TEMPLATE = config.get('MQTT', 'TOPIC_TEMPLATE', fallback='org/unibo/cluster/hifive/node/{hostname}/plugin/coe_calulator/chnl/data/carbon_intensity')
TOPIC = TOPIC_TEMPLATE.format(hostname=hostname)

def get_value():
    value = "{:.2f}".format(get_COE())
    return value
def publish_message(client,value):
    current_timestamp = time.time()
    client.publish(TOPIC,f"{value};{current_timestamp}")
    logging.info(f"Published {value};{current_timestamp} to topic {TOPIC}")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

try:
    while True:
            
        value = get_value()
        for _ in range(30):
            if not client.is_connected():
                client.connect(BROKER, PORT)
            publish_message(client,value)
            time.sleep(120)  
except KeyboardInterrupt:
    print("Script stopped by user.")
finally:
    client.disconnect()
