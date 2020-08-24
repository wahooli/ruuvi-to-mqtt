from ruuvitag_sensor.ruuvi_rx import RuuviTagReactive
import json
import time
import sys
import signal
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish


with open("config.json") as json_config:
    config = json.load(json_config)

macs = [d["mac"].upper() for d in config["sensors"] if "mac" in d]
ruuvi_rx = RuuviTagReactive(macs)
sensormap = {}

for sensor in config["sensors"]:
    sensormap[sensor["mac"].upper()] = {"name": sensor.get("name", sensor["mac"].upper()), "configured": 0 } 

measurementUnits = {
    "temperature" : "°C",
    "humidity" : "hPa",
    "battery" : "V"
}

ignoredValues = ["data_format", "movement_counter", "measurement_sequence_number", "tx_power", "time"]


# CTRL-C handling
def signal_handler(signal, frame):
    print("\nExiting.")
    ruuvi_rx.stop()
    client.disconnect()
    sys.exit(0)

# The callback for when the client receives a CONNACK response from the MQTT server.
def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with result code "+str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("$SYS/#")

signal.signal(signal.SIGINT, signal_handler)
userdetails = {"username": config["mqtt"]["username"], "password": config["mqtt"]["password"]}
broker = config["mqtt"]["host"]
stateTopic = config["mqtt"]["topic_prefix"]

# creating mqtt connection
client = mqtt.Client()
client.username_pw_set(userdetails["username"], userdetails["password"])
client.on_connect = on_connect
client.connect(broker, 1883, 60)
client.loop_start()

def mqtt_publish(data):
    mac = data[0].upper()
    for removal in ignoredValues:
        if removal in data[1]:
            del data[1][removal]
    if (sensormap[mac]["configured"] == 0):
        mqtt_configure_sensor(mac, data[1])
        sensormap[mac]["configured"] = 1
    # sending the state
    if "battery" in data[1]:
        data[1]["battery"] /= 1000;
    mqtt_send("%s/%s" % (stateTopic, sensormap[mac]["name"]), json.dumps(data[1]))
    time.sleep(config["reportinterval"])

def mqtt_configure_sensor(mac, data):
    name = sensormap[mac]["name"]
    identifier = data["mac"]
    for key in data:
        upper = key[0].upper() + key[1:]
        topic = "homeassistant/sensor/%s/%s/config" % (identifier, key)
        sensorConfig = {
            "unique_id": "ruuvi_%s_%s" % (identifier, key),
            "name": "%s %s" % (name, key),
            "state_topic": "%s/%s" % (stateTopic, name),
            "json_attributes_topic": "%s/%s" % (stateTopic, name),
            "unit_of_measurement": measurementUnits.get(key, ""),
            "value_template": "{{ value_json.%s }}" % (key),
            "device": {
                "identifiers": ["ruuvi_%s" % (identifier)],
                "name": name,
                "sw_version": "Ruuvitag2MQTT v0.1",
                "model": mac,
                "manufacturer": "Ruuvi Innovations Oy"
            }
        }
        mqtt_send(topic, json.dumps(sensorConfig))

def mqtt_send(topic, jsondump):
    publish.single(topic, jsondump, hostname=broker, auth=userdetails)

ruuvi_rx.get_subject().\
        group_by(lambda x: x[0]).\
        subscribe(lambda x: x.sample(5000).subscribe(mqtt_publish))
        
