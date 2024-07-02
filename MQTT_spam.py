import time
import threading
import paho.mqtt.client as mqtt
import random 
import os
from dotenv import load_dotenv, find_dotenv

# Laden der umgebungsabhängigen Konfigurationsparameter
load_dotenv(find_dotenv('Config.env'))

# MQTT Broker Konfiguration
mqtt_server = os.getenv('MQTT_SERVER')
mqtt_port = int(os.getenv('MQTT_PORT'))
mqtt_topic = os.getenv('MQTT_TOPIC')
mqtt_username = os.getenv('MQTT_USER')
mqtt_password = os.getenv('MQTT_PW')
messages_per_second = 100

# MAC-Adressen aus der Datei laden
def load_mac_addresses(file_path):
    with open(file_path, 'r') as file:
        mac_addresses = [line.strip() for line in file.readlines()]
    return mac_addresses

mac_addresses = load_mac_addresses("C:/users/linus/downloads/beacon_MAC.txt")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unerwarteter Verbindungsverlust zum MQTT-Broker.")
        while True:
            try:
                print("Versuche, die Verbindung zum MQTT-Broker wiederherzustellen...")
                client.reconnect()
                break
            except Exception as e:
                print(f"Verbindung fehlgeschlagen: {e}")
                time.sleep(5)

def publish_messages(client):
    i = 1
    while True:
        for _ in range(messages_per_second):
            mac_index = (i - 1) % len(mac_addresses)
            mac_address = mac_addresses[mac_index]
            randRSSI = random.randint(100,250)
            if i % 4 == 0:
                lastNum = random.randint(1, 9)
            else:
                lastNum = 2
            if i % 44 == 0 or i % 45 == 0:
                randButton = random.randint(0, 1)
            else:
                randButton = 0
            message_content = f"layer=1, MAC_ROOM=00:00:00:00:00:0{lastNum}, MAC_SENSOR={mac_address}, BATT=100, BUTTON={randButton}, RSSI={randRSSI}"
            
            client.publish(mqtt_topic, message_content)
            print(message_content)
            i += 1
        time.sleep(1)

def main():
    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set(mqtt_username, mqtt_password)
    mqtt_client.on_disconnect = on_disconnect

    while True:
        try:
            mqtt_client.connect(mqtt_server)
            publish_thread = threading.Thread(target=publish_messages, args=(mqtt_client,))
            publish_thread.start()
            mqtt_client.loop_forever()
        except Exception as e:
            print(f"Ein Fehler ist aufgetreten: {e}")
            time.sleep(5)


main()