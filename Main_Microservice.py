import paho.mqtt.client as mqtt  # für die MQTT Verbindung
import mysql.connector  # für die Datenbank-Verbindung
import time  # zur Nutzung von Methoden zur Zeitberechnung
import json  # zum einfachen Separieren der MQTT Nachricht
from pymemcache.client import base  # eine Bibliothek für serverseitigen memcache
import sys  # um Fehlercode bei einem Abbruch zurückzugeben
import os
from dotenv import load_dotenv, find_dotenv

# Laden der ungebungsabhängigen Konfigurationsparameter
load_dotenv(find_dotenv('Config.env'))
# Konfiguration der MySQL-Datenbankverbindung, des Memcached und des MQTT
db_config = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}
db_connection = None    # Globale Variable zum Halten des DB connectors
# MQTT Broker Konfiguration
memcache_server = os.getenv('MEMCACHE_SERVER')
memcache_port = int(os.getenv('MEMCACHE_PORT'))
mqtt_server = os.getenv('MQTT_SERVER')
mqtt_port = int(os.getenv('MQTT_PORT'))
mqtt_topic = os.getenv('MQTT_TOPIC')
#client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client = mqtt.Client()
client.username_pw_set(os.getenv('MQTT_USER'), os.getenv('MQTT_PW'))
# Raumwechsel bei niedrigerem RSSI nach Zeitlücke
room_timegap = int(os.getenv('ROOM_TIMEGAP'))
pairing_timegap = int(os.getenv('TIMEGAP'))
db_update_cycle_beacon = int(os.getenv('DB_UPDATE_CYCLE_BEACON'))
db_update_cycle_hub = int(os.getenv('DB_UPDATE_CYCLE_HUB'))

# Debug Level und Filter Funktion, Debug Meldungen aus dem Text können bitweise auf Leveln definiert werden
debug_level = int(os.getenv('DEBUG_LEVEL'))
debug_count = 0
def do_debug(k):
    temp = debug_level >> (k - 1)
    return temp & 1 != 0

# Serializer zur Memcache Nutzung, erleichten den Zugriff (Strings/Arrays statt bytes).
def json_serializer(key, value):
    return json.dumps(value), 1
def json_deserializer(key, value, flags):
    if flags == 1:
        return json.loads(value)
    raise Exception("Unknown serialization format")

# Verbindung zur Datenbank herstellen
def connect_to_db():
    try:
        return mysql.connector.connect(**db_config)
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

# Optimierungsbedarf: Globale Datenbankverbindung, einmal geöffnet für die Laufzeit des Programms
db_connection = connect_to_db()

# Aufbau der globalen Verbindung zum memcached
memcache = base.Client((memcache_server, memcache_port), serializer=json_serializer, deserializer=json_deserializer)

# Initalisieren von relevanten Daten bei Programmstart aus der Maria-DB
def load_initial_data():
    # Initialisieren von Beacondaten mit und ohne MP
    try:
        cursor = db_connection.cursor(dictionary=True)
        query = ''' SELECT * from beacon_left_join_mp '''
        cursor.execute(query)
        beacon_mp_cache = cursor.fetchall()
        for data_values in beacon_mp_cache:
            if isinstance(data_values, dict):
                beacon_id = data_values['beacon_id']
                beacon_hub_id = data_values['beacon_hub_id']
                beacon_rssi = data_values['beacon_RSSI']
                beacon_timestamp = data_values['beacon_timestamp']
                beacon_hub_ts_beginn = data_values['beacon_hub_ts_beginn']
                beacon_batterie = data_values['beacon_batterie']
                mp_mp_typ_id = data_values['mp_mp_typ_id']  # Null, falls noch nicht zugeordnet
                beacon_MAC = data_values['beacon_MAC']
                beacon_db_sync_timestamp = data_values['beacon_timestamp']
                beacon_neudaten = (beacon_id, beacon_hub_id, beacon_rssi, beacon_timestamp, beacon_hub_ts_beginn, beacon_batterie, mp_mp_typ_id, beacon_db_sync_timestamp)
                # Falls neu oder aktueller als im Memcache, dort aktualisieren
                if memcache.get(beacon_MAC) is None or memcache.get(beacon_MAC)[7] <= beacon_timestamp:
                    beacon_initial_anlegen(beacon_MAC, beacon_neudaten)
                else:
                    beacon_aktualisieren(beacon_MAC, beacon_neudaten)
            else:
                print(f"Unexpected data format: {data_values}")
    except Exception as e:
        print(f"An error occurred: {e}")

    # Initialisieren von zulässigen MP-Verbindungen (für Beaconpaare relevant)
    try:
        cursor = db_connection.cursor(dictionary=True)
        query = ''' SELECT * from mp_mapping '''
        cursor.execute(query)
        mp_mapping_cache = cursor.fetchall()
        # Aktualisieren des Memcache für alle Paarungen
        for data_values in mp_mapping_cache:
            if isinstance(data_values, dict):
                mp_typ_id1 = data_values['mp_mapping_mp_typ_id_1']
                mp_typ_id2 = data_values['mp_mapping_mp_typ_id_2']
                update_mp_typ_mapping(mp_typ_id1, mp_typ_id2)
            else:    
                print(f"Unexpected data format: {data_values}")
    except mysql.connector.Error as err:
        print(f"Error loading initial data: {err}")

    # Initialisieren von bereits vorhandenen Beaconpaaren
    try:
        cursor = db_connection.cursor(dictionary=True)
        query = ''' SELECT * FROM beaconpair '''
        cursor.execute(query)
        mp_beaconpair_cache = cursor.fetchall()
        # Aktualisieren des Memcache für alle Paarungen
        for data_values in mp_beaconpair_cache:
            if isinstance(data_values, dict):
                beaconpair_beacon_id_1 = int(data_values['beaconpair_beacon_id_1'])
                beaconpair_beacon_id_2 = int(data_values['beaconpair_beacon_id_2'])
                update_beaconpairs(beaconpair_beacon_id_1, beaconpair_beacon_id_2)
            else:
                print(f"Unexpected data format: {data_values}")
    except mysql.connector.Error as err:
        print(f"Error loading initial data: {err}")


def beacon_initial_anlegen(beacon_MAC, beacon_neudaten):
    memcache.set(beacon_MAC, beacon_neudaten)
    return None

# Befüllen des Memcache mit zulässigen Mappings (aus der Datenbank)
# Notation: mp_typ_mapping_mp_typ_ID: [MappedMpID1, MappedMpID2, MappedMpID1...]
def update_mp_typ_mapping(mp_typ_id_1, mp_typ_id_2):
    # Eintrag in der ersten Richtung 
    # Holen bestehender Mappings für Typ 1
    mp_typ_mapping_key = f'mp_typ_mapping_{mp_typ_id_1}'
    mp_typ_mapping_temp = memcache.get(mp_typ_mapping_key)
    if mp_typ_mapping_temp is None: mp_typ_mapping_temp = []
    # Hinzufügen der neuen Erlaubnis und Speichern 
    if mp_typ_id_2 not in mp_typ_mapping_temp: 
        mp_typ_mapping_temp.append(mp_typ_id_2)
        memcache.set(mp_typ_mapping_key, mp_typ_mapping_temp)
    if do_debug(2): print(f"Mapping Data von {mp_typ_id_1} in Memcache: {memcache.get(mp_typ_mapping_key)}")

    # Eintrag in der zweiten Richtung
    mp_typ_mapping_key = f'mp_typ_mapping_{mp_typ_id_2}'
    mp_typ_mapping_temp = memcache.get(mp_typ_mapping_key)
    if mp_typ_mapping_temp is None: mp_typ_mapping_temp = []
    if mp_typ_id_1 not in mp_typ_mapping_temp:
        mp_typ_mapping_temp.append(mp_typ_id_1)
        memcache.set(mp_typ_mapping_key, mp_typ_mapping_temp)
    if do_debug(2): print(f"Mapping Data von {mp_typ_id_2} in Memcache: {memcache.get(mp_typ_mapping_key)}")

# Funktion um ein Beaconpaar einzutragen
# Notation: beaconpairs_BeaconID: [MappedBeaconID1, MappendBeacon_ID2, MappedBeaconID...]
def update_beaconpairs(beacon_id_1, beacon_id_2):
    # Für jedes Beacon die jeweils andere ID in den Pairs hinzufügen
    beaconpairs_key = f'beaconpairs_{beacon_id_1}'
    # Bestehende Paarungen holen.
    beaconpairs_temp = memcache.get(beaconpairs_key)
    if beaconpairs_temp is None: beaconpairs_temp = []
    # Neue ID anhängen und Speichern
    if beacon_id_2 not in beaconpairs_temp: beaconpairs_temp.append(beacon_id_2)
    memcache.set(beaconpairs_key, beaconpairs_temp)
    # Für zweite ID entsprechend
    beaconpairs_key = f'beaconpairs_{beacon_id_2}'
    beaconpairs_temp = memcache.get(beaconpairs_key)
    if beaconpairs_temp is None: beaconpairs_temp = []
    if beacon_id_1 not in beaconpairs_temp: beaconpairs_temp.append(beacon_id_1)
    memcache.set(beaconpairs_key, beaconpairs_temp)

    # Hinzufügen der Beaconpaare in Array für Crawler
    tmp_pair_1 = f"{beacon_id_1},{beacon_id_2}"
    tmp_pair_2 = f"{beacon_id_2},{beacon_id_1}" 
    tmp_beaconpairs = memcache.get('beaconpairs')
    # Gibt es noch keine Paare? Dann leer.
    if tmp_beaconpairs is None: tmp_beaconpairs = []
    # Nur hinzufügen, wenn nicht schon existent
    if tmp_pair_1 not in tmp_beaconpairs and tmp_pair_2 not in tmp_beaconpairs:
        tmp_beaconpairs.append(f"{beacon_id_1},{beacon_id_2}")
        memcache.set('beaconpairs', tmp_beaconpairs)
    if do_debug(2): print(f"Beaconpair Data von {beacon_id_2} in Memcache: {memcache.get(beaconpairs_key)}")


# Hub-Zuordnung zu Beacon in DB und Memcache aktualisieren
def hub_aktualisieren(hub_MAC, timestamp):
    hub_data = memcache.get(hub_MAC)  # Suche im Cache
    if hub_data is not None:  # [0]: id, [1]: ts, [2]: ts des letzten updates der Datenbank
        if do_debug(2): print("Aktualisierung Hub: Bereits im Cache: ", memcache.get(hub_MAC))
        hub_id = hub_data[0]
        # Hub Timestamp in DB aktualisieren, dies dient der Nachvollziehbarkeit, dass der Hub noch aktiv ist
        if hub_data[2] + db_update_cycle_hub < timestamp:  
            if do_debug(4): print("Aktualisierung Hub: Bereits im Cache, aber DB Eintrag zu alt: ", memcache.get(hub_MAC))
            try:
                my_cursor = db_connection.cursor()
                my_query = 'update hub set hub_timestamp = %(ts)s where hub_id = "%(id)s"' % {'ts': timestamp, 'id': hub_id}
                my_cursor.execute(my_query)
                db_connection.commit()
                memcache.set(hub_MAC, [hub_id, timestamp, timestamp])
            except mysql.connector.Error as err:
                print(f"Error updating hub in MySQL: {err}")
        else:
            if hub_data[1] < timestamp:
                memcache.set(hub_MAC, [hub_id, timestamp, hub_data[2]])
                if do_debug(2): print("Aktualisierung Hub: Bereits im Cache, aber neuer TS: ", memcache.get(hub_MAC))
    else:
        # Hub neu/noch nicht im Cache
        try:
            my_cursor = db_connection.cursor()
            my_query = 'Select hub_id, hub_timestamp from hub where hub_MAC = "%(hub)s"' % {'hub': hub_MAC}
            my_cursor.execute(my_query)
            my_result = my_cursor.fetchone()
            if my_result is not None:  # hub gefunden, im memcache mit aktuellem ts eintragen
                hub_id = my_result[0]
                memcache.set(hub_MAC, [hub_id, timestamp, timestamp])
            if do_debug(2): print("Hub noch nicht im Cache. Cache geladen mit: ", memcache.get(hub_MAC))
        except mysql.connector.Error as err:
            print(f"Error querying hub from MySQL: {err}")
    return hub_id

# Holen der zuletzt bekannten Beacondaten. Aus Memcache oder bei nach Start des Programms hinzugekommenen Beacons aus der Datenbank.
def beacon_altdaten_holen(beacon_MAC, timestamp):
    beacon_altdaten = memcache.get(beacon_MAC)
    if do_debug(2): print("beacon-Altdaten im cache: ", beacon_altdaten)
    if beacon_altdaten is None:
        try:
            my_cursor = db_connection.cursor()
            my_query = 'SELECT beacon_id, beacon_hub_id, beacon_RSSI, beacon_timestamp, beacon_hub_ts_beginn, beacon_batterie FROM beacon where beacon_MAC = "%(beacon)s"' % {'beacon': beacon_MAC}
            my_cursor.execute(my_query)
            beacon_altdaten = my_cursor.fetchone()
            if beacon_altdaten is not None:
                # In der Datenbank existiert noch kein Timestamp, zur weiteren Verarbeitung ist er erforderlich
                beacon_altdaten = beacon_altdaten + (timestamp,)
                memcache.set(beacon_MAC, beacon_altdaten)
            if do_debug(2): print("beacon-Altdaten im cache: ", beacon_altdaten)
        except mysql.connector.Error as err:
            print(f"Error querying beacon from MySQL: {err}")
    return beacon_altdaten

# Funktion, die einem existenten Beacon zum ersten Mal einem Hub zuweist.
def beacon_erstspeicherung(beacon_MAC, beacon_neudaten):
    if do_debug(2): print("beacon Neudaten speichern (Erstzuweisung)")
    try:
        my_cursor = db_connection.cursor()
        my_query = 'update beacon set beacon_hub_id = "%(hub_id)s", beacon_RSSI = "%(rssi)s", beacon_timestamp = "%(ts)s", beacon_hub_ts_beginn = "%(ts)s", beacon_batterie = "%(bat)s" where beacon_id = "%(id)s"' % {
            'hub_id': beacon_neudaten[1], 'rssi': beacon_neudaten[2], 'ts': beacon_neudaten[3], 'id': beacon_neudaten[0], 'bat': beacon_neudaten[5]}
        my_cursor.execute(my_query)
        db_connection.commit()
        memcache.set(beacon_MAC, beacon_neudaten)
    except mysql.connector.Error as err:
        print(f"Error inserting new beacon data into MySQL: {err}")

# Beacondaten im Memcache und bei Bedarf (z.B. letzter Eintrag mehr als 10 Minuten alt)
# auch in der DB aktualisieren. Es hat aber kein Hubwechsel stattgefunden.
def beacon_aktualisieren(beacon_MAC, beacon_neudaten):
    if do_debug(2): print("beacon im Vergleich speichern")
    if beacon_neudaten[7] + db_update_cycle_beacon < beacon_neudaten[3]:
        if do_debug(2): print("beacon Update in DB speichern (Timer)")
        try:
            my_cursor = db_connection.cursor()
            my_query = 'update beacon set beacon_hub_id = "%(hub_id)s", beacon_RSSI = "%(rssi)s", beacon_timestamp = "%(ts)s", beacon_hub_ts_beginn = "%(ts)s", beacon_batterie = "%(bat)s" where beacon_id = "%(id)s"' % {
                'hub_id': beacon_neudaten[1], 'rssi': beacon_neudaten[2], 'ts': beacon_neudaten[3], 'id': beacon_neudaten[0], 'bat': beacon_neudaten[5]}
            my_cursor.execute(my_query)
            db_connection.commit()
            beacon_neudaten[7] = beacon_neudaten[3]
        except mysql.connector.Error as err:
            print(f"Error updating beacon data in MySQL: {err}")
    memcache.set(beacon_MAC, beacon_neudaten)
    if do_debug(2): print("beacon im Cache/in DB aktualisiert.")

# Hubwechsel. Beacondaten im Memcache und der DB aktualisieren.
def beacon_hubwechsel(beacon_MAC, beacon_neudaten):
    if do_debug(2): print("beacon zu anderem Hub wechseln (DB und Cache)")
    try:
        my_cursor = db_connection.cursor()
        my_query = 'update beacon set beacon_hub_id = "%(hub_id)s", beacon_RSSI = "%(rssi)s", beacon_timestamp = "%(ts)s", beacon_hub_ts_beginn = "%(ts)s", beacon_batterie = "%(bat)s" where beacon_id = "%(id)s"' % {
            'hub_id': beacon_neudaten[1], 'rssi': beacon_neudaten[2], 'ts': beacon_neudaten[3], 'id': beacon_neudaten[0], 'bat': beacon_neudaten[5]}
        my_cursor.execute(my_query)
        db_connection.commit()
        memcache.set(beacon_MAC, beacon_neudaten)
        if do_debug(2): print("beacon im Cache/in DB aktualisiert.")
    except mysql.connector.Error as err:
        print(f"Error updating beacon hub data in MySQL: {err}")

# Neues Beaconpair in die Datenbank eintragen
def beaconpairing_DB_insert(beacon_id_1, beacon_id_2, hub_id, timestamp):
    try:
        my_cursor = db_connection.cursor()
        insert_query = '''
            INSERT INTO beaconpair (beaconpair_beacon_id_1, beaconpair_beacon_id_2, beaconpair_timestamp, beaconpair_hub_id)
            VALUES (%(beacon_id_1)s, %(beacon_id_2)s, %(timestamp)s, %(hub_id)s)
            '''
        insert_data = {
            'beacon_id_1': min(int(beacon_id_1), int(beacon_id_2)),
            'beacon_id_2': max(int(beacon_id_1), int(beacon_id_2)),
            'timestamp': timestamp,
            'hub_id': hub_id,
            }
        my_cursor.execute(insert_query, insert_data)
        db_connection.commit()
        if do_debug(2): print("Eintrag in die DB-Tabelle 'beaconpair' erfolgt oder aktualisiert")
    except mysql.connector.Error as err:
        print(f"Error handling beacon pairing data in MySQL: {err}")


# Funktion, die zwei zulässige Beacons miteinander paart und die Paarung im Memcache und der DB einträgt
# Notation des Caches: beaconpairing_cache_hub_id_mp_typ: {beacon_id , timestamp}
def beaconpairing(hub_id, mp_typ_id, beacon_id, timestamp):
    try:
        beacon_mapping_mp_typen = memcache.get(f"mp_typ_mapping_{mp_typ_id}")
        # Darf dieses Beacon mit anderen Typen gepaart werden?
        if beacon_mapping_mp_typen is not None:
            array_length = len(beacon_mapping_mp_typen)
            i = 0
            # Für jeden Typen nach Partnern suchen, die auf ein Paaren warten
            while i < array_length:
                beaconpairing_temp_key = f"beaconpairing_cache_{hub_id}_{beacon_mapping_mp_typen[i]}"
                moeglicher_partner = memcache.get(beaconpairing_temp_key)
                # Wenn Partner gefunden und Anfrage noch nicht zu alt
                if moeglicher_partner is not None:
                    if timestamp - moeglicher_partner[1] <= pairing_timegap:
                        # Eintragen des Paares in Memcached und Datenbank, Löschen der temporären Paarungsanfrage
                        update_beaconpairs(beacon_id, moeglicher_partner[0])
                        beaconpairing_DB_insert(beacon_id, moeglicher_partner[0], hub_id, timestamp)
                        memcache.delete(beaconpairing_temp_key) 
                        return
                    else:
                        print("Fehler bei Löschen von Cachedaten älter " + str(pairing_timegap) + " Sekunden")
                        return
                i += 1
            # Wenn Funktion bis hierhin kein Return ausgelöst hat, wurde kein Beacon zum Mapping gefunden.
            # Also folgt ein Eintrag in den Cache, der pairing_timegap Sekunden gespeichert wird
            # Der Timestamp ist lediglich eine Sicherheitsmaßnahme
            beaconpairing_temp_key = f"beaconpairing_cache_{hub_id}_{mp_typ_id}"
            beaconpairing_temp_value = (beacon_id, timestamp)
            memcache.set(beaconpairing_temp_key, beaconpairing_temp_value, pairing_timegap)
        else:
            if do_debug(2):
                print("Zu diesem MP gibt es keinen zugelassenen Partner")
            return
    
    except Exception as e:
        print(f"An error occurred: {e}")
    

# Funktion, die ein Beaconpaar als 'kritisch' markiert, wenn einer der Beacons den Hub/Raum gewechselt hat
def beacon_pairing_hubwechsel(beacon_id, new_hub_id, timestamp):
    try:
        beaconpair_key = f'beaconpairs_{beacon_id}'
        beaconpairs = memcache.get(beaconpair_key)
        # Gibt es Beacon-Paare für das zu prüfende Beacon? Wenn nein: Ende.
        if not beaconpairs:
            if do_debug(2): print(f"Keine Mappings für Beacon ID {beacon_id} gefunden.")
            return
        # Für jede ID, die in den destehenden Paarungen angegeben is, eine Prüfung durchführen
        for mapped_beacon_id in beaconpairs:
            # Vorbereitung der Key-Namen, Holen der Paarungen zu den IDs, Prüfen
            beaconpair_krit_key_1 = f'beacon_mapping_krit_{mapped_beacon_id}_{beacon_id}'
            beaconpair_krit_key_2 = f'beacon_mapping_krit_{beacon_id}_{mapped_beacon_id}'
            beaconpair_krit_data_1 = memcache.get(beaconpair_krit_key_1)
            beaconpair_krit_data_2 = memcache.get(beaconpair_krit_key_2)
            if beaconpair_krit_data_1:
                # Wenn auch das Paarungsziel auf neuem Hub bekannt, kritischen Eintrag löschen
                if beaconpair_krit_data_1[1] == new_hub_id:
                    memcache.delete(beaconpair_krit_key_1)
                    memcache.delete(beaconpair_krit_key_2)
                    if do_debug(2): 
                        print(f"Gefährdungseintrag {beaconpair_krit_key_1} und {beaconpair_krit_key_2} gelöscht.")
                else:
                    # Timestamp des bestehenden Eintrags aktualisieren
                    memcache.set(beaconpair_krit_key_2, [timestamp, new_hub_id])
                    if do_debug(2): print(f"Gefährdungseintrag {beaconpair_krit_key_2} aktualisiert.")
            elif beaconpair_krit_data_2:
                if beaconpair_krit_data_2[1] == new_hub_id:
                    memcache.delete(beaconpair_krit_key_1)
                    memcache.delete(beaconpair_krit_key_2)
                    if do_debug(2):
                        print(f"Gefährdungseintrag {beaconpair_krit_key_1} und {beaconpair_krit_key_2} gelöscht.")
                else:
                    memcache.set(beaconpair_krit_key_1, [timestamp, new_hub_id])
                    if do_debug(2): print(f"Gefährdungseintrag {beaconpair_krit_key_1} aktualisiert.")
            else:
                # Es gibt noch keinen Gefährdungseintrag. Erster Beacon einer Paarung, der einen anderen Raum meldet.
                memcache.set(beaconpair_krit_key_1, [timestamp, new_hub_id])
                if do_debug(2): print(f"Neuer Gefährdungseintrag {beaconpair_krit_key_1} erstellt.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Vorverarbeiten der MQTT Nachricht: Zerlegen in die einzelnen Werte, Weitergabe an die Verarbeitung, Laufzeitmessung
def process_message(message):
    global debug_count
    start_time = time.time() # Festhalten des Starts der Verarbeitung zur Laufzeitmessung
    # Startparameter und Positionen von Werten in den Memcached Datenstrukturen
    hub_id = 0  # 0 = kein Hub gefunden. -> keine weitere Verarbeitung.
    beacon_altdaten = None  # None = keine Altdaten/ kein Beacon Datensatz gefunden
    index_beacon_id = 0  # Ab hier: Positionen der einzelnen Werte innerhalb eines gespeicherten Arrays rsp. der Datenbankrückmeldung
    index_beacon_hub_id = 1  # Position der Hub ID im Array
    index_beacon_rssi = 2  # Position des RSSI Wertes im Array
    index_beacon_timestamp = 3  # Position des Timestamp im Array
    index_beacon_hub_timestamp_beginn = 4  # Position der Erstmeldung des beacons am Hub 
    index_beacon_batterie = 5  # Position des Batterie-Werts, nur von Relevanz bei Aktualisierung des beacons
    index_beacon_mp_typ = 6 # Position des dem zugeordneten MP eigenen MP Typs
    index_beacon_db_sync_timestamp = 7  # Position des Zeitpunktes, zu dem der letzte DB Abgleich erfolgte
    
    # Verarbeiten der Nachricht
    try:
        # Konvertierung des JSON-Strings in ein Dictionary
        message_dict = json.loads(message)
        # Direkter Zugriff auf den Zeitpunkt, zu dem die Nachricht im MQTT angekommen ist
        timestamp = int(message_dict["time"])
        # Verarbeitung des data-Strings: Auslesen und den Text in Einzelwerte aufteilen
        data_parts = message_dict["data"].split(", ")
        data_values = {part.split('=')[0]: part.split('=')[1] for part in data_parts}
        # Direkter Zugriff auf die Werte der MQTT Nachricht
        hub_MAC = data_values.get('MAC_ROOM', None)
        beacon_MAC = data_values.get('MAC_SENSOR', None)
        beacon_batterie = int(data_values.get('BATT', 0))
        beacon_taster = int(data_values.get('BUTTON', 0))
        beacon_rssi = int(data_values.get('RSSI', 0))

        # Verarbeitet wird nur bei gültigem Hub!
        # Hub-aktiv-Timestamp aktualisieren. Wenn es den Hub gibt, ist nach Aufruf die hub_id > 0. 
        if hub_MAC is not None:
            hub_id = hub_aktualisieren(hub_MAC, timestamp)
        # Wenn es den Hub gibt: Holen der letzten Meldungsdaten des beacons. Wenn es den beacon gibt, ist nach Aufruf die beacon_id > 0
        if hub_id > 0 and beacon_MAC is not None:
            beacon_altdaten = beacon_altdaten_holen(beacon_MAC, timestamp)
        else:
            if do_debug(4): print("Unberkannte Beacondaten: ", data_parts)
        # Wenn es den beacon und Hub in der Datenbank gibt, verarbeite die neuen Daten
        if beacon_altdaten is not None:
            if do_debug(2): print("beacon Altdaten: ", beacon_altdaten)
            # Wenn Timestamp der neuen Daten noch nicht im Cache, akualisiere, 
            # sonst ignorieren, da Nachrict des Beacons vor Reconnect schon prozessiert wurde 
            if timestamp != beacon_altdaten[index_beacon_timestamp]:
                # Wenn noch kein Hub zugewiesen, wurde ist noch kein Zeitstempel der Zuweisung in den Altdaten, daher aktuellen TS nutzen
                # Wenn schon ein Hub zugewiesen wurde, aber ein Datensatz mit anderm Hub und höherem RSSI kommt, ebenfalls aktuellen TS nutzen
                # Ansonsten Timestamp der Erstverbindung weiterführen, ebenso der letzten Datenbanksynchronisation
                if beacon_altdaten[index_beacon_hub_id] is None:
                    # Case 1: Beacon wurde noch nie in einem Hub gefunden. Daten eintragen. 
                    beacon_neudaten = [beacon_altdaten[index_beacon_id], hub_id, beacon_rssi, timestamp, timestamp, \
                                       beacon_batterie, beacon_altdaten[index_beacon_mp_typ], timestamp] 
                    beacon_erstspeicherung(beacon_MAC, beacon_neudaten)
                    if do_debug(2): print("Ersteintrag Beacon erfolgt.")
                    if do_debug(2): print("Beacon Neudaten sind: ", beacon_neudaten)
                else:
                    if beacon_altdaten[index_beacon_hub_id] == hub_id:
                        # Case 2: Hub gleich? Dann RSSI, Batterie und Zeit des beacons aktualisieren.
                        # Timestamp der Erstverbindung halten, ebenso der letzten DB Synchronisation
                        beacon_neudaten = [beacon_altdaten[index_beacon_id], hub_id, beacon_rssi, timestamp, \
                                           beacon_altdaten[index_beacon_hub_timestamp_beginn], beacon_batterie, \
                                           beacon_altdaten[index_beacon_mp_typ], \
                                           beacon_altdaten[index_beacon_db_sync_timestamp]]
                        beacon_aktualisieren(beacon_MAC, beacon_neudaten)
                        if do_debug(2): print("Beacon aktualisiert")
                        if do_debug(2): print("Beacon Neudaten sind: ", beacon_neudaten)
                        # Taster gedrückt? Aufforderung zur Verknüpfung.
                        if beacon_taster == 1:
                            if do_debug(2): print("Taster wurde gedrückt, Beaconpairing eingeleitet")
                            beaconpairing(hub_id,beacon_altdaten[index_beacon_mp_typ] ,beacon_altdaten[index_beacon_id] , timestamp)
                    else:
                        # Case 3: Wenn neuer Hub mit besserem RSSI 
                        # oder neuer Hub, aber alter Hub-Eintrag älter als 5 Minuten, aktualisiere Hub.
                        if (beacon_altdaten[index_beacon_rssi] < beacon_rssi) or \
                            (beacon_altdaten[index_beacon_timestamp] + room_timegap < timestamp):
                            beacon_neudaten = [beacon_altdaten[index_beacon_id], hub_id, beacon_rssi, \
                                               timestamp, timestamp, beacon_batterie, \
                                               beacon_altdaten[index_beacon_mp_typ], timestamp]
                            beacon_hubwechsel(beacon_MAC, beacon_neudaten)
                            # Überprüfung von Beaconpaaren auf vorliegenden Standortwechsel
                            beacon_pairing_hubwechsel(beacon_altdaten[index_beacon_id], hub_id, timestamp)
                            if do_debug(4): print("beacon hat den Hub gewechselt")
                            if do_debug(4): print("beacon Neudaten sind: ", beacon_neudaten)
        else: 
            if do_debug(4): print("Unberkannte Beacondaten: ", data_parts)
        if do_debug(2): print(timestamp, data_values)
        if do_debug(2): # Messen der Laufzeit einer Verarbeitung
            debug_count += 1
            print("Extraktion und Schicken der Nachricht ", "{0:05d}".format(debug_count), " hat %.5f" % (time.time() - start_time), " Sekunden gedauert")
    except Exception as e:
        print(f"Fehler bei der Verarbeitung der Nachricht: {e}") # ohne debug-filter, das darf nicht passieren.

# MQTT methoden zum Bedienen der connect, disconnect und message events der MQTT Verbindung
# on_mqtt_message überprüft die Datenbankverbindung und gibt die empfangene Nachricht zur Verarbeitung.
# on_mqtt_connect registriert den Client zum Horchen auf das definierte Topic
# on_mqtt_disconnect sorgt bei einem Wegfallen der MQTT Verbindung für einen erneuten Verbindungsaufbau, sowie MQTT wieder zur Verfügung steht
def on_mqtt_message(client, userdata, msg):
    global db_connection
    # Sicherstellen, dass die Datenbankverbindung noch steht, Neuaufbau falls nicht
    loop = 0
    while db_connection.is_connected() == False:
        try:
            db_connection = mysql.connector.connect(**db_config)
        except mysql.connector.Error as err:
            time.sleep(5)
        loop = loop + 1
        if loop > 50:
            exit(51)    # Beendigen des Programms nach 50 erfolglosen Verbindungsversuchen.
    # Verbindung zur Datenbank steht, die Verarbeitung der Nachricht kann starten
    message = msg.payload.decode()
    process_message(message)

def on_mqtt_connect(client, userdata, flags, rc):
    if rc == 0:
        if do_debug(2): print("Mit MQTT-Broker verbunden.")
        client.subscribe(mqtt_topic)
    else:
        if do_debug(2): print(f"Verbindung fehlgeschlagen mit Code {rc}")

def on_mqtt_disconnect(client, userdata, rc):
    if rc != 0:
        if do_debug(2): print("Unerwarteter Verbindungsverlust zum MQTT-Broker.")
        try:
            client.reconnect()
        except Exception as e:
            if do_debug(2): print(f"Fehler beim Versuch, die Verbindung wiederherzustellen: {e}")

# Hauptprogram - Initialisieren und in die Endlosschleife, auf Nachrichten warten
def main():
    # Prüfen, ob memcache und Datenbank verbunden sind
    try:
        memcache.set('some_key', 'some value')
    except Exception as e:
        sys.exit(50) # erfolgt, wenn obiger Befehl fehlschlägt / memchaed nicht verfügbar ist
    # memcache.flush_all() # nur zum Testen mit einem leeren memcached
    # Verbindung zur Datenbank herstellen. Die Connection wird in der globalen 'db_connection' gehalten.
    try:
        db_connection = mysql.connector.connect(**db_config)
    except mysql.connector.Error as err:
        exit(1) # Beenden des Programms mit dem Returnwert 1. Ohne Datenbank ist keine Verarbeitung möglich.
    # Aufbau der initialen Verbindung zum MQTT. Da dieser durchaus neu Starten kann, wird diese Verbindung je nach Nutzung wiederhergestellt.
    # memcache.flush_all()
    # Vorbefüllung des memcache mit den letzten Daten aus der Datenbank
    load_initial_data()
    # Zuweisung der Methoden zur MQTT Verarbeitung
    client.on_connect = on_mqtt_connect          # Bei Aufbau einer Verbindung
    client.on_message = on_mqtt_message          # Bei Eingang einer Nachricht auf dem konfigurirten Topic
    client.on_disconnect = on_mqtt_disconnect    # Bei (i.B. unerwarteten) Verlust der Verbindung

    # Starten der Verbindung zum MQTT - und damit Abarbeiten der Nachrichten
    while True:
        try:
            client.connect(mqtt_server, mqtt_port, 60)
            client.loop_start()
            # Einmal in der Schleife wird bei Wegfall der MQTT Verbindung die für diesen Fall konfiguriesten Methode zum Re-Connect aufgerufen 
            while True:
                time.sleep(1)
        except Exception as e:
            if do_debug(2): print(f"Fehler bei der Verbindung zum MQTT-Broker: {e}. Versuche, in 5 Sekunden erneut zu verbinden...")
            time.sleep(5)

main()
