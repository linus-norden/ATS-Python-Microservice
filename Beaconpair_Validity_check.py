import mysql.connector
import time
from pymemcache.client import base
import json
import sys
import os
from dotenv import load_dotenv, find_dotenv

# Laden der ungebungsabhängigen Konfigurationsparameter
load_dotenv(find_dotenv('Default_Config.env'))
# Konfiguration der MySQL-Datenbankverbindung und ded Memcached
db_config = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_DATABASE')
}
memcache_server = os.getenv('MEMCACHE_SERVER')
memcache_port = os.getenv('MEMCACHE_PORT')
db_connection = None    # Globale Variable zum Halten des DB connectors
timegap = os.getenv('TIMEGAP')

# Debug Level und Filter Funktion, Debug Meldungen aus dem Text können bitweise auf Leveln definiert werden
debug_level = 2
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

# Aufbau der globalen Verbindung zum memcached
memcache = base.Client((memcache_server, memcache_port), serializer=json_serializer, deserializer=json_deserializer)

# Funktion zum Auflösen eines Beaconpaars in der Datenbank und im Memcache
def delete_beaconpair(beacon_id_1, beacon_id_2, DeleteFromArray):
    try:
        cursor = db_connection.cursor()
        # Löschen der beiden möglichen Paarungen 1 + 2 oder 2 + 1 in der Datenbank
        delete_query = '''
            DELETE FROM beaconpair
            WHERE (beaconpair_beacon_id_1 = %(id1)s AND beaconpair_beacon_id_2 = %(id2)s)
               OR (beaconpair_beacon_id_1 = %(id2)s AND beaconpair_beacon_id_2 = %(id1)s)
        '''
        cursor.execute(delete_query, {'id1': beacon_id_1, 'id2': beacon_id_2})
        db_connection.commit()
        if do_debug(2): print(f"Beacon pair {beacon_id_1} and {beacon_id_2} dissolved in DB.")

        # Erstellen der Namen der Datensätze im Memcache und holen der Datensätze
        beaconpairs_key_1 = f'beaconpairs_{beacon_id_1}'
        beaconpairs_key_2 = f'beaconpairs_{beacon_id_2}'
        beaconpairs_temp_1 = memcache.get(beaconpairs_key_1)
        beaconpairs_temp_2 = memcache.get(beaconpairs_key_2)
        
        # Für jeden der Datensätze: Entfernen der jeweils anderen Beacon ID.
        # Zurückschreiben, wenn Paarungen verbleiben, sonst löschen 
        if beaconpairs_temp_1:
            beaconpairs_temp_1 = [id for id in beaconpairs_temp_1 if id != beacon_id_2]
            if beaconpairs_temp_1 == []:
                memcache.delete(beaconpairs_key_1)
            else:
                memcache.set(beaconpairs_key_1, beaconpairs_temp_1)
        if beaconpairs_temp_2:
            beaconpairs_temp_2 = [id for id in beaconpairs_temp_2 if id != beacon_id_1]
            if beaconpairs_temp_2 == []:
                memcache.delete(beaconpairs_key_2)
            else:
                memcache.set(beaconpairs_key_2, beaconpairs_temp_2)
        if do_debug(2): print(f"Beacon pair {beacon_id_1} and {beacon_id_2} dissolved in Memcache.")
        
        beaconpairs_temp_all = memcache.get('beaconpairs')
        beaconpairs_temp_all.pop(DeleteFromArray)
        memcache.set('beaconpairs', beaconpairs_temp_all)

    except mysql.connector.Error as err:
        print(f"Error dissolving beacon pair in MySQL: {err}")


# Funktion zum Überprüfen und Auflösen kritischer Mappings
def beaconpaar_validity_check():
    # Holen aller aktuell eingetragenen Beacon-Pairs
    beaconpairs = memcache.get('beaconpairs')
    if do_debug(4): print(beaconpairs)
    # Wenn die Liste nicht leer ist, wird über einen Positionsindex durch die Liste iteriert.
    # Der Positionsindex wird ebenfalls zum Löschen eines Eintrags der laufenden Liste verwendet.
    if beaconpairs != None:
        array_length = len(beaconpairs) # Bestimmen, wann der Aublauf beendet werden muss
        pairposition = 0 # Die Position des aktuellen Pairs in der Liste. Kein! Hochzählen bei Löschen des aktuellen.
        while pairposition < array_length:
            beacon_id_1 = beaconpairs[pairposition].split(",")[0]
            beacon_id_2 = beaconpairs[pairposition].split(",")[1]
            # Erzeugung der Zugriffsnamen für die kritischen Mapping-Einträge und holen dieser Werte
            krit_key_1 = f'beacon_mapping_krit_{beacon_id_1}_{beacon_id_2}'
            krit_key_2 = f'beacon_mapping_krit_{beacon_id_2}_{beacon_id_1}'
            krit_data_1 = memcache.get(krit_key_1)
            krit_data_2 = memcache.get(krit_key_2)
            # Nutzung der lokalen Zeit zur Prüfung, ob die Zeitgrenze zum Löschen erreicht ist
            current_time = int(time.time())
            # Wenn auch nur eines der möglichen zwei Einträge zu alt ist, dann Löschen des Paares
            if krit_data_1 and current_time - krit_data_1[0] > 30 or krit_data_2 and current_time - krit_data_2[0] > 30:
                delete_beaconpair(beacon_id_1, beacon_id_2, pairposition)
                memcache.delete(krit_key_1)
                if do_debug(2):
                    print(f"Critical mapping {krit_key_1} dissolved.")
                memcache.delete(krit_key_2)
                if do_debug(2):
                    print(f"Critical mapping {krit_key_2} dissolved.")
            else:
                pairposition +=1    # nächste Position, da aktuelle nicht gelöscht wurde

# Hauptprogramm
def main():
    # Prüfen, ob memcache und Datenbank verbunden sind
    try:
        memcache.set('some_key', 'some value')
    except Exception as e:
        sys.exit(50) # erfolgt, wenn obiger Befehl fehlschlägt / memchaed nicht verfügbar ist
    # memcache.flush_all() # nur zum Testen mit einem leeren memcached
    # Verbindung zur Datenbank herstellen
    try:
        db_connection = mysql.connector.connect(**db_config)
    except mysql.connector.Error as err:
        exit(1) # Beenden des Programms mit dem Returnwert 1. Ohne Datenbank ist keine Verarbeitung möglich.
    # Dauerhaftes Prüfen aller Beacon Paare. Die DB connection wird jede Minute genutzt, bleibt daher geöffnet.
    while True:
        beaconpaar_validity_check()
        time.sleep(60)  # 1 Minute warten
main()
