# ATS - Microservice for processing the beacon data

This repository contains Python scripts that were developed as part of a bachelor's thesis about Asset Tracking in Hospitals written in German.
For this reason, many of the variable names and comments are written in German.

The microservice consists of 2 programmes.
"Main_Microservice.py" handles the processing of the MQTT messages and thus provides the majority of the functions.

"Beaconpair_validity_check.py" checks the beacon pairs marked as critical and resolves them if necessary.

Both Python scripts require an instance of Memcached to function correctly.

See "https://memcached.org/" or "https://github.com/memcached/memcached"

In addition, "MQTT_spam.py" is provided to test the microservice and the database under high load.

## All Repositories needed to build ATS:
https://github.com/linus-norden/mosquitto-UNIX-time
https://github.com/linus-norden/ATS-ESP32-BLEScan
https://github.com/linus-norden/ATS-ESP32-WiFiMesh
https://github.com/linus-norden/ATS-WebApp-Frontend
https://github.com/linus-norden/ATS-WebApp-Backend
https://github.com/linus-norden/ATS-Python-Microservice
https://github.com/linus-norden/ATS-SQL-DB
