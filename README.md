# ATS - Microservice for processing the beacon data

The microservice consists of 2 programmes.
"Main_Microservice.py" handles the processing of the MQTT messages and thus provides the majority of the functions.

"Beaconpair_validity_check.py" checks the beacon pairs marked as critical and resolves them if necessary.

In addition, "MQTT_spam.py" is provided to test the microservice and the database under high load.

