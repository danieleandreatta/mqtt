# MQTT
## A simple MQTT broker

This is a simple barebone MQTT broker. It is simply a proof of concept in python3. It does not require any library, it is a self contained application.

I tested it with Mosquitto, and it is able to handle multiple subscribers and publishers. It support just literal topic filtering.

In 3 terminal do

`./mqtt.py`

`mosquitto_sub -h localhost -t /test/ -p 9999`

`mosquitto_pub -h localhost -t /test/ -p 9999 -m 'some random message'`

It does **not** support:
- Topic wildcards
- QoS
- RETAIN
- Will
- username/password
