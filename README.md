# MQTT
## A simple MQTT broker

This is a simple barebone MQTT broker. It is simply a proof of concept in python3. It does not require any library, it is a self contained application.

I tested it with Mosquitto, and it is able to handle multiple subscribers and publishers. It support just literal topic filtering.

`mosquitto_sub  -h localhost  -t /test/ -p 9999`

`mosquitto_pub -h localhost -t /test/ -p 9999 -l < test.txt `

It does **not** support:
- Topic wildcards
- QoS
- RETAIL
- Will
- username/password
