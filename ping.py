#!/usr/bin/python3
# vim: ai et sw=4 ts=4

import argparse
import time
import socket

parser = argparse.ArgumentParser(description='Simple MQTT ping load test')
parser.add_argument('--port', type=int, action='store', default=1883, help='Port to use for MQTT, default 1883')
parser.add_argument('--req', type=int, action='store', default=1000, help='Number of requests, default 1000')
args = parser.parse_args()

HOST = 'localhost'    # The remote host
PORT = args.port              # The same port as used by the server
NREQ = args.req

#PORT = 9999              # The same port as used by the server
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((HOST, PORT))
s.sendall(b'\x10\x16\x00\x06MQIsdp\x03\x00\x00\x10\x00\x08asdfasdf')
data = s.recv(1024)
print(data)
t0=time.time()
for i in range(NREQ):
    s.sendall(b'\xc0\00')
    data=b''
    while True:
        data += s.recv(1024) 
        #data += b'1'
        if len(data)==2:
            break
t1 = time.time()
s.sendall(b'\xe0\x00')
s.close()
print(int(1000*(t1-t0)), 'msec', int(NREQ/(t1-t0)), 'qps')
