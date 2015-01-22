#!/usr/bin/python3
# vim: et sw=4 ts=4 ai

import socket
import threading
import argparse
import select
import collections
import logging


def lsb(x):
    return x&0xff
def msb(x):
    return (x>>8)&0xff

class Payload(object):
    def __init__(self, data=b''):
        self.data = data
    def __len__(self):
        return len(self.data)
    def get_int(self):
        logging.debug('get_int: %s', self.data)
        ans = self.data[0]*256 + self.data[1]
        self.data = self.data[2:]
        logging.debug('ans: %d', ans)
        return ans
    def get_str(self):
        logging.debug('get_str: %s', self.data)
        len = self.get_int()
        ans = self.data[:len]
        self.data = self.data[len:]
        logging.debug('ans: %s', ans)
        return ans
    def get_byte(self):
        logging.debug('get_byte: %s', self.data)
        ans = self.data[0]
        self.data = self.data[1:]
        logging.debug('ans: %d', ans)
        return ans
    def get_bytes(self, n):
        ans = self.data[:n]
        self.data = self.data[n:]
        return ans
    def add_int(self, val):
        self.data += bytes(((val>>8)&0xff, val&0xff))
    def add_str(self, val):
        self.add_int(len(val))
        self.data += val
    def add_byte(self, val):
        self.data += bytes([val])
    def add_bytes(self, val):
        self.data += bytes(val)

class MQTTTopic(object):

    def __init__(self, topic):
        self.topic = topic

    def __eq__(self, other):
        return self.topic == other.topic

class MQTTPacket(object):
    type = 'DUMMY'
    def __init__(self, data=None, header=None):
        self.dup = 0
        self.qos = 0
        self.ret = 0

        if data is not None:
            if isinstance(data, dict):
                self.__dict__.update(data)
            else:
                self._id = (header>>4)&0xf
                self.dup = (header>>3)&0x1
                self.qos = (header>>1)&0x3
                self.ret = header&0x1
                self._raw=data
                self._decode(Payload(data))

    def __str__(self):
        return self.type + " " + str(self.__dict__)
 
    def _decode(self, data):
        pass

    def encode_length(self, val):
        ans = [val & 0x7f]
        val = val >> 7
        while val > 0:
            x = 0x80 | (val & 0x7f)
            val = val >> 7
            ans.append(x)
        ans.reverse()
        return ans

    def make_header(self):
        return [((self.id & 0x0f) << 4) + ((self.dup & 1)<<3) + ((self.qos & 3)<<1) + (self.ret&1)]

    def as_bytes(self): 
        payload = self.payload()
        return bytes(self.make_header() + self.encode_length(len(payload))) + payload

    def payload(self):
        return b''

class MQTTConnect(MQTTPacket):
    type = 'CONNECT'
    id = 1
    def _decode(self, data):
        self.proto = data.get_str()
        self.version = data.get_byte()
        f = data.get_byte()
        self.f_user = (f>>7)
        self.f_pass = (f>>6) & 1
        self.f_reta = (f>>5) & 1
        self.f_qos = (f>>4) & 3
        self.f_will = (f>>2) & 1
        self.f_clean = (f>>1) & 1
        self.keepalive = data.get_int()
        self.client = data.get_str()
        if self.f_will:
            self.will_tpc = data.get_str()
            self.will_msg = data.get_str()
        if self.f_user:
            self.user = data.get_str()
        if self.f_user:
            self.passwd = data.get_str()

class MQTTConnack(MQTTPacket):
    type = 'CONNACK'
    id = 2
    def make_header(self):
        return (0x00, self.ans)
    def make_payload(self):
        return (0x00, self.ans)

    def as_bytes(self):
        ans = 0x00
        return bytes((0x20, 0x02, 0x00, ans))

class MQTTPingreq(MQTTPacket):
    id = 12
    type = 'PINGREQ'

class MQTTPingresp(MQTTPacket):
    id = 13
    type = 'PINGRESP'

class MQTTDisconnect(MQTTPacket):
    type = 'DISCONNECT'

class MQTTSuback(MQTTPacket):
    type = 'SUBACK'
    id = 9
    def payload(self):
        p = Payload()
        p.add_int(self.msgid)
        for k,v in self.topics.items():
            p.add_byte(v)
        return p.data

class MQTTSubscribe(MQTTPacket):
    type = 'SUBSCRIBE'
    id = 8
    def _decode(self, data):
        self.msgid = data.get_int()
        self.topics = {}
        while len(data)>0:
            t = data.get_str()
            q = data.get_byte()
            self.topics[t] = q & 0x3

class MQTTUnsubscribe(MQTTPacket):
    type = 'UNSUBSCRIBE'
    id = 10
    def _decode(self, data):
        pass

class MQTTUnsuback(MQTTPacket):
    type = 'UNSUBACK'
    id = 11

class MQTTPublish(MQTTPacket):
    type = 'PUBLISH'
    id = 3
    def _decode(self, data):
        self.topic = data.get_str()
        payload_len = len(self._raw) - (2+len(self.topic))
        if self.qos > 0:
            self.msgid = data.get_int()
            payload_len -= 2
        self.payload = data.get_bytes(payload_len)

    def payload(self):
        p = Payload()
        logging.info('%s %s', str(self.topic), str(self.message))
        p.add_str(self.topic)
        if self.msgid is not None:
            p.add_int(self.msgid)
        p.add_bytes(self.message)
        return p.data

class MQTTDummy(MQTTPacket):
    type = 'DUMMY'
    id = 0

class MQTTPacketFactory(object):

    type_map = {
        'CONNECT': MQTTConnect,
        'CONNACK': MQTTConnack,
        'PINGREQ': MQTTPingreq,
        'PINGRESP': MQTTPingresp,
        'PUBLISH': MQTTPublish,
        'DISCONNECT': MQTTDisconnect,
        'SUBSCRIBE': MQTTSubscribe,
        'SUBACK': MQTTSuback,
    }
    typeid_map = {
        1: MQTTConnect,
        2: MQTTConnack,
        3: MQTTPublish,
        8: MQTTSubscribe,
        9: MQTTSuback,
        12: MQTTPingreq,
        13: MQTTPingresp,
        14: MQTTDisconnect,
    }

    def __init__(self, request=None, data=None):
        if request is not None:
            self.packet = self.decode(request)
        elif data is not None:   
            self.packet = self.create(data)

    def decode(self, req):
        header = b''
        while len(header) < 1:
            header = req.recv(1)
        header = header[0] 
        length = self._get_len(req)
        msg_type = header>>4
        data = req.recv(length)
        return self.typeid_map.get(msg_type, MQTTDummy)(data=data, header=header)

    def create(self, data):
        return self.typeid_map.get(data['type'], MQTTDummy)(data)

    def _get_len(self, req):
        l = 0
        while True:
            x = req.recv(1)[0]
            l = l*128 + (x & 0x3F)
            if x<128:
                break
        return l

        
class MQTTHandler(object):

    def __init__(self, conn, messages, subscriptions):
        self._mqtt_state = 'NONE'
        self.conn = conn
        self.qos = 0
        self.messages = messages
        self.subscriptions = subscriptions

    def close(self):
        self.conn.close()

    def send(self, packet):
        if packet is not None:
            logging.info('> %s %s', str(packet), str(packet.as_bytes()))
            self.conn.sendall(packet.as_bytes())
        
    def handle(self):
        p = MQTTPacketFactory(request=self.conn).packet
        q = None
        done = False
        logging.info('< %s %s', str(self._mqtt_state), str(p))
        if self._mqtt_state == 'NONE' and p.type == 'CONNECT':
            self.qos = p.qos
            q = MQTTConnack()
            self._mqtt_state = 'CONNECTED'
        elif self._mqtt_state == 'CONNECTED':
            if p.type == 'DISCONNECT':
                self._mqtt_state = 'NONE'
                done = True
            elif p.type == 'PINGREQ':
                q = MQTTPingresp()
            elif p.type == 'SUBSCRIBE':
                for topic, topic_qos in p.topics.items():
                    self.subscriptions.append((topic, self, topic_qos))
                q = MQTTSuback({'msgid':p.msgid, 'topics': p.topics})
            elif p.type == 'PUBLISH':
                logging.info('Appending message %s', str(p.topic))
                self.messages.append((p.topic, p.payload))
                if self.qos == 0:
                    pass
                elif self.qos == 1:
                    pass
                    #q = MQTTPuback()
                elif self.qos == 2:
                    pass
                    #q = MQTTPubrec()
        else:
            logging.warn('Ignoring request. Client not connected and it is not a CONNECT request')

        self.send(q)

        return done
                

class MQTTServer(object):
    def __init__(self, HOST, PORT, handler, poll_interval=0.5):
        self.host = HOST
        self.port = PORT
        self.handler = handler
        self.socket = None
        self.clients = []
        self.handlers = {}
        self.poll_interval = 0.5
        self.initialize()
        self.messages = []
        self.subscriptions = []

    def initialize(self):
        logging.info('Listening on port {}'.format(self.port))
        self.socket = socket.socket()
        self.socket.bind((self.host, self.port))
        self.socket.listen(2)

    def match_topics(self, sub, mes):
        return sub == mes

    def publish_data(self):
        logging.debug('%s', str(self.subscriptions))
        logging.debug('%s', str(self.messages))
        for mes_topic, mes_message in self.messages:
            for sub_topic, sub_conn, sub_qos in self.subscriptions:
                if self.match_topics(sub_topic, mes_topic):
                    q = MQTTPublish(data={'qos': sub_qos, 'msgid': None, 'topic': mes_topic, 'message': mes_message})
                    sub_conn.send(q)

        # I cannot simply assign a new empty array,
        # i.e. self.messages = []
        # I need the same object since it is
        # shared it with the handler class
        # I have to empty this object
        while len(self.messages) > 0:
            self.messages.pop()

    def serve_forever(self):
        while True:
            rs, ws, es = select.select([self.socket] + self.clients, [], [], self.poll_interval)
            for r in rs:
                conn = None
                handler = None
                if r == self.socket:
                    conn, _client = r.accept()
                    logging.info('Accepted connection from %s', str( _client))
                    handler = self.handler(conn, self.messages, self.subscriptions)
                    self.clients.append(conn)
                    self.handlers[conn] = handler
                else:
                    handler = self.handlers[r]

                done = handler.handle()
                logging.info('%s', str(done))

                if done:
                    handler.close()
                    self.clients = [c for c in self.clients if c != handler.conn]
                    del self.handlers[handler.conn]

            if len(self.messages) > 0:
                self.publish_data()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simple MQTT broker')
    parser.add_argument('--port', type=int, action='store', default=9999, help='Port to use for MQTT, default 9999')
    parser.add_argument('--verbose', action='store_const', default=False, const=True, help='Verbose output')
    parser.add_argument('--debug', action='store_const', default=False, const=True, help='Debug output')
    args = parser.parse_args()
    log_level = logging.WARN
    if args.debug:
        log_level = logging.DEBUG
    elif args.verbose:
        log_level = logging.INFO
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=log_level)    
    HOST, PORT = '0.0.0.0', args.port
    server = MQTTServer(HOST, PORT, MQTTHandler)
    server.serve_forever()
	
