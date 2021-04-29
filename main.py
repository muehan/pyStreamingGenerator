#!/usr/bin/env python3

import socket
import time
from threading import *

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = '127.0.0.1'  # The server's hostname or IP address
port = 9999         # The port used by the server

i = 0

serversocket.bind((host, port))

class client(Thread):
    def __init__(self, socket, address):
        print('client established')
        Thread.__init__(self)
        self.sock = socket
        self.addr = address
        self.start()

    def run(self):
        while 1:
            print('Client sent:', self.sock.recv(1024).decode())
            self.sock.send(b'Oi you sent something to me')

serversocket.listen(5)
print ('server started and listening')
while 1:
    clientsocket, address = serversocket.accept()
    client(clientsocket, address)
    
    for j in range(50):
        print("send hello world")
        clientsocket.send(bytes("Hallo World", "utf-8"))
        i += 1
        time.sleep(2)
    clientsocket.close()