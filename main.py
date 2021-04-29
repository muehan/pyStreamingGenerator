#!/usr/bin/env python3

import socket
import time
from threading import *

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = '127.0.0.1'  # The server's hostname or IP address
port = 9999         # The port used by the server

i = 0

serversocket.bind((host, port))

serversocket.listen(5)
print ('server started and listening')
while 1:
    clientsocket, address = serversocket.accept()
    
    for j in range(50):
        print("send hello world \n")
        clientsocket.send(bytes("Hallo World", "utf-8"))
        i += 1
        time.sleep(2)
    clientsocket.close()