#!/usr/bin/env python3

import socket
import time
from threading import *

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
host = '127.0.0.1'
port = 9999

i = 0

serversocket.bind((host, port))

serversocket.listen(5)
print ('server started and listening')
while 1:
    clientsocket, address = serversocket.accept()
    print ("read file")
    
    with open("~/mw_trace50.csv") as f:
        for i, line in enumerate(f):             
            clientsocket.send(bytes("line", "utf-8"))

    clientsocket.close()