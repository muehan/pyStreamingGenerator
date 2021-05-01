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
    
    with open("/home/hduser/mw_trace50.csv") as file:
        for line in file:
            print("send line: " + line)
            clientsocket.send(bytes(line + "\n", "utf-8"))
            time.sleep(10)

    clientsocket.close()