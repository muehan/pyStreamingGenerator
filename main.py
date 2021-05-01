#!/usr/bin/env python3

import socket
import time
import datetime
from threading import *

serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
host = '127.0.0.1'
port = 9999

i = 0

serversocket.bind((host, port))

serversocket.listen(5)
print ('server started and listening')
while 1:
    clientsocket, address = serversocket.accept()
    print ("read file")
    
    first = True
    with open("/home/hduser/mw_trace50.csv") as file:
        for line in file:
            if first:
                first = False
            else:
                values = line.split(",")
                if values[6].strip != '':
                    milis = int(line[6])
                    milis = milis + 220898664000 # 7 years in miliseconds
                    values[6] = milis

                    newline = ','.join(str(e) for e in line)

                    print("send line: " + newline)

                    clientsocket.send(bytes(newline + "\n", "utf-8"))
                    time.sleep(8)

    clientsocket.close()