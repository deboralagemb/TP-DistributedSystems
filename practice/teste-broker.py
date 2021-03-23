import _thread as thread
from socket import *
import concurrent.futures
import queue
import random
import time
import logging
import threading
import pickle

# Nem mexi aqui, só no client.
# todo, usar .select() para escutar os clientes e evitar conexões recusadas.



class Broker:
    def __init__(self):
        self.host = '127.0.0.1'
        self.port = 8080  # 1-65535

    def start(self):
        portInput = input("Enter the Broker port number: ")
        self.port = 8080 if portInput == "" else int(portInput)
        print("Default port number selected: " + str(self.port)) if portInput == "" else {}
        print("Listening...", end="\n\n")
        
        with socket(AF_INET, SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()            
            
            while True:
                try:
                    conn, addr = s.accept()
                    with conn:
                        print('Connected by ', addr)
                        data = b''
                        while True:
                            tmp = conn.recv(4096)
                            data += tmp
                            if not tmp:
                                break
                            print('Received', pickle.loads(data))
                            conn.send(data)
                            
                except (ConnectionResetError, ConnectionAbortedError):
                    pass
                except KeyboardInterrupt:
                    break
                    

broker = Broker()
broker.start()

