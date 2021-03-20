import _thread as thread
from socket import *
import time

class Client:
    def __init__(self):
        self.host = '127.0.0.1'  # The server's hostname or IP address
        self.port = 8080         # The port used by the server

    def start(self):      
        with socket(AF_INET, SOCK_STREAM) as s:
            portInput = input("Connect with Broker on port number: ")
            self.port = self.port if portInput == "" else int(portInput)
            
            s.connect((self.host, self.port))
            s.sendall(b'Hello, world')
            
            data = s.recv(1024)
        
        print('\nReceived', repr(data))

client = Client()
client.start()
