import _thread as thread
import socket
import time
import pickle

class Client:
    def __init__(self):
        self.host = '127.0.0.1'  # The server's hostname or IP address
        self.port = 8080         # The port used by the server

    def start(self):      
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            portInput = input("Connect with Broker on port number: ")
            self.port = self.port if portInput == "" else int(portInput)
            try:
                s.connect((self.host, self.port))
                print(self.host, self.port)
                tmp = pickle.dumps('Felipe -release -var-X 127.0.0.1 8081')
                #print(tmp)
                s.sendall(tmp)
                data = s.recv(4096)
                #print(data)
                tmp = pickle.loads(data)
                print('\nReceived', repr(tmp))
                
            except ConnectionRefusedError:
                print("Connection refused.")
        

client = Client()
client.start()
