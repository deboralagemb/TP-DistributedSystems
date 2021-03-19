import _thread as thread
from socket import *
import time

class Broker:  # Commit teste.
    def __init__(self):
        self.host = '127.0.0.1'
        self.port = 8080
        self.sockobj = socket(AF_INET, SOCK_STREAM)

    def start(self):
        portInput = input("Enter the Broker port number: ")
        self.port = int(portInput)
        self.sockobj.bind((self.host, self.port))
        self.sockobj.listen(1)

        # algo não está funcionando na linha abaixo
        connection, address = self.sockobj.accept()
        print('Listening on port: ', self.port)

        # Inicia nova thread para lidar com o cliente
        thread.start_new_thread(self.dealWithClient, (connection, address))

        shouldShutdown = input('Shutdown the broker (Y|N)?: ')
        if shouldShutdown == 'Y' or shouldShutdown == 'y':
            print('Broker stoped...')
            self.sockobj.close()


    def printClient(self):
        return "Mensagem do cliente: "

    def dealWithClient(self, connection, answer):
        # Simula atividade no bloco
        data = connection.recv(1024)
        print (self.printClient(), data.decode())
        time.sleep(1)
        connection.send(answer.encode())
        connection.close

Broker = Broker()
Broker.start()
