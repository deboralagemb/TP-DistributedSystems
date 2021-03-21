import _thread as thread
import threading
import logging
import concurrent.futures
import numpy as np
from socket import *
import time
import random
import pickle

duracao = 10

# Não testado! Nem sei o que eu tô fazendo.


def port_in_use(port, obj):
    if port == "":
        return True
    with socket(AF_INET, SOCK_STREAM) as s:
        return s.connect_ex((obj._host, int(port))) == 0
    

class Client:
    def __init__(self):
        self.broker_host = '127.0.0.1'
        self.broker_port = 8080
        self._host = '127.0.0.1'
        self._port = 8081
        self._lock = threading.Lock()
        #self.acquired = False
        self.requested = False
        self.hold = False  # Recebendo informação.
        self.name = None
        self.name_s = 'Débora'
        self.queue = None  # Primeiro da fila = próxima execução.
        self.terminate = False

        
    def listen(self, s, event):
        logging.info("Listening on [%s:%s]...", self._host, self._port)
        s.bind(self._host, self._port)        
        s.listen()  # (Fora ou dentro do loop?)
        
        while not event.is_set() and not self.terminate:  # Tempo da main thread / mensagem de término do broker.
            conn, addr = s.accept()  # (!) Bloqueia a execução!
            
            with conn, self._lock:  # self._lock impede acesso à variável self.hold pela thread request().
                logging.info('Connected with Broker %s, receiving message...', addr)                
                self.hold = True  # Aguarde enquanto a queue é totalmente recebida (Talvez desnecessária, já que existe self._lock).
                data = b''
                while True:
                    tmp = conn.recv(4096)
                    data += tmp
                    if not tmp:  # Nenhuma mensagem recebida.
                        msg = pickle.loads(data)  # Recebe o array (queue) do Broker / mensagem de término.
                        if msg == 'terminate':
                            self.terminate = True
                            break
                        
                        self.queue = msg
                        self.hold = False
                        logging.info('Queue received.')
                        logging.debug(self.queue)
                        break
        logging.info("Closing listen thread.")
        
    
    def request(self, s, event):
        logging.info("Entering request thread as %s...", self.name_s)
        
        while not event.is_set() and not self.terminate:
            if not self.hold:
                proximo = self.queue[0].split()  # Ex.: ['Débora', '-acquire', '-var-X']
                if proximo[0] == self.name_s and proximo[1] == '-acquire':
                    time.sleep(random.uniform(0.1, 0.3))  # Faça algo com var-X
                    s.connect((self.broker_host, self.broker_port))
                    s.sendall(self.name + b' -release -var-X')
                        
        logging.info("Closing request thread.")
        

    def start(self):   
        self.name = bytes(self.name_s, 'utf-8')  # string para bytes.
        
        with socket(AF_INET, SOCK_STREAM) as s:
            portInput = input("Connect with BROKER on port number: ")
            self.broker_port = self.broker_port if portInput == "" else int(portInput)
            
            portInput = input("Connect CLIENT on port number: ")
            while port_in_use(portInput, self):
                portInput = input("Port already in use, provide a new port number: ")
            
            self.broker_port = int(portInput)
            
            event = threading.Event()
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                executor.submit(self.listen(), s, event)
                executor.submit(self.request(), s, event)
                
                time.sleep(duracao)  # Tempo da aplicação.
                event.set()


client = Client()
client.start()

#logging.getLogger().setLevel(logging.DEBUG)  # Imprimir os debugs.






