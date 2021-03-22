import threading
import concurrent.futures
import socket
import time
import random
import pickle

duracao = 10


def port_in_use(port, obj):
    if port == "":
        return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((obj._host, int(port))) == 0
    

class Client:
    def __init__(self):
        self.broker_host = '127.0.0.1'
        self.broker_port = 8080
        self._host = '127.0.0.1'
        self._port = 8081
        #self._lock = threading.Lock()
        #self.acquired = False
        self.requested = False
        self.hold = False  # Recebendo informação.
        self.name = 'Débora'  # Único.
        self.queue = []  # Primeiro da fila = próxima execução.
        self.terminate = False

        
    def listen(self, s, event):
        print("Listening on [%s:%s]" % (self._host, self._port))
        s.bind((self._host, self._port))      
        s.listen()
        
        print(event.is_set(), self.terminate)
        while not event.is_set() and not self.terminate:  # Tempo da main thread / mensagem de término do broker.
            print('Esperando mensagem')
            conn, addr = s.accept()  # (!) Bloqueia a execução!
            print('Recebida!')
            
            with conn:
                print('Connected with Broker %s, receiving message' % addr)                
                self.hold = True  # Aguarde enquanto a queue é totalmente recebida (Talvez desnecessária, já que existe self._lock).
                data = conn.recv(4096)

                msg = pickle.loads(data)  # Recebe o array (queue) do Broker / mensagem de término.
                if msg == 'terminate':
                    self.terminate = True
                    continue
                
                self.queue = msg
                self.hold = False
                print('Queue received.', self.queue)
                
        print("Closing listen thread.")
        
    
    def request(self, s, event):
        print("Entering request thread as %s" % self.name)
        
        while not event.is_set() and not self.terminate:
            if not self.hold:
                if not self.requested:  # Se já não mandou um 'acquire'.
                    aleatorio = random.randint(0, 99)
                    if aleatorio < 10:
                        try:
                            print('tentando conexão ', end = '')
                            print(self.broker_host, self.broker_port)
                            s.connect((self.broker_host, self.broker_port))
                            print('conectou')
                            
                            # Manda junto informação sobre a porta de escuta.
                            msg = pickle.dumps(self.name + ' -acquire -var-X %s %s' % (self._host, self._port))
                            s.sendall(msg)
                            self.requested = True
                            
                        except ConnectionRefusedError:
                            print("Connection refused on acquire.")
                
                elif len(self.queue) > 0:  
                    proximo = self.queue[0].split()  # Ex.: ['Débora', '-acquire', '-var-X']
                    if proximo[0] == self.name and proximo[1] == '-acquire':
                        print('Estou utilizando o recurso...')
                        time.sleep(random.uniform(0.2, 0.5))  # Faça algo com var-X
                        print('Terminei!')
                        
                        try:
                            s.connect((self.broker_host, self.broker_port))
                            msg = pickle.dumps(self.name + ' -release -var-X %s %s' % (self._host, self._port))
                            s.sendall(msg)
                            self.requested = False
                            
                        except ConnectionRefusedError:
                            print("Connection refused on release.")
                        
        print("Closing request thread.")
        s.connect((self.broker_host, self.broker_port))
        s.sendall(pickle.dumps('%s exited' % self.name))  # Manda mensagem final.
        

    def start(self):
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_listen, \
                socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_request:
            portInput = input("Connect with BROKER on port number: ")
            self.broker_port = self.broker_port if portInput == "" else int(portInput)
            
            portInput = input("CLIENT will listen on port number: ")
            while port_in_use(portInput, self):
                portInput = input("Port already in use, provide a new port number: ")
            print('')
            
            self._port = int(portInput)
            
            event = threading.Event()
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                executor.submit(self.listen, s_listen, event)  # Thread para escutar o broker.
                executor.submit(self.request, s_request, event)  # Thread para mandar mensagem para o broker.
                
                time.sleep(duracao)  # Tempo da aplicação.
                event.set()


client = Client()
client.start()

#logging.getLogger().setLevel(logging.DEBUG)  # Imprimir os debugs.


def port(port):
    if port == "":
        return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', int(port))) == 0
