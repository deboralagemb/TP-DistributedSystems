import threading
import concurrent.futures
import socket
import time
import random
import pickle

duracao = 10
socket.setdefaulttimeout(3)


def port_in_use(port, obj):
    if port == "":
        return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((obj._host, int(port))) == 0
    

class Client:
    def __init__(self, name, host, port):
        self.name = name  # Único.
        self.broker_host = '127.0.0.1'
        self.broker_port = 8080
        self._host = host
        self._port = port
        #self._lock = threading.Lock()
        self.requested = False
        self.hold = False  # Recebendo informação.
        self.queue = None  # Primeiro da fila = próxima execução.
        self.terminate = False

        
    def listen(self, event):        
        
        #print(event.is_set(), self.terminate)
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            #print("Listening on [%s:%s]" % (self._host, self._port))
            s.bind((self._host, self._port))
            s.listen()
            
            while not event.is_set() and not self.terminate:  # Tempo da main thread / mensagem de término do broker.
                
                print('\nEsperando mensagem (%s)' % self.name)
                
                conn, addr = None, None
                try:
                    conn, addr = s.accept()  # (!) Bloqueia a execução!
                except socket.timeout:
                    #print('\nsocket.accept timed out on', self.name)
                    continue
                
                with conn:
                    data = conn.recv(4096)
                    #print('Recebida!')
                    #print('Connected with Broker %s %s, receiving message ' % (addr[0], addr[1]))
        
                    self.hold = True            # Aguarde enquanto a queue é totalmente recebida (Talvez desnecessária, já que existe self._lock).
        
                    msg = pickle.loads(data)    # Recebe o array (queue) do Broker / mensagem de término.
                    
                    if msg == 'terminate':
                        self.terminate = True
                        continue
                    elif isinstance(msg, list):
                        if self.queue == None:  # Subscribe.
                            self.queue = msg
                            print('Queue atualizada: subscribe')
                            
                        elif len(msg) == 1:
                            if msg[0] == '%pop%':
                                self.queue.pop(0)
                                print('Queue atualizada: release')
                            else:               # Atualização na queue (próximo acquire recebido).
                                self.queue.append(msg[0])
                                print('Queue atualizada: acquire')
                                                        
                    else:
                        print('ERRO 01: Mensagem inválida')
                
                    self.hold = False
                    
        print("Closing listen thread.")
        
    
    def request(self, event):
        #print("Entering request thread as %s" % self.name)
        
        while not event.is_set() and not self.terminate:
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                #print(not self.hold, self.queue)
                if not self.hold:
                    if not self.requested:  # Se já não mandou um 'acquire'.
                        aleatorio = random.uniform(0.5, 2)
                        time.sleep(aleatorio)
                        
                        try:
                            #print('tentando conexão ', end = '')
                            print(self.broker_host, self.broker_port)
                            s.connect((self.broker_host, self.broker_port))
                            #print('conectou')
                            
                            # Manda junto informação sobre a porta de escuta.
                            msg = pickle.dumps(self.name + ' -acquire -var-X %s %s' % (self._host, self._port))
                            s.sendall(msg)
                            print(self.name)
                            self.requested = True
                            
                        except ConnectionRefusedError:
                            print("Connection refused on acquire.")
                    
                    elif self.queue != None:  # Já deu subscribe.                    
                        if len(self.queue) > 0:
                            #print(self.name, self.queue)
                            proximo = self.queue[0]  # Ex.: ['Débora', '-acquire', '-var-X']
                            if proximo == self.name:
                                print('>>> %s: Estou utilizando o recurso...' % self.name)
                                time.sleep(random.uniform(0.2, 0.5))  # Faça algo com var-X
                                #print('Terminei!')
                                
                                try:
                                    s.connect((self.broker_host, self.broker_port))
                                    print('>>> Liberando a variável')
                                    msg = pickle.dumps(self.name + ' -release -var-X ' + self._host + ' ' +  str(self._port))
                                    #print('==========================')
                                    s.sendall(msg)
                                    #print('enviado')
                                    self.requested = False
                                    
                                except ConnectionRefusedError:
                                    print("Connection refused on release.")
                                
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("Closing request thread.")
            s.connect((self.broker_host, self.broker_port))
            s.sendall(pickle.dumps('%s exited' % self.name))  # Manda mensagem final.
            self.queue = None
        

    def start(self):
        
# =============================================================================
#         portInput = input("Connect with BROKER on port number: ")
#         self.broker_port = self.broker_port if portInput == "" else int(portInput)
#         
#         portInput = input("CLIENT will listen on port number: ")
#         while port_in_use(portInput, self):
#             portInput = input("Port already in use, provide a new port number: ")
#         print('')
#         
#         self._port = int(portInput)
# =============================================================================
        
        event = threading.Event()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(self.listen, event)  # Thread para escutar o broker.
            executor.submit(self.request, event)  # Thread para mandar mensagem para o broker.
            
            time.sleep(duracao)  # Tempo da aplicação.
            event.set()


with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    executor.submit( Client('Débora', '127.0.0.1', 8081).start )
    executor.submit( Client('Felipe', '127.0.0.1', 8082).start )
    executor.submit( Client('Gabriel', '127.0.0.1', 8083).start )



#logging.getLogger().setLevel(logging.DEBUG)  # Imprimir os debugs.






def port(port):
    if port == "":
        return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', int(port))) == 0
