import threading
import concurrent.futures
import socket
import time
import random
import pickle

duracao = 15
socket.setdefaulttimeout(3)

def port_in_use(port, obj):
    if port == "":
        return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((obj._host, int(port))) == 0
    

class Client:
    def __init__(self, name, host, port):
        self.name = name  # Único.
        self.broker = [{'ip': '127.0.0.1', 'port': 8080}, {'ip': '127.0.0.1', 'port': 8079}]
        self._host = host
        self._port = port
        self._lock = threading.Lock()
        self.requested = False
        self.queue = None  # Primeiro da fila = próxima execução.
        self.terminate = False
        self.okr = True

        
    def listen(self, event):
        
        def deal_with_queue(_queue):
            print('\n[%s]: Queue atualizada: ' % self.name, end='')
            if self.queue == None:  # Subscribe.
                print('ação subscribe %s' % self.queue)
            else:
                print('sincronizando contexto com o broker backup %s' % self.queue)
                #notify() também entraria aqui
            
            self.queue = _queue
            
            self.okr = True  # Caso seja sincronização com o backup e o cliente tenha mandado mensagem de release para o principal não lida.
            if self.name in self.queue:
                self.requested = True        
        
        #print(event.is_set(), self.terminate)
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            #print("Listening on [%s:%s]" % (self._host, self._port))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self._host, self._port))
            s.listen()
            
            while not event.is_set() and not self.terminate:  # Tempo da main thread / mensagem de término do broker.
                
                #print('\nEsperando mensagem (%s)' % self.name)                
                conn, addr = None, None
                try:
                    conn, addr = s.accept()  # (!) Bloqueia a execução!
                except socket.timeout:
                    #print('\nsocket.accept timed out on', self.name)
                    continue
                
                with conn:
                    data = conn.recv(4096)      # Recebe resposta do broker.        
                    msg = pickle.loads(data)    # Recebe o array (queue) do Broker / mensagem de término.
                    
                    with self._lock:
                        if msg == 'okr':      
                            self.requested = False
                            self.okr = True
                            #print('[%s] ======= RECEBI UM OK! =========' % self.name)
                            
                        elif isinstance(msg, list):
                            if len(msg) > 0:
                                if msg[0] == '%pop%':
                                    self.queue.pop(0)
                                    print('\n[%s]: Queue atualizada: ação release %s' % (self.name, self.queue))
                                elif msg[0] == '%app%':               # Atualização na queue (próximo acquire recebido).
                                    self.queue.append(msg[1])
                                    print('\n[%s]: Queue atualizada: ação acquire %s' % (self.name, self.queue))
                                else:
                                    deal_with_queue(msg)
                            else:
                                deal_with_queue(msg)
                                
                        else:
                            print('ERRO 01: Mensagem inválida')
                            raise NotImplementedError

                    conn.close()
                    
        print("Closing listen thread.")
        
        
    def connect_to_broker(self, host, msg, flag = False):
        if flag:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host['ip'], host['port']))
                s.sendall(pickle.dumps('SOS'))  # "Não consegui me conectar com o broker principal"
            
            self.broker.pop(0)  # Retira o broker desconectado da lista.
            
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host['ip'], host['port']))

            # Manda junto informação sobre a porta de escuta.
            msg = pickle.dumps(msg)
            s.sendall(msg)
    
    
    def try_connection(self, msg):
        #print('Ready to start sending')
        try:
            self.connect_to_broker(self.broker[0], msg)                        
        except ConnectionRefusedError:
            print("Connection refused. Notifying backup broker ...")
            if(len(self.broker) > 1):
                try:
                    self.connect_to_broker(self.broker[1], msg, True)                        
                except ConnectionRefusedError:
                    print("Connection REFUSED 😡")
            else:
                print('There is no broker left. 😢')
    
    
    def request(self, event):
        #print("Entering request thread as %s" % self.name)
        
        while not event.is_set() and not self.terminate:

            if not self.requested:  # Se já não mandou um 'acquire'.
                aleatorio = random.uniform(0.5, 2)
                time.sleep(aleatorio)
                
                #print('Sending message ...')
                self.try_connection(self.name + ' -acquire -var-X %s %s' % (self._host, self._port))
                #print('I\'ve just sent an acquire.')
                with self._lock:
                    self.requested = True
            
            elif self.queue != None and self.okr:  # Já deu subscribe.                    
                if len(self.queue) > 0:
                    proximo = self.queue[0]  # Ex.: ['Débora', '-acquire', '-var-X']
                    if proximo == self.name:
                        #print('\n>>> [%s]: Estou utilizando o recurso...' % self.name)
                        time.sleep(random.uniform(0.2, 0.5))  # Faça algo com var-X
                        #print('Terminei!')
                        
                        #print('\nOPA ======== %s - %s - %s =========' % (self.name, self.queue, proximo))
                        self.try_connection(self.name + ' -release -var-X ' + self._host + ' ' +  str(self._port))
                        print('%s liberou o recurso' % self.name)
                        with self._lock:
                            self.okr = False
                            #print('\n---> %s atualizei okr: %s' % (self.name, self.okr))

                                
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            #s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            print("[%s] Closing request thread." % self.name)
            s.connect((self.broker[0]['ip'], self.broker[0]['port']))
            s.sendall(pickle.dumps('%s exited' % self.name))  # Manda mensagem final.
            self.queue = None
        

    def start(self):        
        event = threading.Event()
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            executor.submit(self.listen, event)  # Thread para escutar o broker.
            executor.submit(self.request, event)  # Thread para mandar mensagem para o broker.
            
            time.sleep(duracao)  # Tempo da aplicação.
            event.set()


with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    executor.submit( Client('Midoriya', '127.0.0.1', 8084).start )
    executor.submit( Client('Boa_Hancock', '127.0.0.1', 8085).start )
    executor.submit( Client('Edward_Elric', '127.0.0.1', 8086).start )




# Util.

def port(port):
    if port == "":
        return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', int(port))) == 0
