import threading
import concurrent.futures
import socket
import time
import random
import pickle

duracao = 20
socket.setdefaulttimeout(3)

### .notify() não funciona.

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
        self._lock = threading.Lock()
        self._condition = threading.Condition()
        self.requested = False
        self.queue = None  # Primeiro da fila = próxima execução.
        self.terminate = False
        self.okr = True
    

    def listen(self, event):
        
        #print(event.is_set(), self.terminate)
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            #print("Listening on [%s:%s]" % (self._host, self._port))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
                    data = conn.recv(4096)      # Recebe resposta do broker.
        
                    msg = pickle.loads(data)    # Recebe o array (queue) do Broker / mensagem de término.
                    print('message that im recieving from broker: ', msg)
                    with self._lock:
                        if msg == 'okr':      # (Não usado.)
                            self.requested = False
                            self.okr = True
                            print('======= RECEBI UM OK! =========')
                            
                        elif isinstance(msg, list):                            
                            if self.queue == None:  # Subscribe.
                                self.queue = msg
                                print('\n[%s]: Queue atualizada: ação subscribe %s' % (self.name, self.queue))
                                
                            elif len(msg) == 1:
                                if msg[0] == '%pop%':
                                    self.queue.pop(0)
                                    print('\n[%s]: Queue atualizada: ação release %s' % (self.name, self.queue))
                                    
                                    with self._condition as cv:
                                        print('Notifying...')
                                        cv.notifyAll()  # Notifica que a queue foi atualizada (apenas quando há algum 'release').
                                        print('Notified request thread')
                                    
                                else:               # Atualização na queue (próximo acquire recebido).
                                    self.queue.append(msg[0])
                                    print('\n[%s]: Queue atualizada: ação acquire %s' % (self.name, self.queue))
                                                            
                        else:
                            print('ERRO 01: Mensagem inválida')

                    conn.close()
                    
        print("Closing listen thread.")
        
    
    def request(self, event):
        #print("Entering request thread as %s" % self.name)
        
        while not event.is_set() and not self.terminate:
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                #s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

                if not self.requested:  # Se já não mandou um 'acquire'.
                    aleatorio = random.uniform(0.5, 2)
                    time.sleep(aleatorio)
                    
                    try:
                        print(self.broker_host, self.broker_port)
                        s.connect((self.broker_host, self.broker_port))
                        
                        # Manda junto informação sobre a porta de escuta.
                        msg = pickle.dumps(self.name + ' -acquire -var-X %s %s' % (self._host, self._port))
                        s.sendall(msg)
                        
                        print(self.name)
                        with self._lock:
                            self.requested = True
                        
                    except ConnectionRefusedError:
                        print("Connection refused on acquire.")
                
                elif self.queue != None and self.okr:  # Já deu subscribe.                
                    if len(self.queue) > 0:
                        proximo = self.queue[0]  # Ex.: ['Débora', '-acquire', '-var-X']
                        if proximo == self.name:
                            print('\n>>> [%s]: Estou utilizando o recurso...' % self.name)
                            time.sleep(random.uniform(0.2, 0.5))  # Faça algo com var-X
                            #print('Terminei!')
                            
                            try:
                                s.connect((self.broker_host, self.broker_port))
                                msg = pickle.dumps(self.name + ' -release -var-X ' + self._host + ' ' +  str(self._port))
                                print('%s liberou o recurso' % self.name)
                                s.sendall(msg)
                                with self._lock:
                                    self.okr = False
                                #self.requested = False
                                
                            except ConnectionRefusedError:
                                print("Connection refused on release.")
                        else:  # Não é a minha vez.
                            with self._condition as cv:
                                print('[%s] Waiting...' % self.name)
                                cv.wait()
                                print('[%s] Resuming...' % self.name)
                                
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            #s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            print("[%s] Closing request thread." % self.name)
            s.connect((self.broker_host, self.broker_port))
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
    executor.submit( Client('Hisoka', '127.0.0.1', 8085).start )
    executor.submit( Client('Boa_Hancock', '127.0.0.1', 8086).start )




# Util.

def port(port):
    if port == "":
        return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', int(port))) == 0




# =============================================================================
# import threading
# import time
# import logging
# 
# logging.basicConfig(level=logging.DEBUG,
#                     format='(%(threadName)-9s) %(message)s',)
# 
# def consumer(cv):
#     logging.debug('Consumer thread started ...')
#     with cv:
#         logging.debug('Consumer waiting ...')
#         cv.wait()
#         logging.debug('Consumer consumed the resource')
#         
#     with cv:
#         cv.wait()
#         logging.debug('Consumer consumed the resource')
#         
# 
# def producer(cv):
#     logging.debug('Producer thread started ...')
#     with cv:
#         logging.debug('Making resource available')
#         logging.debug('Notifying to all consumers')
#         cv.notifyAll()
#         
#     time.sleep(2)
#     with cv:
#         cv.notifyAll()
# 
# if __name__ == '__main__':
#     condition = threading.Condition()
#     cs1 = threading.Thread(name='consumer1', target=consumer, args=(condition,))
#     cs2 = threading.Thread(name='consumer2', target=consumer, args=(condition,))
#     pd = threading.Thread(name='producer', target=producer, args=(condition,))
# 
#     cs1.start()
#     time.sleep(2)
#     cs2.start()
#     time.sleep(2)
#     pd.start()
# =============================================================================
