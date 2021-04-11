# BROKER

import selectors
import socket
import types
import threading
import pickle

host = '127.0.0.1'
port = 8080
selector_timeout = 3

### todo: BUGS INSERIDOS - aparece erro de release.

class Broker:
    
    def __init__(self):
        self.host = '127.0.0.1'
        self.port = 8080  # 1-65535
        self.clients = {}
        self.queue = []
        self.count = 0
        self._lock = threading.Lock()
        self.sibling_broker = {'ip': '127.0.0.1', 'port': '8079'}
        self._main = True  # False: Backup
        
        
    def sendMessageToClients(self, sub, acq):        
        with self._lock:  # Lock queue.            
            for client_name in self.clients:  # Manda a queue para todos os clientes.                
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    
                    retorno = b''
                    if client_name == sub:  # Subscribing.
                        retorno = pickle.dumps(self.queue)  # Manda o array todo.
                        print('%s SUBSCRIBED!' % client_name)
                    else:
                        if acq:
                            retorno = pickle.dumps([self.queue[-1]])  # O último a mandar acquire.
                        else:
                            retorno = pickle.dumps(['%pop%'])
                            
                    #print('enviando para %s' % client_name)
                    
                    try:
                        s.connect((self.clients[client_name]['host'], self.clients[client_name]['port']))
                        s.sendall(retorno)
                    except ConnectionRefusedError:
                        print("Connection REFUSED on:", client_name, end=' ')
                        print(pickle.loads(retorno))
                    
        
    def update_queue(self, msg):
        if self.queue == None:  # Primeira mensagem.
            self.queue = msg
            print('Queue atualizada')
            
        elif len(msg) == 1:
            if msg[0] == '%pop%':
                self.queue.pop(0)
                print('\n[%s]: Queue atualizada: ação release %s' % (self.name, self.queue))
            else:  # Atualização na queue (próximo acquire recebido).
                self.queue.append(msg[0])
                print('\n[%s]: Queue atualizada: ação acquire %s' % (self.name, self.queue))
    
        
    def resolveMsg(self, msg):
        
        msg = pickle.loads(msg)
        
        if not self._main:  # É backup.
            if msg == 'SOS':
                print('I am now the main broker 👍')
                self._main = True    
                
            elif isinstance(msg, list):  # Mensagem do broker principal.
                self.update_queue(msg)
                
            else:  # Encaminha a mensagem para o broker principal.
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    try:
                        print('Forwarding client message ...')
                        s.connect((self.sibling_broker['ip'], self.sibling_broker['port']))
                        s.sendall(pickle.dumps(msg))
                    except ConnectionRefusedError:
                        print("Connection REFUSED on main BROKER. 😡😡😡")
            return
        
        with self._lock:
            self.count += 1
            
        msg = msg.split() # Ex.: ['Débora', '-acquire', '-var-X', '127.0.0.1', '8080']
        _id = msg[0]  # Nome do cliente.
        
        if msg[1] == 'exited':
            self.clients.pop(_id)  # Retira o cliente do conjunto de clientes.
            print('\n----------------\n%s saiu\n----------------' % _id)
            try:
                self.queue.remove(_id)
            except ValueError:
                pass            
            return
        
        print('%3s. %s' % (self.count, " ".join(msg[:-2])), end='  ')  # Esta mensagem pode estar fora de sincronia.
        
        sub = _id if (_id not in self.clients and _id not in self.queue) else ''  # Se é o primeiro contato do cliente, mande todo o array (subscribe).        
        #if msg[-2] != self.sibling_broker['ip'] or msg[-1] != self.sibling_broker['port']:  # Não é o broker backup mandando mensagem.
        self.clients[_id] = {'host': msg[-2], 'port': int(msg[-1])}  # 'id': [host, port], inclusive do broker backup.
        action = msg[1]
        
        if action == '-acquire':
            if _id in self.queue:
                print('>>> [ERRO] Acquire duplo')
            else:            
                self.queue.append(_id)  # Põe o nome do cliente no fim da lista.
                print(self.queue)                
                self.sendMessageToClients(sub, True)
                
        elif action == '-release':
            if len(self.queue) > 0:
                if self.queue[0] == _id:  # -> Quem ta dando -release é quem está com o recurso?
                    self.queue.pop(0)
                    print(self.queue)
                    
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:  # Release recebido.
                        try:
                            s.connect((self.clients[_id]['host'], self.clients[_id]['port']))
                            s.sendall(pickle.dumps('okr'))
                        except ConnectionRefusedError:
                            #print('%s NÃO recebeu o OK!' % _id)
                            pass
                        
                    self.sendMessageToClients(sub, False)
                else:
                    print('>>> [ERRO] Release inválido. Requerente: %s | Próximo na fila: %s' % (_id, self.queue[0]))
            else:
                print('>>> [ERRO] Tentativa de release com queue vazia!')
                
        
    def accept_wrapper(self, sock):
        conn, addr = sock.accept()  # Está pronto para receber informação.
        #print('accepted connection from', addr)
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
        
        # Guarda os dados que queremos incluídos junto com o socket.
        # Queremos saber quando o cliente está pronto para reading ou writing.
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.sel.register(conn, events, data=data)
        
    
    # mask contém os eventos que estão prontos.
    # key contém o objeto socket.
    def service_connection(self, key, mask):
        sock = key.fileobj
        data = key.data
        
        if mask & selectors.EVENT_READ:        
            recv_data = sock.recv(4096)  # Should be ready to read
            
            if recv_data:
                # Append qualquer mensagem recebida na variável data.outb.
                data.outb += recv_data
            else:
                self.resolveMsg(data.outb)
                #data.outb = b''
                
                #print('closing connection to', data.addr)
                
                # O socket não é mais monitorado pelo select().
                self.sel.unregister(sock)
                sock.close()
                
        if mask & selectors.EVENT_WRITE:
            if data.outb:
                pass  # Tratado usando função específica para comunicação com todos os clientes.
        
        
    def start(self):
        

        self.sel = selectors.DefaultSelector()
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.bind((self.host, self.port))
        lsock.listen()
        print('listening on', (host, port))
        
        # Não bloqueará a execução.
        lsock.setblocking(False)
        
        # 'data' é qualquer mensagem que você queira atrelar ao socket.
        self.sel.register(lsock, selectors.EVENT_READ, data=None)
        
        while True:
            
            # Bloqueia até que tenha sockets prontos para I/O.
            # Retorna lista de tuplas (key, events) para cada socket.
            # Se key.data == None, então espera um socket do client.
            
            try:
                #print('Escutando...')
                events = self.sel.select(timeout=selector_timeout)  # timeout em segundos [Float].
                for key, mask in events:
                    if key.data is None:
                        self.accept_wrapper(key.fileobj)
                    else:
                        self.service_connection(key, mask)
                        
            except OSError:
                pass
            
            except KeyboardInterrupt:
                #lsock.shutdown(1)
                lsock.close()  # Libera a porta.
                break

if __name__ == "__main__":
    broker = Broker()
    broker.start()


# =============================================================================
# # Caso a porta não esteja liberada por um erro do programa:
# from psutil import process_iter
# from signal import SIGTERM # or SIGKILL
# for proc in process_iter():
#     for conns in proc.connections(kind='inet'):
#         if conns.laddr.port == 8080:  # qualquer porta
#             proc.send_signal(SIGTERM) # or SIGKILL
# =============================================================================
