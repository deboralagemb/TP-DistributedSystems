# BROKER

import selectors
import socket
import types
import threading
import pickle
import traceback

selector_timeout = 3

### Esta aplicação comporta 2 (dois) brokers.
### Broker BACKUP pode receber mensagens de clientes e repassa essas mensagens para o broker principal.
### Broker BACKUP NÃO manda mensagens para os clientes.
### Broker BACKUP se comporta como cliente, com adicional de estar recebendo a lista de clientes atualizada do broker PRINCIPAL.
### Assim que o broker PRINCIPAL cai, os clientes avisam o backup e ele se torna o principal.
### O broker que virou pricipal inicialmente manda a queue inteira para todos os clientes atualizarem o contexto.
### @todo: colocar mais de uma variável, ao invés de apenas -var-X (alteração tanto no broker quanto no cliente).

class Broker:
    
    def __init__(self):
        self.name = 'Backup'
        self.host = '127.0.0.1'
        self.port = 8079  # 1-65535
        self.clients = {}  # Caso coloque este como principal, adicionar o backup como cliente.
        self.queue = []
        self.count = 0
        self._lock = threading.Lock()
        self.sibling_broker = {'host': '127.0.0.1', 'port': 8080}
        self._main = False  # False: Backup
        self.sibling_is_dead = False
        self.msg_to_backup = ['clients']
        
        
    def sendMessageToClients(self, sub, acq, toAll = False):        
        with self._lock:  # Lock queue.            
            for client_name in self.clients:  # Manda mensagem para todos os clientes.                
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    
                    retorno = b''
                    if client_name == sub or toAll:  # Subscribing.
                        retorno = pickle.dumps(self.queue)  # Manda o array todo.
                        print('%s SUBSCRIBED!' % client_name)
                    else:
                        if acq:
                            retorno = pickle.dumps(['%app%', self.queue[-1]])  # O último a mandar acquire.
                        else:
                            retorno = pickle.dumps(['%pop%'])
                            
                    #print('enviando para %s' % client_name)
                    
                    try:
                        s.connect((self.clients[client_name]['host'], self.clients[client_name]['port']))
                        s.sendall(retorno)
                    except ConnectionRefusedError:
                        print("Connection REFUSED on:", client_name, end=' ')
                        print(pickle.loads(retorno))
                    
    
    def sendClientListToBackup(self):
        self.msg_to_backup = ['clients']
        self.msg_to_backup.append(self.clients)
        print('\n------------> avisando o backup %s' % self.msg_to_backup)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.sibling_broker['host'], self.sibling_broker['port']))  # Broker backup.
            s.sendall(pickle.dumps(self.msg_to_backup))
            
    
    def update_queue(self, msg):  # Se comporta como cliente. Sempre será uma lista.
        if len(msg) > 0:
            if msg[0] == '%pop%':
                self.queue.pop(0)
                print('\n[%s]: Queue atualizada: ação release %s' % (self.name, self.queue))
            elif msg[0] == '%app%':  # Atualização na queue (próximo acquire recebido).
                self.queue.append(msg[1])
                print('\n[%s]: Queue atualizada: ação acquire %s' % (self.name, self.queue))
            else:
                self.queue = msg
                print('Queue atualizada %s' % self.queue)
        else:
            self.queue = []
            print('Queue atualizada %s' % self.queue)
    
        
    def resolveMsg(self, msg):
        
        msg = pickle.loads(msg)
        if msg == None:
            return
        
        def nowIAmMainBroker():
            self._main = True
            self.sibling_is_dead = True
            self.sendMessageToClients('', False, True)
            
        # ========== Tratando broker backup [INÍCIO] ========== #
        
        if not self._main:  # É backup.
            #print('I am a backup.')
            if msg == 'SOS':
                print('I am now the main broker 👍')
                nowIAmMainBroker()
                            
            elif isinstance(msg, list):  # Mensagem do broker principal. Os clientes só mandam strings. Broker só manda lista.
                if msg[0] == 'clients':
                    self.clients = msg[1]
                    #print(msg[1])
                    print('\nAtualizei minha lista de clientes.')
                else:
                    print('\natualizando minha queue %s' % msg)
                    self.update_queue(msg)
                
            else:  # Encaminha a mensagem para o broker principal.
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    try:
                        print('[%s] Forwarding client message ...' % (msg.split()[0]))
                        s.connect((self.sibling_broker['host'], self.sibling_broker['port']))
                        s.sendall(pickle.dumps(msg))
                    except ConnectionRefusedError:
                        print("Connection REFUSED on main BROKER. 😡😡😡 %s" % msg)  # Poderia virar principal aqui, sem precisar de mensagens dos clientes.
                        nowIAmMainBroker()
            return
        elif msg == 'SOS':  # Todas as outras mensagens de aviso serão descartadas.
            return
        
        # ========== Tratando broker backup [FIM] ========== #
        
        with self._lock:
            self.count += 1
        
        if isinstance(msg, list):  # Mensagem do principal recebida após o backup receber mensagem para se tornar principal.
            return
        
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
        #if msg[-2] != self.sibling_broker['host'] or msg[-1] != self.sibling_broker['port']:  # Não é o broker backup mandando mensagem.
        
        if _id not in self.clients:  # Atualiza a lista de clientes.
            self.clients[_id] = {'host': msg[-2], 'port': int(msg[-1])}  # 'id': [host, port], inclusive do broker backup.
            if not self.sibling_is_dead:  ### @todo para funcionar com apensa um broker, adicione 'and False'.
                self.sendClientListToBackup()
        
        action = msg[1]
        
        if action == '-acquire':
            if _id in self.queue:
                print('\n-[WARNING] Acquire duplo')  # Agora irrelevante
            else:            
                self.queue.append(_id)  # Põe o nome do cliente no fim da lista.
                print(self.queue)                
                self.sendMessageToClients(sub, True)
                
        elif action == '-release':
            if len(self.queue) > 0:
                if self.queue[0] == _id:  # -> Quem ta dando -release é quem está com o recurso?
                    self.queue.pop(0)
                    print(self.queue)
                    
                    self.sendMessageToClients(sub, False)  # (!) Antes de mandar o 'okr'. Como não há 'ok acquire', o cliente pode receber um 'okr' antes de receber um 'pop' e atualizar a sua queue, ocasioanndo erros de releases duplo.
                    
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:  # Release recebido.
                        try:
                            s.connect((self.clients[_id]['host'], self.clients[_id]['port']))
                            s.sendall(pickle.dumps('okr'))
                        except ConnectionRefusedError:
                            #print('%s NÃO recebeu o OK!' % _id)
                            pass
                        
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
        print('listening on', (self.host, self.port))
        
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
    try:
        broker = Broker()
        print('Sou o broker PRINCIPAL!\n') if broker._main else print('Sou o broker BACKUP!\n')
        broker.start()
    except Exception:
        traceback.print_exc()
        # Caso a porta não esteja liberada por um erro do programa:
        from psutil import process_iter  ### @todo pip install psutil, caso não tenha instalado.
        from signal import SIGTERM  # or SIGKILL
        for proc in process_iter():
            for conns in proc.connections(kind='inet'):
                if conns.laddr.port == 8079:   # qualquer porta
                    proc.send_signal(SIGTERM)  # or SIGKILL
