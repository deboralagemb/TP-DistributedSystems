# BROKER PRINCIPAL

import selectors
import socket
import types
import threading
import pickle
import traceback
import sys
from copy import deepcopy

selector_timeout = 3

"""
Esta aplicação comporta 2 (dois) brokers.
Broker BACKUP pode receber mensagens de clientes e repassa essas mensagens para o broker principal.
Broker BACKUP NÃO manda mensagens para os clientes.
Broker BACKUP se comporta como cliente, com adicional de estar recebendo a lista de clientes atualizada do broker PRINCIPAL.
Assim que o broker PRINCIPAL cai, os clientes avisam o backup e ele se torna o principal.
O broker que virou pricipal inicialmente manda a queue inteira para todos os clientes atualizarem o contexto.
"""

class Broker:

    def __init__(self, host, port, backup_host, backup_port):
        self.name = 'Main'
        self.host = host
        self.port = port  # 1-65535
        self.clients = {'Backup': {'host': backup_host, 'port': backup_port}}
        self.queue = []
        self.queue_var_X = []
        self.queue_var_Y = []
        self.count = 0
        self._lock = threading.Lock()
        self.sibling_broker = {'host': backup_host, 'port': backup_port}  # Broker backup
        self._main = True  # True: Principal
        self.sibling_is_dead = False
        self.msg_to_backup = ['clients']
        

    def send_message_to_clients(self, sub, acq, queue, isVarX, toAll=False):
        with self._lock:  # Lock queue.
            for client_name in self.clients:  # Manda a queue para todos os clientes.
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

                    retorno = b''
                    if client_name == sub or toAll:  # Subscribing.
                        # QUEUE AQUI: ['Midoriya', 'Hisoka', 'Boa_Hancock'] - preciso saber qual queue é de qual
                        retorno = ['-var-X', self.queue_var_X, '-var-Y', self.queue_var_Y]
                        retorno = pickle.dumps(retorno)  # Manda o array todo.
                        print('%s SUBSCRIBED!' % client_name)
                    else:
                        if acq:
                            if isVarX:
                                retorno = ['-var-X', ['%app%', self.queue_var_X[-1]], '-var-Y', None]
                            else:
                                retorno = ['-var-X', None, '-var-Y', ['%app%', self.queue_var_Y[-1]]]
                            retorno = pickle.dumps(retorno)
                        else:
                            if isVarX:
                                retorno = ['-var-X', ['%pop%'], '-var-Y', None]
                            else:
                                retorno = ['-var-X', None, '-var-Y', ['%pop%']]
                            retorno = pickle.dumps(retorno)
                    try:
                        s.connect((self.clients[client_name]['host'], self.clients[client_name]['port']))
                        s.sendall(retorno)
                    except ConnectionRefusedError:
                        print("Connection REFUSED on:", client_name, end=' ')
                        print(pickle.loads(retorno))
                        

    def send_client_list_to_backup(self):
        self.msg_to_backup = ['clients']
        self.msg_to_backup.append(self.clients)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.sibling_broker['host'], self.sibling_broker['port']))  # Broker backup.
                s.sendall(pickle.dumps(self.msg_to_backup))
        except ConnectionRefusedError:
                print("BACKUP is dead ☠")
                self.sibling_is_dead = True
                self.clients.pop('Backup')
        

    def update_queue(self, msg, queue):  # Se comporta como cliente. Sempre será uma lista.
        if len(msg) > 0:
            if msg[0] == '%pop%':
                queue.pop(0)
                print('\n[%s]: Queue atualizada: ação release %s' % (self.name, queue))
            elif msg[0] == '%app%':  # Atualização na queue (próximo acquire recebido).
                queue.append(msg[1])
                print('\n[%s]: Queue atualizada: ação acquire %s' % (self.name, queue))
            else:
                queue = msg
                print('Queue atualizada %s' % queue)
        else:
            queue = []
            print('Queue atualizada %s' % queue)


    def resolveMsg(self, msg):
        msg = pickle.loads(msg)
        if msg is None:
            return

        def now_I_am_main_broker():
            self._main = True
            self.sibling_is_dead = True
            print('\x1b[0;33;40m' + 'I am now the main broker 👍' + '\x1b[0m')
            self.send_message_to_clients('', False, self.queue_var_X, True, True)
            self.send_message_to_clients('', False, self.queue_var_Y, False, True)

        # ========== Tratando broker backup [INÍCIO] ========== #

        if not self._main:  # É backup.
            # print('I am a backup.')
            if msg == 'SOS':
                print('I am now the main broker 👍')
                now_I_am_main_broker()

            elif isinstance(msg,
                            list):  # Mensagem do broker principal. Os clientes só mandam strings. Broker só manda lista.
                if msg[0] == 'clients':
                    self.clients = msg[1]
                    print('\nAtualizei minha lista de clientes.')
                else:
                    print('\natualizando minha queue %s' % msg)
                    if "-var-X" in msg[2]:
                        self.update_queue(msg, self.queue_var_X)
                    elif "-var-Y" in msg[2]:
                        self.update_queue(msg, self.queue_var_Y)
                    else:
                        print('Variable %s does not exist 🤷' % msg[2])

            else:  # Encaminha a mensagem para o broker principal.
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    try:
                        print('[%s] Forwarding client message ...' % (msg.split()[0]))
                        s.connect((self.sibling_broker['host'], self.sibling_broker['port']))
                        s.sendall(pickle.dumps(msg))
                    except ConnectionRefusedError:
                        print("☠️☠️☠️☠️☠️☠️☠️ Main broker is DEAD! ☠️☠️☠️☠️☠️☠️☠️")
                        now_I_am_main_broker()
            return
        elif msg == 'SOS':  # Todas as outras mensagens de aviso serão descartadas.
            return

        # ========== Tratando broker backup [FIM] ========== #

        with self._lock:
            self.count += 1

        if isinstance(msg,
                      list):  # Mensagem do principal recebida após o backup receber mensagem para se tornar principal.
            return

        msg = msg.split()  # Ex.: ['Débora', '-acquire', '-var-X', '127.0.0.1', '8080']
        _id = msg[0]  # Nome do cliente.

        if msg[1] == 'exited':
            self.clients.pop(_id)  # Retira o cliente do conjunto de clientes.
            print('\n----------------\n%s saiu\n----------------' % _id)
            try:
                self.queue_var_X.remove(_id)
                self.queue_var_Y.remove(_id)
            except ValueError:
                pass
            return
        
        tmp = deepcopy(msg)
        tmp.insert(2, '\x1b[0;31;40m') if msg[2] == '-var-X' else tmp.insert(2, '\x1b[0;32;40m')
        tmp.insert(4, '\x1b[0m')
        print('%3s. %s' % (self.count, " ".join(tmp[:-2])), end='  ')  # Esta mensagem pode estar fora de sincronia.

        if '-var-X' in msg[2]:
            sub = _id if (_id not in self.clients and _id not in self.queue_var_X) else ''
        else:
            sub = _id if (_id not in self.clients and _id not in self.queue_var_Y) else ''

        if _id not in self.clients:  # Atualiza a lista de clientes.
            self.clients[_id] = {'host': msg[-2],
                                 'port': int(msg[-1])}  # 'id': [host, port], inclusive do broker backup.
            if not self.sibling_is_dead:
                self.send_client_list_to_backup()

        action = msg[1]
        if "-var-X" in msg[2]:
            self.try_acquire(self.queue_var_X, action, _id, sub, True)
        elif "-var-Y" in msg[2]:
            self.try_acquire(self.queue_var_Y, action, _id, sub, False)
        else:
            print('Variable %s does not exist 🤷' % msg[2])


    def try_acquire(self, queue, action, _id, sub, isVarX):
        def respond_client(__id, __msg):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:  # Release recebido.
                try:
                    s.connect((self.clients[__id]['host'], self.clients[__id]['port']))
                    var = ('-var-X' if isVarX else '-var-Y')
                    sendmsg = [var, __msg]
                    s.sendall(pickle.dumps(sendmsg))
                except ConnectionRefusedError:
                    print('ERRO: Response not sent [%s: %s] 🤧' % (__id, __msg))
                    pass

        if action == '-acquire':
            if _id in queue:
                print('[WARNING]❗ Acquire duplo')
            else:
                queue.append(_id)  # Põe o nome do cliente no fim da lista.
                print(queue)
                self.send_message_to_clients(sub, True, queue, isVarX, False)

        elif action == '-release':
            if len(queue) > 0:
                if queue[0] == _id:  # -> Quem ta dando -release é quem está com o recurso?
                    queue.pop(0)
                    print(queue)
                    self.send_message_to_clients(sub, False, queue, isVarX,
                                              False)  # (!) Antes de mandar o 'okr'. Como não há 'ok acquire', o cliente pode receber um 'okr' antes de receber um 'pop' e atualizar a sua queue, ocasioanndo erros de releases duplo.
                    respond_client(_id, 'okr')
                else:
                    print('\x1b[0;30;41m' + '>>> [ERRO] Release inválido. Requerente: %s | Próximo na fila: %s' % (_id, queue[0]) + '\x1b[0m')
            else:
                print('>>> [ERRO] Tentativa de release com queue vazia!')


    def accept_wrapper(self, sock):
        conn, addr = sock.accept()  # Está pronto para receber informação.
        # print('accepted connection from', addr)
        conn.setblocking(False)
        data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')

        # Guarda os dados que queremos incluídos junto com o socket.
        # Queremos saber quando o cliente está pronto para reading ou writing.
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.sel.register(conn, events, data=data)


    """mask contém os eventos que estão prontos.
    key contém o objeto socket.
    """
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
                # data.outb = b''
                # print('closing connection to', data.addr)
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
            """
            Bloqueia até que tenha sockets prontos para I/O.
            Retorna lista de tuplas (key, events) para cada socket.
            Se key.data == None, então espera um socket do client.
            """

            try:
                # print('Escutando...')
                events = self.sel.select(timeout=selector_timeout)  # timeout em segundos [Float].
                for key, mask in events:
                    if key.data is None:
                        self.accept_wrapper(key.fileobj)
                    else:
                        self.service_connection(key, mask)

            except OSError:
                pass

            except KeyboardInterrupt:
                lsock.close()  # Libera a porta.
                break


input_host_ = sys.argv[1]
input_port_ = sys.argv[2]
backup_host_ = sys.argv[3]
backup_port_ = sys.argv[4]


if __name__ == "__main__":
    try:
        broker = Broker(input_host_, int(input_port_), backup_host_, int(backup_port_))
        print('Sou o broker PRINCIPAL!\n') if broker._main else print('Sou o broker BACKUP!\n')
        broker.start()
    except Exception:
        traceback.print_exc()
        # Caso a porta não esteja liberada por um erro do programa:
        from psutil import process_iter
        from signal import SIGTERM  # or SIGKILL

        for proc in process_iter():
            for conns in proc.connections(kind='inet'):
                if conns.laddr.port == 8080:  # qualquer porta
                    proc.send_signal(SIGTERM)  # or SIGKILL
