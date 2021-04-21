import threading
import concurrent.futures
import socket
import time
import random
import pickle
import sys

duracao = 40
socket.setdefaulttimeout(3)


def port_in_use(port, obj):
    if port == "":
        return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((obj.host, int(port))) == 0


class VariableContext(object):
    def __init__(self, client, var_name, queue, okr, requested):
        self.client = client
        self.var_name = var_name
        self.queue = queue
        self.okr = okr
        self.requested = requested
        

    def handle_msg_okr(self):
        self.requested = False
        self.okr = True
        

    def handle_update_queue(self, msg):
        if len(msg) > 0:
            if msg[0] == '%pop%':
                self.queue.pop(0)
                print('\n[%s]: Queue %s atualizada: ação release %s' % (self.client.name, self.var_name, self.queue))
            elif msg[0] == '%app%':  # Atualização na queue (próximo acquire recebido).
                if self.queue is None:
                    self.queue = msg[1]
                else:
                    self.queue.append(msg[1])
                print('\n[%s]: Queue %s atualizada: ação acquire %s' % (self.client.name, self.var_name, self.queue))
            else:
                self.deal_with_queue(msg)
        else:
            self.deal_with_queue(msg)
            

    def deal_with_queue(self, msg):
        print('\n[%s]: Queue atualizada: ' % self.client.name, end='')
        if self.queue is None:  # Subscribe.
            print('ação subscribe %s' % self.queue)
        else:
            print('sincronizando contexto com o broker backup')
            # notify() também entraria aqui

        # todo pegar a queue apenas na posição referente da msg
        self.queue = msg
        self.okr = True  # Caso seja sincronização com o backup e o cliente tenha mandado mensagem de release para o principal não lida.
        if self.client.name in self.queue:
            self.requested = True
    

    def handle_use_variable(self):
        if not self.requested:  # Se já não mandou um 'acquire'.
            aleatorio = random.uniform(0.5, 2)
            time.sleep(aleatorio)
            self.client.try_connection(self.client.name + ' -acquire %s %s %s' % (self.var_name,
                                                                                  self.client.host,
                                                                                  self.client.port), self)
        elif self.queue is not None and self.okr:  # Já deu subscribe.
            time.sleep(0.5)  # Pode ter casos em que o broker vá receber acquire corretamente mais levará mais que 0.5s para responder, o que irá gerar acquire duplo.
            if self.client.name not in self.queue:  # Tratando caso em que broker principal cai logo após receber acquire.
                print('\n(!) ---> [%s] My acquire request somehow was never received ... 🤔 trying again!' % self.client.name)
                self.requested = False
                return True

            if len(self.queue) > 0:
                proximo = self.queue[0]  # Ex.: ['Débora', '-acquire', '-var-X']
                if proximo == self.client.name:
                    time.sleep(random.uniform(0.2, 0.5))  # Faça algo com var-X
                    self.client.try_connection(
                        self.client.name + ' -release ' + self.var_name + ' ' + self.client.host + ' ' + str(self.client.port), self)
                    # print('%s liberou o recurso' % self.var_name)
                    with self.client.lock:
                        self.okr = False


"""
Cada variável tem sua queue, 
seu okr,
seu requested.

Primeiro passo: escutar requisições do broker
Segundo passo: identificar as variáveis do broker e criar Clients
"""

class Client:
    def __init__(self, name, client_host, client_port, broker_host, broker_port, backup_host, backup_port):
        self.name = name  # Único.
        self.broker = [{'host': broker_host, 'port': broker_port},
                       {'host': backup_host, 'port': backup_port}]  # Conectado ao BACKUP (:8079)
        self.host = client_host
        self.port = client_port
        self.lock = threading.Lock()
        self.variablesContext = []
        self.variablesNames = []
        

    def listen(self, event):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("\nListening on [%s:%s]" % (self.host, self.port))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.host, self.port))
            s.listen()

            while not event.is_set():  # Tempo da main thread / mensagem de término do broker.
                conn, addr = None, None
                try:
                    conn, addr = s.accept()  # (!) Bloqueia a execução!
                except socket.timeout:
                    continue

                with conn:
                    data = conn.recv(4096)  # Recebe resposta do broker.
                    msg = pickle.loads(data)  # Recebe o array (queue) do Broker / mensagem de término.
                    # todo thread para acquire/release ok?
                    # iterando de 2 em 2 pois a mensagem vem no seguinte formato
                    # ['-var-X', ['Débora'], '-var-Y', []]

                    for i in range(0, len(msg), 2):
                        if not msg[i] in self.variablesNames:
                            self.variablesNames.append(msg[i])
                            queue = None if not msg[i + 1] else msg[i + 1]  # pega a fila se tiver, None se não tiver
                            new_variable = VariableContext(self, msg[i], queue, True, False)
                            self.variablesContext.append(new_variable)
                            
                        for var in self.variablesContext:
                            with self.lock:
                                if var.var_name == msg[i]:
                                    if None is msg[i + 1]:
                                        break
                                    if 'okr' in msg[i + 1]:
                                        var.handle_msg_okr()
                                    elif isinstance(msg[i + 1], list):
                                        var.handle_update_queue(msg[i + 1])
                                    elif len(msg[i + 1]) > 0:
                                        print('ERRO 01: Mensagem inválida')
                                        raise NotImplementedError

        print("Closing listen thread.")
        

    def connect_to_broker(self, host, msg, var):
        self.send(host, msg)
        with self.lock:
            var.requested = True
            

    def try_connection(self, msg, var):
        try:
            self.connect_to_broker(self.broker[0], msg, var)
        except ConnectionRefusedError:
            print("Connection refused. Notifying backup broker ...")
            if len(self.broker) > 1:
                try:
                    self.connect_to_broker(self.broker[1], msg, var)
                except ConnectionRefusedError:
                    print("Connection REFUSED 😡")
            else:
                print('There is no broker left. 😢')


    def request(self, event):
        # print("Entering request thread as %s" % self.name)
        while not event.is_set():
            # todo thread para esse caso
            if not self.variablesContext:
                self.variablesNames.append('-var-X')
                testing = VariableContext(self, '-var-X', None, True, False)
                self.variablesContext.append(testing)

            for var in self.variablesContext:
                var.handle_use_variable()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("[%s] Closing request thread." % self.name)
            s.connect((self.broker[0]['host'], self.broker[0]['port']))
            s.sendall(pickle.dumps('%s exited' % self.name))  # Manda mensagem final.
            
            for var in self.variablesContext:
                var.queue = None
                

    def send(self, host, m):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host['host'], host['port']))
            msg = pickle.dumps(m)
            s.sendall(msg)
            

    def check_broker(self, event):
        while not event.is_set():
            time.sleep(1.5)
            try:
                self.send(self.broker[0], None)
            except ConnectionRefusedError:
                print('Notifying backup broker ...')
                self.send(self.broker[1], 'SOS')
                time.sleep(0.25)
                self.broker.pop(0)
                break
        

    def start(self):
        event = threading.Event()
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executorClient:
            executorClient.submit(self.listen, event)  # Thread para escutar o broker.
            executorClient.submit(self.request, event)  # Thread para mandar mensagem para o broker.
            executorClient.submit(self.check_broker, event)  # - Are you there?

            time.sleep(duracao)  # Tempo da aplicação.
            event.set()


client_append_name = sys.argv[1]
client_host_ = sys.argv[2]
broker_host_ = sys.argv[3]
broker_port_ = sys.argv[4]
backup_host_ = sys.argv[5]
backup_port_ = sys.argv[6]

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    executor.submit( Client(client_append_name+'_Midoriya', client_host_, 8084,
                            broker_host_, int(broker_port_), backup_host_, int(backup_port_)).start )
    executor.submit( Client(client_append_name+'_Boa_Hancock', client_host_, 8085,
                            broker_host_, int(broker_port_), backup_host_, int(backup_port_)).start )
    executor.submit( Client(client_append_name+'_Edward_Elric', client_host_, 8086,
                            broker_host_, int(broker_port_), backup_host_, int(backup_port_)).start )
