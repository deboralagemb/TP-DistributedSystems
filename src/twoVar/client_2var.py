import threading
import concurrent.futures
import socket
import time
import random
import pickle

duracao = 20
socket.setdefaulttimeout(3)


def port_in_use(port, obj):
    if port == "":
        return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((obj._host, int(port))) == 0

# cada variável tem sua queue
# seu okr
# seu requested

# Primeiro passo: escutar requisições do broker
# Segundo passo: identificar as variáveis do broker e criar Clients

class Client:
    def __init__(self, name, host, port):
        self.name = name  # Único.
        self.broker = [{'host': '127.0.0.1', 'port': 8079},
                       {'host': '127.0.0.1', 'port': 8080}]  # Conectado ao BACKUP (:8079)
        self._host = host
        self._port = port
        self._lock = threading.Lock()
        self.variablesContext = []
        self.variablesNames = []
        self.terminate = False

    def listen(self, event):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # print("Listening on [%s:%s]" % (self._host, self._port))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self._host, self._port))
            s.listen()

            while not event.is_set() and not self.terminate:  # Tempo da main thread / mensagem de término do broker.

                conn, addr = None, None
                try:
                    print("====> tentando conexão!!!! : ")
                    conn, addr = s.accept()  # (!) Bloqueia a execução!
                    print("====> a conexão!!!! : ")
                except socket.timeout:
                    continue

                with conn:
                    print("====> recebemos conexão!!!! : ", msg)
                    data = conn.recv(4096)  # Recebe resposta do broker.
                    msg = pickle.loads(data)  # Recebe o array (queue) do Broker / mensagem de término.
                    print("====> recebemos mensagem!!!! : ", msg)
                    # todo thread para acquire/release ok?
                    # iterando de 2 em 2 pois a mensagem vem no seguinte formato
                    # ['-var-X', ['Débora'], '-var-Y', []]
                    for i in range(0, len(msg), 2):
                        if i % 0:

                            if not msg[i] in self.variablesNames:
                                self.variablesNames.append(msg[i])
                                queue = None if not msg[i+1] else msg[i+1]  # pega a fila se tiver, None se não tiver
                                newvar = VariableContext(self, msg[i], queue, True, False)
                                self.variablesContext.append(newvar)
                            else:
                                for var in self.variablesContext:
                                    if self.variablesContext[var].var_name == msg[i]:
                                        if 'okr' in msg[i+1]:
                                            self.variablesContext[var].handle_msg_okr()
                                        elif isinstance(msg[i+1], list):
                                            self.variablesContext[var].handle_update_queue(msg[i+1])

                    conn.close()

        print("Closing listen thread.")

    def connect_to_broker(self, host, msg, flag=False):
        self.send(host, msg)
        with self._lock:
            self.requested = True

    def try_connection(self, msg):
        try:
            self.connect_to_broker(self.broker[0], msg)
        except ConnectionRefusedError:
            print("Connection refused. Notifying backup broker ...")
            if (len(self.broker) > 1):
                try:
                    self.connect_to_broker(self.broker[1], msg, True)
                except ConnectionRefusedError:
                    print("Connection REFUSED 😡")
            else:
                print('There is no broker left. 😢')

    def request(self, event):
        # print("Entering request thread as %s" % self.name)

        while not event.is_set() and not self.terminate:
            # todo thread para esse caso
            for var in self.variablesContext:
                self.variablesContext[var].handle_use_variable()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("[%s] Closing request thread." % self.name)
            s.connect((self.broker[0]['host'], self.broker[0]['port']))
            s.sendall(pickle.dumps('%s exited' % self.name))  # Manda mensagem final.

            # todo esvaziar todas as queues
            for var in self.variablesContext:
                self.variablesContext[var].queue = None

    def send(self, host, m):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host['host'], host['port']))
            msg = pickle.dumps(m)
            s.sendall(msg)

    def checkBroker(self, event):
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
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            executor.submit(self.listen, event)  # Thread para escutar o broker.
            executor.submit(self.request, event)  # Thread para mandar mensagem para o broker.
            executor.submit(self.checkBroker, event)  # - Are you there?

            time.sleep(duracao)  # Tempo da aplicação.
            event.set()


with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    executor.submit(Client('Débora', '127.0.0.1', 8081).start)
    # executor.submit(Client('Felipe', '127.0.0.1', 8082).start)
    # executor.submit(Client('Gabriel', '127.0.0.1', 8083).start)


# Util.

def port(port):
    if port == "":
        return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', int(port))) == 0

class VariableContext:
    def __init__(self, client, var_name, queue, okr, requested):
        self.client = Client(client)
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
                print('\n[%s]: Queue atualizada: ação release %s' % (self.client.name, self.queue))
            elif msg[0] == '%app%':  # Atualização na queue (próximo acquire recebido).
                self.queue.append(msg[1])
                print('\n[%s]: Queue atualizada: ação acquire %s' % (self.client.name, self.queue))
            else:
                self.deal_with_queue(msg)
        else:
            self.deal_with_queue(msg)

    def deal_with_queue(self, msg):
        # mensagem aqui: ['-var-X', ['Débora'], '-var-Y', []]
        print('\n[%s]: Queue atualizada: ' % self.client.name, end='')
        if self.queue == None:  # Subscribe.
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
            self.client.try_connection(self.client.name + ' -acquire %s %s %s' % (self.var_name, self.client._host, self.client._port))

        elif self.queue != None and self.okr:  # Já deu subscribe.
            time.sleep(
                0.5)  # Pode ter casos em que o broker vá receber acquire corretamente mais levará mais que 0.5s para responder, o que irá gerar acquire duplo.
            if self.client.name not in self.queue:  # Tratando caso em que broker principal cai logo após receber acquire.
                # print('\n(!) ---> [%s] My acquire request somehow was never received ... 🤔 trying again!' % self.name)
                self.requested = False
                # todo: coloquei elif e apaguei o continue

            elif len(self.queue) > 0:
                proximo = self.queue[0]  # Ex.: ['Débora', '-acquire', '-var-X']
                if proximo == self.client.name:
                    time.sleep(random.uniform(0.2, 0.5))  # Faça algo com var-X

                    self.client.try_connection(self.client.name + ' -release %s ' + self.client._host + ' ' + str(self.client._port) % (self.var_name))
                    # print('%s liberou o recurso' % self.name)
                    with self.client._lock:
                        self.okr = False