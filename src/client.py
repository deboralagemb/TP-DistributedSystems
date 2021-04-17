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


class Client:
    def __init__(self, name, host, port):
        self.name = name  # Único.
        self.broker = [{'host': '127.0.0.1', 'port': 8079},
                       {'host': '127.0.0.1', 'port': 8080}]  # Conectado ao BACKUP (:8079)
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
                print('sincronizando contexto com o broker backup')
                # notify() também entraria aqui

            self.queue = _queue

            self.okr = True  # Caso seja sincronização com o backup e o cliente tenha mandado mensagem de release para o principal não lida.
            if self.name in self.queue:
                self.requested = True

                # print(event.is_set(), self.terminate)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # print("Listening on [%s:%s]" % (self._host, self._port))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self._host, self._port))
            s.listen()

            while not event.is_set() and not self.terminate:  # Tempo da main thread / mensagem de término do broker.

                conn, addr = None, None
                try:
                    conn, addr = s.accept()  # (!) Bloqueia a execução!
                except socket.timeout:
                    continue

                with conn:
                    data = conn.recv(4096)  # Recebe resposta do broker.
                    msg = pickle.loads(data)  # Recebe o array (queue) do Broker / mensagem de término.

                    with self._lock:
                        if msg == 'okr':
                            self.requested = False
                            self.okr = True

                        elif isinstance(msg, list):
                            if len(msg) > 0:
                                if msg[0] == '%pop%':
                                    self.queue.pop(0)
                                    print('\n[%s]: Queue atualizada: ação release %s' % (self.name, self.queue))
                                elif msg[0] == '%app%':  # Atualização na queue (próximo acquire recebido).
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

            if not self.requested:  # Se já não mandou um 'acquire'.
                aleatorio = random.uniform(0.5, 2)
                time.sleep(aleatorio)

                self.try_connection(self.name + ' -acquire -var-X %s %s' % (self._host, self._port))

            elif self.queue != None and self.okr:  # Já deu subscribe.
                time.sleep(
                    0.5)  # Pode ter casos em que o broker vá receber acquire corretamente mais levará mais que 0.5s para responder, o que irá gerar acquire duplo.
                if self.name not in self.queue:  # Tratando caso em que broker principal cai logo após receber acquire.
                    print(
                        '\n(!) ---> [%s] My acquire request somehow was never received ... 🤔 trying again!' % self.name)
                    self.requested = False
                    continue

                if len(self.queue) > 0:
                    proximo = self.queue[0]  # Ex.: ['Débora', '-acquire', '-var-X']
                    if proximo == self.name:
                        time.sleep(random.uniform(0.2, 0.5))  # Faça algo com var-X

                        self.try_connection(self.name + ' -release -var-X ' + self._host + ' ' + str(self._port))
                        print('%s liberou o recurso' % self.name)
                        with self._lock:
                            self.okr = False

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            # s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            print("[%s] Closing request thread." % self.name)
            s.connect((self.broker[0]['host'], self.broker[0]['port']))
            s.sendall(pickle.dumps('%s exited' % self.name))  # Manda mensagem final.
            self.queue = None

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
    executor.submit(Client('Felipe', '127.0.0.1', 8082).start)
    executor.submit(Client('Gabriel', '127.0.0.1', 8083).start)


# Util.

def port(port):
    if port == "":
        return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', int(port))) == 0
