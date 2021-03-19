import _thread as thread
from socket import *
import time

class Broker:
    def __init__(self):
        self.host = '127.0.0.1'
        self.port = 65432  # 1-65535
        #self.sockobj = socket(socket.AF_INET, socket.SOCK_STREAM)

    def start(self):
        portInput = input("Enter the Broker port number: ")
        self.port = 65432 if portInput == "" else int(portInput)
        
        # Não é necessário chamar s.close().
        # AF_INET é a família de endereços para IPV4 (tupla (host, port)).
        # Com SOCK_STREAM, o protocolo padrão é o TCP.
        with socket(AF_INET, SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            
            # Habilita o broker a aceitar conexões.
            # Parâmetro: número limite de conexões recusadas para que o servidor recuse quaisquer novas conexões.
            s.listen()
            
            # Bloqueia a execução e espera por novas conexões.
            # Retorna a conexão (socket do cliente) e uma tupla (host, port) - para IPV4.
            conn, addr = s.accept()
            with conn:  # Socket para comunicação com o cliente.
                print('Connected by ', addr)
                while True:
                    # Lê a mensagem do cliente.
                    data = conn.recv(1024)
                    
                    # Se a mensagem for vazia, a conexão é terminada.
                    # Como está no contexto do 'with', o socket é automaticamente fechado.
                    if not data:
                        break
                    
                    # Manda de volta a mesma mensagem (Não é o que faremos).
                    conn.sendall(data)


    def printClient(self):
        return "Client message: "

    def dealWithClient(self, connection, answer):
        # Simula atividade no bloco
        data = connection.recv(1024)
        print (self.printClient(), data.decode())
        time.sleep(1)
        connection.send(answer.encode())
        connection.close

broker = Broker()
broker.start()
