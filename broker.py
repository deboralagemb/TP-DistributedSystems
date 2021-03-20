import _thread as thread
from socket import *
import time

class Broker:
    def __init__(self):
        self.host = '127.0.0.1'
        self.port = 8080  # 1-65535
        #self.sockobj = socket(AF_INET, SOCK_STREAM)

    def start(self):
        portInput = input("Enter the Broker port number: ")
        self.port = 8080 if portInput == "" else int(portInput)
        print("Default port number selected: " + str(self.port)) if portInput == "" else {}
        print("Listening...", end="\n\n")
        
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
                    # Parâmetro: maior quantidade de dados recebidos de uma só vez (bytes).
                    # Nota: Isso não quer dizer que ele vá receber a mensagem inteira, menor que 1024 bytes!
                    # .send() se comporta da mesma maneira! (E retorna a qtde de dados enviados, em bytes).
                    data = conn.recv(1024)
                    
                    # Se a mensagem for vazia, a conexão é terminada.
                    # Como está no contexto do 'with', o socket é automaticamente fechado.
                    if not data:
                        break
                    
                    # Manda de volta a mesma mensagem (Não é o que faremos).
                    # Retorna 'None' em caso de sucesso.
                    conn.sendall(data)
                    

broker = Broker()
broker.start()
