# Rodar preferencialmente em LINUX.
# Usa select(), não threads!

import selectors
from socket import *
import types

host = '127.0.0.1'
port = 8080
num_conns = 3


sel = selectors.DefaultSelector()
messages = [b'Message 1 from client.', b'Message 2 from client.']
    

def start_connections(host, port, num_conns):
    server_addr = (host, port)
    for i in range(0, num_conns):
        connid = i + 1
        print('starting connection', connid, 'to', server_addr)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setblocking(False)
        
        # Ao contrário de connect(), não lança um IO Exception.
        sock.connect_ex(server_addr)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        data = types.SimpleNamespace(connid=connid,
                                     msg_total=sum(len(m) for m in messages),
                                     recv_total=0,
                                     messages=list(messages),
                                     outb=b'')
        sel.register(sock, events, data=data)

        
def service_connection(key, mask):
    sock = key.fileobj
    data = key.data
    
    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(1024)  # Should be ready to read
        if recv_data:
            print('received', repr(recv_data), 'from connection', data.connid)
            data.recv_total += len(recv_data)
            
        # Como o server retorna a mesma mensagem, aqui apenas verifica o tamanho da mensagem de retorno (data.recv_total == data.msg_total).
        if not recv_data or data.recv_total == data.msg_total:
            print('closing connection', data.connid)
            sel.unregister(sock)
            
            # O servidor detecta este fechamento e também fecha do lado dele.
            # Caso haja erro no cliente e ele não feche a conexão, permanecerá sempre aberto no server.
            # Isto deve ser tratado colocando um timeout no lado do servidor.
            sock.close()
            
    if mask & selectors.EVENT_WRITE:
        if not data.outb and data.messages:
            data.outb = data.messages.pop(0)
        if data.outb:
            print('sending', repr(data.outb), 'to connection', data.connid)
            sent = sock.send(data.outb)  # Should be ready to write
            data.outb = data.outb[sent:]


start_connections(host, port, num_conns)


# select() NÃO FUNCIONA BEM NO WINDOWS pois dá erro no atributo 'timeout'.
# O problema é só no Windows, porém os sockets são recebidos e enviados normalmente.
try:
    events = sel.select(timeout=1)  # timeout em segundos [Float].
    for key, mask in events:
        service_connection(key, mask)
        
except OSError:
    pass





