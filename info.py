
# Tratando sockets através de select(), não threads!


import _thread as thread
from socket import *
import time

import selectors
sel = selectors.DefaultSelector()
# ...
lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
lsock.bind((host, port))
lsock.listen()
print('listening on', (host, port))
# Não bloqueará a execução.
lsock.setblocking(False)
# 'data' é qualquer mensagem que você queira atrelar ao socket.
sel.register(lsock, selectors.EVENT_READ, data=None)


# =============================================================================

sel = selectors.DefaultSelector()

# ...

while True:
    # Bloqueia até que tenha sockets prontos para I/O.
    # Retorna lista de tuplas (key, events) para cada socket.
    # Se key.data == None, então é um socket do client.
    events = sel.select(timeout=None)
    for key, mask in events:
        if key.data is None:
            accept_wrapper(key.fileobj)
        else:
            service_connection(key, mask)


# =============================================================================
# Broker

def accept_wrapper(sock):
    conn, addr = sock.accept()  # Should be ready to read
    print('accepted connection from', addr)
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b'', outb=b'')
    
    # Guarda os dados que queremos incluídos junto com o socket.
    # Queremos saber quando o cliente está pronto para reading ou writing.
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sel.register(conn, events, data=data)
    


# mask contém os eventos que estão prontos.
# key contém o objeto socket.
def service_connection(key, mask):
    sock = key.fileobj
    data = key.data
    if mask & selectors.EVENT_READ:
        recv_data = sock.recv(1024)  # Should be ready to read
        if recv_data:
            # Append qualquer mensagem recebida na variável data.outb.
            data.outb += recv_data
        else:
            print('closing connection to', data.addr)
            
            # O socket não é mais monitorado pelo select().
            sel.unregister(sock)
            
            sock.close()
            
    if mask & selectors.EVENT_WRITE:
        if data.outb:
            print('echoing', repr(data.outb), 'to', data.addr)
            sent = sock.send(data.outb)  # Should be ready to write
            
            # Bytes removidos do buffer.
            data.outb = data.outb[sent:]


# =============================================================================
# Client

messages = [b'Message 1 from client.', b'Message 2 from client.']


def start_connections(host, port, num_conns):
    server_addr = (host, port)
    for i in range(0, num_conns):
        connid = i + 1
        print('starting connection', connid, 'to', server_addr)
        sock = socket(AF_INET, SOCK_STREAM)
        sock.setblocking(False)
        
        # Ao contrrário de connect(), não lança um IO Exception.
        sock.connect_ex(server_addr)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        data = types.SimpleNamespace(connid=connid,
                                     msg_total=sum(len(m) for m in messages),  # Bytes total.
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
            # Caso ahja erro no cliente e ele não feche a conexão, permanecerá sempre aberto no server.
            # Isto deve ser tratado colocando um timeout no lado do servidor.
            sock.close()
            
    if mask & selectors.EVENT_WRITE:
        if not data.outb and data.messages:
            data.outb = data.messages.pop(0)
        if data.outb:
            print('sending', repr(data.outb), 'to connection', data.connid)
            sent = sock.send(data.outb)  # Should be ready to write
            data.outb = data.outb[sent:]




    