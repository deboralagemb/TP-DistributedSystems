# Rodar preferencialmente em LINUX.
# Usa select(), não threads!

import selectors
from socket import *
import types

host = '127.0.0.1'
port = 8080

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


sel = selectors.DefaultSelector()
lsock = socket(AF_INET, SOCK_STREAM)
lsock.bind((host, port))
lsock.listen()
print('listening on', (host, port))

# Não bloqueará a execução.
lsock.setblocking(False)

# 'data' é qualquer mensagem que você queira atrelar ao socket.
sel.register(lsock, selectors.EVENT_READ, data=None)

while True:
    # select() NÃO FUNCIONA BEM NO WINDOWS pois dá erro no atributo 'timeout'.
    # O problema é só no Windows, porém os sockets são recebidos e enviados normalmente.
    
    # Bloqueia até que tenha sockets prontos para I/O.
    # Retorna lista de tuplas (key, events) para cada socket.
    # Se key.data == None, então espera um socket do client.
    
    try:
        events = sel.select(timeout=1)  # timeout em segundos [Float].
        for key, mask in events:
            if key.data is None:
                accept_wrapper(key.fileobj)
            else:
                service_connection(key, mask)
                
    except OSError:
        pass
    
    except KeyboardInterrupt:
        break
    
#lsock.shutdown(1)
lsock.close()  # Libera a porta.



# =============================================================================
# # Caso a porta não esteja liberada por um erro do programa:
# from psutil import process_iter
# from signal import SIGTERM # or SIGKILL
# for proc in process_iter():
#     for conns in proc.connections(kind='inet'):
#         if conns.laddr.port == port:  # ex.: 8080
#             proc.send_signal(SIGTERM) # or SIGKILL
# =============================================================================
