import _thread as thread
from socket import *
import concurrent.futures
import queue
import random
import time
import logging
import threading

duracao = 5

# Nem mexi aqui, só no client.
# todo, usar .select() para escutar os clientes e evitar conexões recusadas.


class Resource:
    def __init__(self):
        self.value = 0
        self._lock = threading.Lock()


class Broker:
    def __init__(self):
        self.host = '127.0.0.1'
        self.port = 8080  # 1-65535

    def start(self):
        portInput = input("Enter the Broker port number: ")
        self.port = 8080 if portInput == "" else int(portInput)
        print("Default port number selected: " + str(self.port)) if portInput == "" else {}
        print("Listening...", end="\n\n")
        
        with socket(AF_INET, SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            
            t_end = time.time() + duracao            
            while time.time() < t_end:
                s.listen()            
                conn, addr = s.accept()
                with conn:
                    print('Connected by ', addr)
                    while True:
                        data = conn.recv(1024)
                        if not data:
                            break
                        conn.sendall(data)
                    

broker = Broker()
broker.start()


# =============================================================================
# def producer(queue, event):
#     """Pretend we're getting a number from the network."""
#     while not event.is_set():
#         message = random.randint(1, 101)
#         logging.info("Producer got message: %s", message)
#         queue.put(message)
# 
#     logging.info("Producer received event. Exiting")
# 
# def consumer(queue, event):
#     """Pretend we're saving a number in the database."""
#     while not event.is_set() or not queue.empty():
#         message = queue.get()
#         logging.info(
#             "Consumer storing message: %s (size=%d)", message, queue.qsize()
#         )
# 
#     logging.info("Consumer received event. Exiting")
# 
# if __name__ == "__main__":
#     format = "%(asctime)s: %(message)s"
#     logging.basicConfig(format=format, level=logging.INFO,
#                         datefmt="%H:%M:%S")
# 
#     pipeline = queue.Queue(maxsize=10)
#     event = threading.Event()
#     with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
#         executor.submit(producer, pipeline, event)
#         executor.submit(consumer, pipeline, event)
# 
#         time.sleep(0.1)
#         logging.info("Main: about to set event")
#         event.set()
#         logging.info("Main: event set.")
# =============================================================================
