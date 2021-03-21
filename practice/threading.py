import logging
import threading
import time
import concurrent.futures

class Resource:
    def __init__(self):
        self.value = 0
        self._lock = threading.Lock()

    def update(self, name):
        logging.info("Thread %s: starting update", name)
        logging.debug("Thread %s about to lock", name)
        with self._lock:
            logging.debug("Thread %s has lock", name)
            local_copy = self.value  # 'Thread-safe' - variáveis locais em uma função são seguras em threads, pois cópias dela serão feitas.
            local_copy += 1
            time.sleep(0.1)
            self.value = local_copy
            logging.debug("Thread %s about to release lock", name)
        logging.debug("Thread %s after release", name)
        logging.info("Thread %s: finishing update", name)


def thread_function(name):
    logging.info("Thread %s: starting", name)
    time.sleep(2)
    logging.info("Thread %s: finishing", name)

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")
    
    resource = Resource()
    logging.info("Testing update. Starting value is %d.", resource.value)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        for index in range(2):
            executor.submit(resource.update, index)
    
    logging.info("Testing update. Ending value is %d.", resource.value)
    logging.getLogger().setLevel(logging.DEBUG)



# =============================================================================
#     with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
#         executor.map(thread_function, range(3))  # Starting multiple threads
# =============================================================================

# =============================================================================
#     logging.info("Main    : before creating thread")
#     x = threading.Thread(target=thread_function, args=(1,))
#     logging.info("Main    : before running thread")
#     x.start()
#     logging.info("Main    : wait for the thread to finish")
#     x.join()
#     logging.info("Main    : all done")
# =============================================================================


# =============================================================================
# # Serialization example
# 
# import numpy as np
# import pickle
# 
# # Serializing
# arr = np.array([ [1, 2, 3], [4, 5, 6], [7, 8, 9] ])
# arr = pickle.dumps(arr)
# print(arr)
# 
# # Deserializing
# print(pickle.loads(arr))
# =============================================================================

