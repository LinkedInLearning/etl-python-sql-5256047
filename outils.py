import time
import logging

def chronometre(operation):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = operation(*args, **kwargs)
        print(f"⏱ {operation.__name__} : {round(time.time() - start, 2)} sec")
        return result
    return wrapper

def chronometre_logging(operation):
    def wrapper(*args, **kwargs):
        start = time.time()
        result = operation(*args, **kwargs)
        logging.info(f"⏱ {operation.__name__} : {round(time.time() - start, 2)} sec")
        return result
    return wrapper

def chronometre_logging_lambda(etape: str, operation):
    start = time.time()
    result = operation()
    duree = round(time.time() - start, 2)
    logging.info(f"⏱ {etape} terminé en {duree}s")
    return result