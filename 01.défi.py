from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(
    # filename="etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# logging.info("Début du traitement")

# Erreur !
# logging -> une erreur s'est produite.

try:
    raise ValueError("Erreur !")
except Exception as e:
    logging.exception("une erreur s'est produite.")