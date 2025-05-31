import json
import os
import time
from pathlib import Path
from datetime import datetime
import yaml

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Configuration
fichier = 'C:\\Users\\linkedin\\base-joconde-extrait.json'
BATCH_SIZE = 10          # nombre d'enregistrements par fichier
INTERVAL_SECONDS = 5     # délai entre chaque dépôt de fichier

# Préparer le répertoire de sortie
os.makedirs(config["watchdog"]["input_directory"], exist_ok=True)

# Charger les données sources
with open(fichier, encoding="utf-8") as f:
    data = json.load(f)

# Découpe en lots et export
for i in range(0, len(data), BATCH_SIZE):
    batch = data[i:i+BATCH_SIZE]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"joconde_batch_{i//BATCH_SIZE + 1}_{timestamp}.json"
    filepath = Path(config["watchdog"]["input_directory"]) / filename

    with open(filepath, "w", encoding="utf-8") as f_out:
        json.dump(batch, f_out, ensure_ascii=False, indent=2)

    print(f"Fichier déposé : {filepath.name} ({len(batch)} notices)")
    time.sleep(INTERVAL_SECONDS)
