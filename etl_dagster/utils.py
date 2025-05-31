import yaml

def get_config():
    with open("config.yaml", "r") as f:
        return yaml.safe_load(f)
