from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
import urllib
import os
import sys
import pandas as pd

import logging

logging.basicConfig(
    filename="etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()

params = urllib.parse.quote_plus(
    "DRIVER=ODBC Driver 17 for SQL Server;"
    "SERVER=localhost;"
    "DATABASE=PachadataFormation;"
    "UID=rudi;"
    f"PWD={os.getenv("SQLSERVER_PASSWORD")};"
    "TrustServerCertificate=yes;"
)

engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

query = """
SELECT nom, prenom, email
FROM Contact.Contact;
"""

try:
    df = pd.read_sql(query, con=engine)
except OperationalError as e:
    logging.error("Échec de connexion à la base : %s", e)
    sys.exit(1)

print(df.head())