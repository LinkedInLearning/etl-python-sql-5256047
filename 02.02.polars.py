from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
import urllib
import os
import sys
import pandas as pd
import polars as pl

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
    df_pandas = pd.read_sql(query, con=engine)
    df_polars = pl.read_database(query, connection=engine)
except OperationalError as e:
    logging.error("Échec de connexion à la base : %s", e)
    sys.exit(1)

# Calcul mémoire
mem_pandas = df_pandas.memory_usage(deep=True).sum()
mem_polars = df_polars.estimated_size(unit='b')

# Affichage
print(f"Mémoire utilisée par pandas : {round(mem_pandas / (1024**2), 2)} Mo")
print(f"Mémoire utilisée par polars : {round(mem_polars / (1024**2), 2)} Mo")