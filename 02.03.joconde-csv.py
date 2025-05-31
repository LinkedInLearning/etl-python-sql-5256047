import polars as pl
import outils

@outils.chronometre
def charger_fichier(path):
    return pl.read_csv(path, separator=";", schema_overrides={"Date_entree_dans_le_domaine_public": pl.Utf8})
    # df = pl.read_csv("joconde.csv", encoding="ISO-8859-1")
    # df = pl.read_csv("joconde.csv", columns=["Reference", "Appellation", "Ville"])
    # Lire en streaming si le fichier est très gros :
    # df = pl.read_csv("joconde.csv", low_memory=True)

fichier = 'C:\\Users\\linkedin\\base-joconde-extrait.csv'
df = charger_fichier(fichier)

print(f"Mémoire utilisée : {round(df.estimated_size(unit='b') / (1024**2), 2)} Mo")

print(df.head(5))
df = df.select(["Reference", "Appellation", "Ville"])
print(df.columns)

# Gérer les valeurs manquantes :
# df = df.drop_nulls()
