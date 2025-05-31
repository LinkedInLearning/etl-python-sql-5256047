USE Joconde;
GO

-- vérification des doublons
SELECT reference, COUNT(*)
FROM joconde_staging.staging.joconde
GROUP BY reference
HAVING COUNT(*) > 1
ORDER BY reference

-- taille des colonnes
SELECT
    MAX(LEN(reference))      AS max_len_reference,
    MAX(LEN(appellation))    AS max_len_appellation,
    MAX(LEN(auteur))         AS max_len_auteur,
    MAX(LEN(date_creation))  AS max_len_date_creation,
    MAX(LEN(region))         AS max_len_region,
    MAX(LEN(departement))    AS max_len_departement,
    MAX(LEN(description))    AS max_len_description,
    MAX(LEN(source_system))  AS max_len_source_system,
    MAX(LEN(load_process))   AS max_len_load_process
FROM joconde_staging.staging.joconde;
GO

-- table de référence
CREATE SCHEMA ref;
GO

CREATE TABLE ref.departement_region (
    departement VARCHAR(30) PRIMARY KEY NOT NULL,
    region VARCHAR(30) NOT NULL
);

INSERT INTO ref.departement_region
SELECT DISTINCT COALESCE(departement, 'inconnu'), COALESCE(region, 'inconnu')
FROM joconde_staging.staging.joconde;

-- création de la table de destination
CREATE TABLE dbo.joconde_oeuvre (
    reference VARCHAR(20) PRIMARY KEY NOT NULL,
    appellation NVARCHAR(255),
    auteur NVARCHAR(1500),
    annee_creation INT,
    departement VARCHAR(30) REFERENCES ref.departement_region (departement),
    description NVARCHAR(MAX),
    date_import_utc DATETIMEOFFSET(3),
    source_system VARCHAR(20)
)
