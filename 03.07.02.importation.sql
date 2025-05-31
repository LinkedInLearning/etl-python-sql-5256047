USE Joconde;
GO

INSERT INTO dbo.joconde_oeuvre (
    reference,
    appellation,
    auteur,
    annee_creation,
    departement,
    description,
    date_import_utc,
    source_system
)
SELECT
    -- Nettoyage des identifiants
    TRIM(reference) AS reference,

    -- Formatage des colonnes texte
    TRIM(appellation) AS appellation,
    TRIM(auteur) AS auteur,

    -- Extraction de l'année depuis une chaîne comme "1997" ou "19e siècle"
    TRY_CAST(LEFT(TRIM(date_creation), 4) AS INT) AS annee_creation,

    -- On ne garde que les départements qui existent dans la table de référence
    TRIM(s.departement) AS departement,

    -- Description éventuellement tronquée si très longue
    CASE 
        WHEN LEN(description) > 1000 THEN LEFT(description, 1000) + '...'
        ELSE description
    END AS description,

    -- Reprise du timestamp d’import
    s.load_timestamp_utc,
    s.source_system
FROM joconde_staging.staging.joconde s
INNER JOIN ref.departement_region d
    ON COALESCE(s.departement, 'inconnu') = d.departement
WHERE
    s.reference IS NOT NULL
    AND LEFT(TRIM(s.date_creation), 4) LIKE '[1-2][0-9][0-9][0-9]'  -- filtrage années plausibles
    AND TRY_CAST(LEFT(TRIM(s.date_creation), 4) AS INT) BETWEEN 1000 AND YEAR(GETDATE())
