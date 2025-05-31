USE Joconde;
GO

MERGE dbo.joconde_oeuvres_temporelle AS cible
USING (
    SELECT
        TRIM(reference) AS reference,
        TRIM(appellation) AS appellation,
        TRIM(auteur) AS auteur,
        TRY_CAST(LEFT(date_creation, 4) AS INT) AS annee_creation,
        TRIM(departement) AS departement,
        description,
        load_timestamp_utc as date_import_utc,
        source_system
    FROM joconde_staging.staging.joconde
    WHERE reference IS NOT NULL
) AS source
ON source.reference = cible.reference

WHEN MATCHED AND (
    ISNULL(source.appellation, '') <> ISNULL(cible.appellation, '') OR
    ISNULL(source.auteur, '') <> ISNULL(cible.auteur, '') OR
    ISNULL(source.annee_creation, -1) <> ISNULL(cible.annee_creation, -1) OR
    ISNULL(source.departement, '') <> ISNULL(cible.departement, '') OR
    ISNULL(source.description, '') <> ISNULL(cible.description, '')
) THEN
    UPDATE SET
        appellation = source.appellation,
        auteur = source.auteur,
        annee_creation = source.annee_creation,
        departement = source.departement,
        description = source.description,
        date_import_utc = source.date_import_utc,
        source_system = source.source_system

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        reference, appellation, auteur, annee_creation,
        departement, description, date_import_utc, source_system
    )
    VALUES (
        source.reference, source.appellation, source.auteur,
        source.annee_creation, source.departement,
        source.description, source.date_import_utc, source.source_system
    );
