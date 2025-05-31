USE Joconde;
GO

CREATE TABLE dbo.joconde_oeuvres_temporelle (
    reference VARCHAR(20) NOT NULL PRIMARY KEY,
    appellation NVARCHAR(255),
    auteur NVARCHAR(1500),
    annee_creation INT,
    departement VARCHAR(30),
    description NVARCHAR(MAX),
    date_import_utc DATETIMEOFFSET(3),
    source_system VARCHAR(20),

    -- colonnes temporelles
    sys_start_time DATETIME2 GENERATED ALWAYS AS ROW START NOT NULL,
    sys_end_time DATETIME2 GENERATED ALWAYS AS ROW END NOT NULL,
    PERIOD FOR SYSTEM_TIME (sys_start_time, sys_end_time)
)
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.joconde_oeuvres_temporelle_archive));
GO