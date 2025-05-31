CREATE DATABASE joconde_staging
ON
( NAME = N'joconde_staging', FILENAME = N'/var/opt/mssql/data/joconde_staging.mdf' , 
         SIZE = 500MB , FILEGROWTH = 64MB )
 LOG ON 
( NAME = N'joconde_staging_log', FILENAME = N'/var/opt/mssql/data/joconde_staging_log.ldf' , 
         SIZE = 100MB , FILEGROWTH = 64MB )
GO
ALTER DATABASE [joconde_staging] SET RECOVERY SIMPLE 
ALTER DATABASE [joconde_staging] SET DELAYED_DURABILITY = FORCED 
GO

USE joconde_staging;
GO

CREATE SCHEMA staging;
GO

CREATE TABLE staging.joconde (
    --id INT IDENTITY PRIMARY KEY NOT NULL,
    reference NVARCHAR(50),
    appellation NVARCHAR(255),
    auteur NVARCHAR(255),
    date_creation NVARCHAR(50),
    region NVARCHAR(100),
    departement NVARCHAR(100),
    description NVARCHAR(MAX),
    load_timestamp_utc DATETIMEOFFSET(3) DEFAULT GETUTCDATE(),
    source_system VARCHAR(50),
    load_process varchar(50)
) WITH (DATA_COMPRESSION = ROW);
GO

-- permissions

USE [master]
GO
CREATE LOGIN [joconde_import] 
WITH PASSWORD=N'MsacR%GK85.VoykyEU', 
DEFAULT_DATABASE=[joconde_staging], 
CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO

USE joconde_staging
GO
CREATE USER [joconde_import] FOR LOGIN [joconde_import];
GO

GRANT SELECT, INSERT, DELETE, ALTER ON SCHEMA::staging TO [joconde_import];
GO

EXECUTE AS USER = 'joconde_import'

TRUNCATE TABLE staging.joconde;

REVERT;