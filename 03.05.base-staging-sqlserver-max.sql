USE [joconde_staging]
GO

DROP TABLE [staging].[joconde]
GO

CREATE TABLE [staging].[joconde](
	[reference] [nvarchar](max) NULL,
	[appellation] [nvarchar](max) NULL,
	[auteur] [nvarchar](max) NULL,
	[date_creation] [nvarchar](max) NULL,
	[region] [nvarchar](max) NULL,
	[departement] [nvarchar](max) NULL,
	[description] [nvarchar](max) NULL,
	[load_timestamp_utc] [datetimeoffset](3) NOT NULL DEFAULT (getutcdate()),
	[source_system] [varchar](50) NULL,
	[load_process] [varchar](50) NULL
) 
WITH ( DATA_COMPRESSION = ROW)
GO
