SELECT *
FROM dbo.joconde_oeuvres_temporelle
FOR SYSTEM_TIME AS OF '2024-01-01T00:00:00'
WHERE reference = '00000055011';