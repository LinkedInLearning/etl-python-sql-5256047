USE joconde_staging;
GO

SELECT COUNT(*) FROM staging.joconde;

-- Nombre d’œuvres par région
SELECT
    region,
    COUNT(*) AS nombre_oeuvres
FROM staging.joconde
GROUP BY region
ORDER BY nombre_oeuvres DESC;

-- Répartition par département et présence d’auteur
SELECT
    departement,
    COUNT(*) AS total,
    COUNT(CASE WHEN auteur IS NULL THEN 1 END) AS sans_auteur,
    COUNT(CASE WHEN auteur IS NOT NULL THEN 1 END) AS avec_auteur
FROM staging.joconde
GROUP BY departement
ORDER BY total DESC;

-- Auteurs les plus fréquents
SELECT
    auteur,
    COUNT(*) AS oeuvres
FROM staging.joconde
WHERE auteur IS NOT NULL
GROUP BY auteur
ORDER BY oeuvres DESC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;

-- Nombre moyen de caractères dans la description par région
SELECT
    region,
    AVG(LEN(description)) AS longueur_moyenne
FROM staging.joconde
WHERE description IS NOT NULL
GROUP BY region
ORDER BY longueur_moyenne DESC;
GO

CREATE OR ALTER VIEW staging.v_auteurs_par_departement
AS
SELECT TOP 100 PERCENT
    departement,
    COUNT(*) AS total,
    COUNT(CASE WHEN auteur IS NULL THEN 1 END) AS sans_auteur,
    COUNT(CASE WHEN auteur IS NOT NULL THEN 1 END) AS avec_auteur
FROM staging.joconde
GROUP BY departement
ORDER BY total DESC;
GO

SELECT *
FROM staging.v_auteurs_par_departement;