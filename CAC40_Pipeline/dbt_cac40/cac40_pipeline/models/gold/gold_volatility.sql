-- Modèle Gold Volatilité : calcul de la volatilité par symbole.
-- Utilisé pour le treemap du dashboard Power BI.

WITH stats AS (
    SELECT
        symbole,
        AVG(prix_actuel)                           AS prix_moyen,
        STDDEV(prix_actuel)                        AS volatilite,
        COUNT(*)                                   AS nb_observations,
        MAX(fetch_timestamp)                       AS derniere_maj
    FROM {{ ref('silver_cac40_quotes') }}
    GROUP BY symbole
)

SELECT
    symbole,
    ROUND(prix_moyen, 2)      AS prix_moyen,
    ROUND(volatilite, 4)      AS volatilite,

    -- volatilité relative : entre 0 et 1 pour le gradient de couleur Power BI
    ROUND(
        volatilite / NULLIF(MAX(volatilite) OVER (), 0),
    4)                        AS volatilite_relative,

    nb_observations,
    derniere_maj
FROM stats