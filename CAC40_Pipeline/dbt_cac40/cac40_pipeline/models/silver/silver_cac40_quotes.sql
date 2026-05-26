-- Modèle Silver : nettoyage et enrichissement de la couche Bronze.
-- Arrondi à 2 décimales, filtre les lignes nulles,
-- ajout de colonnes dérivées utiles pour les analyses.

SELECT
    symbole,

    -- arrondi à 2 décimales pour homogénéité
    ROUND(prix_actuel, 2)        AS prix_actuel,
    ROUND(prix_ouverture, 2)     AS prix_ouverture,
    ROUND(prix_haut, 2)          AS prix_haut,
    ROUND(prix_bas, 2)           AS prix_bas,
    ROUND(cloture_precedente, 2) AS cloture_precedente,
    volume,
    fetch_timestamp,

    -- variation absolue par rapport à la clôture précédente
    ROUND(prix_actuel - cloture_precedente, 2)        AS variation_absolue,

    -- variation en pourcentage
    ROUND(
        (prix_actuel - cloture_precedente) / NULLIF(cloture_precedente, 0) * 100,
    2)                                                 AS variation_pct,

    -- extraction de la date seule pour les agrégations journalières
    DATE(fetch_timestamp)                              AS trade_date

FROM {{ ref('bronze_cac40_quotes') }}

-- filtre les lignes sans prix (données corrompues)
WHERE prix_actuel IS NOT NULL
  AND prix_actuel > 0