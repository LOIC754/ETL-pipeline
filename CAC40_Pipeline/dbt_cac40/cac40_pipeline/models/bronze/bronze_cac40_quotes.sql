-- Modèle Bronze : parse le JSON brut (colonne V de type VARIANT)
-- et projette chaque champ avec son type correct.
-- Matérialisé en VIEW : pas de stockage supplémentaire, lecture directe.

SELECT
    V:"symbole"::VARCHAR            AS symbole,
    V:"prix_actuel"::FLOAT          AS prix_actuel,
    V:"prix_ouverture"::FLOAT       AS prix_ouverture,
    V:"prix_haut"::FLOAT            AS prix_haut,
    V:"prix_bas"::FLOAT             AS prix_bas,
    V:"cloture_precedente"::FLOAT   AS cloture_precedente,
    V:"volume"::INTEGER             AS volume,
    -- conversion du timestamp unix en timestamp Snowflake
    TO_TIMESTAMP(V:"fetch_timestamp"::INTEGER) AS fetch_timestamp

FROM {{ source('raw', 'BRONZE_CAC40_QUOTES_RAW') }}