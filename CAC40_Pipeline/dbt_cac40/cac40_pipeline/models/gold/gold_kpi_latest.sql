-- Modèle Gold KPI : derniers prix connus par symbole.
-- Une ligne par symbole = vue synthétique pour les cartes KPI du dashboard.

WITH latest AS (
    SELECT
        symbole,
        prix_actuel,
        variation_absolue,
        variation_pct,
        prix_haut,
        prix_bas,
        volume,
        fetch_timestamp,
        -- numérotation par symbole, plus récent en premier
        ROW_NUMBER() OVER (
            PARTITION BY symbole
            ORDER BY fetch_timestamp DESC
        ) AS rn
    FROM {{ ref('silver_cac40_quotes') }}
)

SELECT
    symbole,
    prix_actuel,
    variation_absolue,
    variation_pct,
    prix_haut,
    prix_bas,
    volume,
    fetch_timestamp
FROM latest
WHERE rn = 1