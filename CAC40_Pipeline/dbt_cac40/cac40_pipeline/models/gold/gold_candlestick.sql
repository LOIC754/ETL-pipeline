-- Modèle Gold Candlestick : OHLC journalier par symbole.
-- Open/High/Low/Close agrégés par jour pour le graphique en chandeliers.

SELECT
    symbole,
    trade_date,

    -- prix d'ouverture = premier prix de la journée
    FIRST_VALUE(prix_actuel) OVER (
        PARTITION BY symbole, trade_date
        ORDER BY fetch_timestamp ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                              AS candle_open,

    -- prix de clôture = dernier prix de la journée
    LAST_VALUE(prix_actuel) OVER (
        PARTITION BY symbole, trade_date
        ORDER BY fetch_timestamp ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )                              AS candle_close,

    MAX(prix_haut)   OVER (PARTITION BY symbole, trade_date) AS candle_high,
    MIN(prix_bas)    OVER (PARTITION BY symbole, trade_date) AS candle_low,
    SUM(volume)      OVER (PARTITION BY symbole, trade_date) AS volume_jour

FROM {{ ref('silver_cac40_quotes') }}