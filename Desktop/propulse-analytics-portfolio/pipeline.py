"""
pipeline.py — Propulse Analytics DuckDB Pipeline
Medallion architecture: Bronze → Silver → Gold

Run:
    python pipeline.py
"""

import duckdb
from datetime import datetime, timezone
from pathlib import Path

# ─────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────
ROOT = Path(__file__).parent
BRONZE_DIR = ROOT / "data" / "bronze"
SILVER_DIR = ROOT / "data" / "silver"
GOLD_DIR   = ROOT / "data" / "gold"

SILVER_DIR.mkdir(parents=True, exist_ok=True)
GOLD_DIR.mkdir(parents=True, exist_ok=True)

# In-memory DuckDB (all transformations happen here)
con = duckdb.connect()

INGESTED_AT = datetime.now(timezone.utc).isoformat()


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 1 — BRONZE
# Objectif : ingérer les données brutes sans aucune modification.
# On ne corrige rien ici — la couche bronze est le "source of truth" auditable.
# Ajouter _ingested_at permet de retracer l'origine temporelle de chaque lot
# d'ingestion, essentiel en cas de litige ou de bug de données en production.
# ══════════════════════════════════════════════════════════════════════════════

print("=" * 60)
print("LAYER 1 — BRONZE  (ingestion brute)")
print("=" * 60)

# Chaque CSV est lu tel quel — DuckDB infère les types mais on ne les force pas.
# La colonne _ingested_at trace chaque run du pipeline : si une anomalie
# apparaît, on sait exactement quel lot est concerné.

bronze_tables = {
    "subscribers":   str(BRONZE_DIR / "subscribers_raw.csv"),
    "email_events":  str(BRONZE_DIR / "email_events_raw.csv"),
    "transactions":  str(BRONZE_DIR / "transactions_raw.csv"),
}

for name, path in bronze_tables.items():
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze_{name} AS
        SELECT
            *,
            '{INGESTED_AT}' AS _ingested_at
        FROM read_csv_auto('{path}', all_varchar=true)
    """)
    rows = con.execute(f"SELECT COUNT(*) FROM bronze_{name}").fetchone()[0]
    print(f"  bronze_{name:<20} {rows:>8,} lignes  ← {Path(path).name}")

# Sauvegarde Parquet bronze (données brutes + timestamp)
con.execute(f"COPY bronze_subscribers  TO '{BRONZE_DIR}/subscribers_raw.parquet'  (FORMAT PARQUET)")
con.execute(f"COPY bronze_email_events TO '{BRONZE_DIR}/email_events_raw.parquet' (FORMAT PARQUET)")
con.execute(f"COPY bronze_transactions TO '{BRONZE_DIR}/transactions_raw.parquet' (FORMAT PARQUET)")
print()


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 2 — SILVER
# Objectif : données propres, typées, enrichies de colonnes métier.
# La silver est la couche que les analystes et les dashboards consomment
# au quotidien. Chaque règle de nettoyage ici correspond à une définition
# métier précise — pas juste une convention technique.
# ══════════════════════════════════════════════════════════════════════════════

print("=" * 60)
print("LAYER 2 — SILVER  (nettoyage + enrichissement métier)")
print("=" * 60)

# ─────────────────────────────────────────────
# 2A — SILVER SUBSCRIBERS
#
# POURQUOI le segment ?
#   Les partenaires abonnement pilotent leur revenue à trois niveaux de valeur :
#   - premium (≥ 30 €/mois) : LTV élevée, priorité support, rétention critique
#   - standard (≥ 15 €/mois) : cœur de marché, upsell potentiel
#   - basic (< 15 €/mois)    : volume + conversion ; coût d'acquisition à surveiller
#   Sans ce segment, tout analyse de churn ou de MRR est aveugle à la valeur réelle.
#
# POURQUOI is_converted ?
#   Un "lead" non converti pollue les KPIs de rétention si on le compte
#   comme abonné. Séparer les convertis des leads permet de calculer
#   un taux de churn et un MRR purement sur la base payante.
# ─────────────────────────────────────────────

con.execute("""
    CREATE OR REPLACE TABLE silver_subscribers AS
    SELECT
        subscriber_id,
        partner_id,
        partner_name,
        acquisition_channel,

        -- Typage strict des dates : sans ça, les calculs de durée (LTV, churn delay)
        -- retournent des résultats incohérents selon la locale du serveur.
        TRY_CAST(acquisition_date  AS DATE) AS acquisition_date,
        TRY_CAST(conversion_date   AS DATE) AS conversion_date,
        TRY_CAST(churn_date        AS DATE) AS churn_date,

        plan_type,
        TRY_CAST(monthly_amount AS DOUBLE) AS monthly_amount,
        status,
        churn_reason,
        country,

        -- Segment de valeur : base des analyses de rétention par tier.
        -- Les partenaires SaaS abonnement segmentent systématiquement leur base
        -- pour cibler les campagnes de sauvegarde (ex. : offrir un downgrade
        -- plutôt qu'une résiliation sur le segment premium).
        CASE
            WHEN TRY_CAST(monthly_amount AS DOUBLE) >= 30 THEN 'premium'
            WHEN TRY_CAST(monthly_amount AS DOUBLE) >= 15 THEN 'standard'
            ELSE 'basic'
        END AS segment,

        -- is_converted : distingue les abonnés payants des simples leads.
        -- Sans cette séparation, le churn rate inclut des non-payants
        -- et sous-estime le problème réel de rétention.
        (conversion_date IS NOT NULL AND conversion_date != '') AS is_converted,

        _ingested_at
    FROM bronze_subscribers
""")

rows = con.execute("SELECT COUNT(*) FROM silver_subscribers").fetchone()[0]
print(f"  silver_subscribers       {rows:>8,} lignes")
print(f"    segments : {con.execute('SELECT segment, COUNT(*) FROM silver_subscribers GROUP BY segment ORDER BY segment').fetchall()}")


# ─────────────────────────────────────────────
# 2B — SILVER EMAIL EVENTS
#
# POURQUOI l'engagement_score ?
#   Le simple taux d'ouverture est trompeur (pixels bloqués, prévisualisation).
#   Un score composite opened=1 / clicked=2 pondère l'intention réelle :
#   un clic prouve l'engagement, une ouverture le suggère seulement.
#   Ce score alimente la corrélation email ↔ churn en couche gold :
#   les abonnés avec engagement_score faible churnen-ils plus vite ?
#   C'est exactement ce qu'un partenaire veut savoir avant de couper
#   son budget emailing ou de retravailler ses séquences de nurturing.
# ─────────────────────────────────────────────

con.execute("""
    CREATE OR REPLACE TABLE silver_email_events AS
    SELECT
        event_id,
        subscriber_id,
        partner_id,
        TRY_CAST(sent_date AS DATE) AS sent_date,
        email_type,

        -- Typage des booléens depuis les chaînes 'True'/'False' du CSV source.
        -- DuckDB ne reconnaît pas automatiquement ces valeurs Python-style.
        (LOWER(opened)       = 'true') AS opened,
        (LOWER(clicked)      = 'true') AS clicked,
        (LOWER(unsubscribed) = 'true') AS unsubscribed,

        -- Score d'engagement : clic = 2 pts (action forte), ouverture = 1 pt,
        -- ignoré = 0. Agrégé par abonné, cela donne un signal prédictif du churn
        -- bien plus fiable qu'un simple comptage d'emails envoyés.
        CASE
            WHEN LOWER(clicked)= 'true' THEN 2
            WHEN LOWER(opened) = 'true' THEN 1
            ELSE 0
        END AS engagement_score,

        _ingested_at
    FROM bronze_email_events
""")

rows = con.execute("SELECT COUNT(*) FROM silver_email_events").fetchone()[0]
avg_score = con.execute("SELECT ROUND(AVG(engagement_score),3) FROM silver_email_events").fetchone()[0]
print(f"  silver_email_events      {rows:>8,} lignes  (avg engagement_score = {avg_score})")


# ─────────────────────────────────────────────
# 2C — SILVER TRANSACTIONS
#
# POURQUOI is_failed et revenue séparés ?
#   Compter un paiement échoué comme du chiffre d'affaires est l'erreur
#   comptable la plus fréquente dans les plateformes abonnement.
#   revenue = 0 sur les transactions failed garantit que le MRR en gold
#   reflète l'argent réellement encaissé — pas les tentatives de débit.
#   is_failed est un signal opérationnel critique : un taux d'échec
#   supérieur à 5 % sur un partenaire indique souvent un problème
#   de moyens de paiement ou un pic de cartes expirées.
# ─────────────────────────────────────────────

con.execute("""
    CREATE OR REPLACE TABLE silver_transactions AS
    SELECT
        transaction_id,
        subscriber_id,
        partner_id,
        TRY_CAST(transaction_date AS DATE) AS transaction_date,

        -- Montant en DOUBLE : les calculs de MRR et de LTV exigent
        -- de la précision numérique, pas des chaînes de caractères.
        TRY_CAST(amount AS DOUBLE) AS amount,

        status,
        payment_method,
        plan_type,
        (LOWER(is_retry) = 'true') AS is_retry,

        -- is_failed : flag opérationnel pour monitorer les incidents de paiement
        -- et les distinguer des annulations volontaires (churn).
        (LOWER(status) = 'failed') AS is_failed,

        -- revenue : chiffre d'affaires réellement encaissé.
        -- Toujours utiliser cette colonne pour le MRR — jamais `amount` brut.
        CASE WHEN LOWER(status) = 'failed' THEN 0.0
             ELSE TRY_CAST(amount AS DOUBLE)
        END AS revenue,

        _ingested_at
    FROM bronze_transactions
""")

rows       = con.execute("SELECT COUNT(*) FROM silver_transactions").fetchone()[0]
fail_rate  = con.execute("SELECT ROUND(100.0 * SUM(is_failed::INT) / COUNT(*), 2) FROM silver_transactions").fetchone()[0]
print(f"  silver_transactions      {rows:>8,} lignes  (taux échec paiement = {fail_rate} %)")
print()

# ── Exports Parquet silver ──
con.execute(f"COPY silver_subscribers  TO '{SILVER_DIR}/subscribers.parquet'  (FORMAT PARQUET)")
con.execute(f"COPY silver_email_events TO '{SILVER_DIR}/email_events.parquet' (FORMAT PARQUET)")
con.execute(f"COPY silver_transactions TO '{SILVER_DIR}/transactions.parquet' (FORMAT PARQUET)")
print(f"  ✓ Parquet silver exportés → {SILVER_DIR}/\n")


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 3 — GOLD
# Objectif : tables analytiques prêtes pour dashboards et rapports exécutifs.
# La couche gold répond à des questions business précises — pas à des besoins
# génériques. Chaque table est conçue autour d'un KPI stratégique.
# ══════════════════════════════════════════════════════════════════════════════

print("=" * 60)
print("LAYER 3 — GOLD  (tables analytiques)")
print("=" * 60)

# ─────────────────────────────────────────────
# 3A — GOLD MRR MONTHLY
#
# POURQUOI le MRR par mois ET par partenaire ?
#   Le MRR (Monthly Recurring Revenue) est LE KPI primaire des business
#   abonnement. Décomposé par partenaire, il permet de détecter :
#   - un partenaire dont le MRR stagne alors que les autres croissent
#   - un mois de churn anormalement élevé (saisonnier ou incident produit)
#   - la contribution réelle de chaque partenaire au revenue global
#   Utiliser DATE_TRUNC sur transaction_date (et non sur billing_date théorique)
#   ancre le MRR sur l'argent réellement reçu — c'est la définition comptable.
# ─────────────────────────────────────────────

con.execute("""
    CREATE OR REPLACE TABLE gold_mrr_monthly AS
    SELECT
        DATE_TRUNC('month', transaction_date)  AS month,
        partner_id,

        -- On joint partner_name depuis subscribers pour lisibilité des dashboards.
        -- Une clé partner_id seule force les analystes à faire des lookups manuels.
        MAX(s.partner_name) AS partner_name,

        COUNT(DISTINCT t.subscriber_id)        AS active_subscribers,
        SUM(t.revenue)                         AS mrr,
        ROUND(SUM(t.revenue) / NULLIF(COUNT(DISTINCT t.subscriber_id), 0), 2) AS arpu

    FROM silver_transactions t
    LEFT JOIN (
        SELECT DISTINCT subscriber_id, partner_name
        FROM silver_subscribers
    ) s USING (subscriber_id)

    -- Exclure les mois sans revenue réel : ils biaisent les tendances MRR
    -- si on les laisse à 0 dans une visualisation temporelle.
    WHERE t.revenue > 0
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

rows    = con.execute("SELECT COUNT(*) FROM gold_mrr_monthly").fetchone()[0]
total_mrr = con.execute("SELECT ROUND(SUM(mrr), 2) FROM gold_mrr_monthly").fetchone()[0]
print(f"  gold_mrr_monthly         {rows:>8,} lignes  (MRR cumulé = {total_mrr:,.2f} €)")


# ─────────────────────────────────────────────
# 3B — GOLD CHURN BY SEGMENT
#
# POURQUOI churn par segment ET par plan ?
#   Le churn global est une métrique trompeuse : un churn de 5 % sur
#   les clients premium représente 3× plus de revenue perdu que
#   5 % sur les clients basic. Décomposer par segment révèle où
#   concentrer les efforts de rétention et de succès client.
#   Par plan (monthly/quarterly/annual), on identifie si les abonnés
#   annuels churnen après renouvellement — signal d'une promesse produit
#   non tenue sur la durée.
# ─────────────────────────────────────────────

con.execute("""
    CREATE OR REPLACE TABLE gold_churn_by_segment AS
    SELECT
        segment,
        plan_type,

        COUNT(*)                                                       AS total_subscribers,
        SUM(CASE WHEN status = 'churned' THEN 1 ELSE 0 END)           AS churned_count,

        -- Churn rate sur la base convertie uniquement : un lead qui ne convertit
        -- jamais ne "churn" pas — il n'a jamais été client.
        ROUND(
            100.0 * SUM(CASE WHEN status = 'churned' THEN 1 ELSE 0 END)
            / NULLIF(SUM(CASE WHEN is_converted THEN 1 ELSE 0 END), 0),
        2) AS churn_rate_pct,

        ROUND(AVG(monthly_amount), 2) AS avg_monthly_amount

    FROM silver_subscribers
    WHERE is_converted   -- exclure les leads non convertis du calcul de churn
    GROUP BY 1, 2
    ORDER BY churn_rate_pct DESC NULLS LAST
""")

rows = con.execute("SELECT COUNT(*) FROM gold_churn_by_segment").fetchone()[0]
global_churn = con.execute("""
    SELECT ROUND(100.0 * SUM(churned_count) / NULLIF(SUM(total_subscribers), 0), 2)
    FROM gold_churn_by_segment
""").fetchone()[0]
print(f"  gold_churn_by_segment    {rows:>8,} lignes  (churn rate global = {global_churn} %)")


# ─────────────────────────────────────────────
# 3C — GOLD LTV BY CHANNEL
#
# POURQUOI la LTV par canal d'acquisition ?
#   Le CAC (Coût d'Acquisition Client) varie fortement selon le canal,
#   mais c'est la LTV qui détermine si le canal est rentable.
#   Un canal paid_social peut sembler cher au CPC tout en générant
#   les LTV les plus élevées (abonnés plus engagés, moindre churn).
#   Cette table permet aux partenaires d'arbitrer leur budget marketing
#   sur des données réelles plutôt que sur des hypothèses de CAC seul.
#   LTV = revenue total encaissé par abonné sur toute sa durée de vie.
# ─────────────────────────────────────────────

con.execute("""
    CREATE OR REPLACE TABLE gold_ltv_by_channel AS

    WITH subscriber_ltv AS (
        -- LTV individuelle = somme des paiements réels de l'abonné
        -- (revenue exclut déjà les transactions échouées)
        SELECT
            t.subscriber_id,
            SUM(t.revenue) AS lifetime_revenue
        FROM silver_transactions t
        GROUP BY 1
    )

    SELECT
        s.acquisition_channel,
        COUNT(DISTINCT s.subscriber_id)             AS subscribers,
        ROUND(AVG(sl.lifetime_revenue), 2)          AS avg_ltv,
        ROUND(MEDIAN(sl.lifetime_revenue), 2)       AS median_ltv,

        -- La LTV max signale les outliers (comptes entreprise, partenariats) :
        -- utile pour décider si le canal attire ponctuellement de gros comptes
        -- ou s'il génère structurellement des abonnés à haute valeur.
        ROUND(MAX(sl.lifetime_revenue), 2)          AS max_ltv,

        ROUND(MIN(sl.lifetime_revenue), 2)          AS min_ltv

    FROM silver_subscribers s
    INNER JOIN subscriber_ltv sl USING (subscriber_id)
    WHERE s.is_converted
    GROUP BY 1
    ORDER BY avg_ltv DESC
""")

rows         = con.execute("SELECT COUNT(*) FROM gold_ltv_by_channel").fetchone()[0]
best_channel = con.execute("SELECT acquisition_channel, avg_ltv FROM gold_ltv_by_channel ORDER BY avg_ltv DESC LIMIT 1").fetchone()
print(f"  gold_ltv_by_channel      {rows:>8,} lignes  (meilleur canal = {best_channel[0]} @ {best_channel[1]:.2f} € LTV)")


# ─────────────────────────────────────────────
# 3D — GOLD EMAIL CHURN CORRELATION
#
# POURQUOI corréler engagement email et churn ?
#   Les équipes marketing demandent souvent si "envoyer plus d'emails réduit
#   le churn". La vraie question est : les abonnés qui ouvrent et cliquent
#   churnen-ils moins ? Si oui, l'emailing est un levier de rétention ;
#   si non, c'est un coût sans ROI de rétention.
#   On utilise des quartiles d'open rate (pas des déciles) pour avoir
#   des groupes suffisamment larges pour être statistiquement significatifs
#   même sur des segments petits (ex. : premium avec 200 abonnés).
#   Cette table alimente directement les recommandations d'investissement
#   emailing dans les rapports partenaires.
# ─────────────────────────────────────────────

con.execute("""
    CREATE OR REPLACE TABLE gold_email_churn_correlation AS

    WITH email_stats AS (
        -- Agrégat email par abonné : open rate et score d'engagement moyen.
        -- On calcule sur l'ensemble de l'historique (pas les 90 derniers jours)
        -- car on veut la corrélation sur la LTV complète, pas sur une fenêtre courte.
        SELECT
            subscriber_id,
            COUNT(*)                              AS emails_received,
            ROUND(AVG(opened::INT), 4)            AS open_rate,
            ROUND(AVG(clicked::INT), 4)           AS click_rate,
            ROUND(AVG(engagement_score), 4)       AS avg_engagement_score
        FROM silver_email_events
        GROUP BY 1
    ),

    subscriber_enriched AS (
        SELECT
            s.subscriber_id,
            s.status,
            s.segment,
            s.plan_type,
            e.open_rate,
            e.click_rate,
            e.avg_engagement_score,
            e.emails_received
        FROM silver_subscribers s
        INNER JOIN email_stats e USING (subscriber_id)
        WHERE s.is_converted  -- leads exclus : ils n'ont pas de comportement de churn mesurable
    ),

    -- Quartile matérialisé dans une CTE séparée : DuckDB interdit les window
    -- functions directement dans GROUP BY — on les calcule ici d'abord.
    with_quartile AS (
        SELECT
            *,
            NTILE(4) OVER (ORDER BY open_rate) AS open_rate_quartile
        FROM subscriber_enriched
    )

    SELECT
        open_rate_quartile,

        CASE open_rate_quartile
            WHEN 1 THEN 'Q1 - Très faible engagement (0–25 %)'
            WHEN 2 THEN 'Q2 - Faible engagement (25–50 %)'
            WHEN 3 THEN 'Q3 - Bon engagement (50–75 %)'
            WHEN 4 THEN 'Q4 - Fort engagement (75–100 %)'
        END AS engagement_tier,

        COUNT(*)                                                      AS subscribers,
        ROUND(AVG(open_rate) * 100, 2)                               AS avg_open_rate_pct,
        ROUND(AVG(click_rate) * 100, 2)                              AS avg_click_rate_pct,
        ROUND(AVG(avg_engagement_score), 3)                          AS avg_engagement_score,
        ROUND(AVG(emails_received), 1)                               AS avg_emails_received,

        SUM(CASE WHEN status = 'churned' THEN 1 ELSE 0 END)          AS churned_count,

        -- Churn rate par quartile : c'est ici que la corrélation apparaît.
        -- Un churn rate décroissant de Q1 vers Q4 confirme que l'engagement
        -- email est un prédicteur du churn — et justifie d'investir dans
        -- des séquences de re-engagement ciblant les abonnés Q1 et Q2.
        ROUND(
            100.0 * SUM(CASE WHEN status = 'churned' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
        2) AS churn_rate_pct

    FROM with_quartile
    GROUP BY open_rate_quartile
    ORDER BY open_rate_quartile
""")

rows = con.execute("SELECT COUNT(*) FROM gold_email_churn_correlation").fetchone()[0]
print(f"  gold_email_churn_correlation {rows:>5,} lignes  (corrélation email ↔ churn par quartile)")
print()

# ── Exports Parquet gold ──
con.execute(f"COPY gold_mrr_monthly             TO '{GOLD_DIR}/mrr_monthly.parquet'             (FORMAT PARQUET)")
con.execute(f"COPY gold_churn_by_segment        TO '{GOLD_DIR}/churn_by_segment.parquet'        (FORMAT PARQUET)")
con.execute(f"COPY gold_ltv_by_channel          TO '{GOLD_DIR}/ltv_by_channel.parquet'          (FORMAT PARQUET)")
con.execute(f"COPY gold_email_churn_correlation TO '{GOLD_DIR}/email_churn_correlation.parquet' (FORMAT PARQUET)")
print(f"  ✓ Parquet gold exportés → {GOLD_DIR}/\n")


# ══════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ EXÉCUTIF
# Ce bloc restitue les KPIs clés qu'un partenaire abonnement attend
# en premier regard : MRR total, churn, meilleur canal.
# ══════════════════════════════════════════════════════════════════════════════

print("=" * 60)
print("RÉSUMÉ — KPIs CLÉS")
print("=" * 60)

# MRR total (somme de toutes les transactions réussies sur la période)
total_revenue = con.execute("SELECT ROUND(SUM(mrr), 2) FROM gold_mrr_monthly").fetchone()[0]
latest_month  = con.execute(
    "SELECT month, ROUND(SUM(mrr),2) FROM gold_mrr_monthly "
    "GROUP BY month ORDER BY month DESC LIMIT 1"
).fetchone()

# Churn global
churn_stats = con.execute("""
    SELECT
        SUM(total_subscribers)    AS total,
        SUM(churned_count)        AS churned,
        ROUND(100.0 * SUM(churned_count) / NULLIF(SUM(total_subscribers), 0), 2) AS rate
    FROM gold_churn_by_segment
""").fetchone()

# Meilleur canal LTV
best_ltv = con.execute(
    "SELECT acquisition_channel, avg_ltv, subscribers "
    "FROM gold_ltv_by_channel ORDER BY avg_ltv DESC LIMIT 1"
).fetchone()

# Corrélation email : différence de churn Q1 vs Q4
email_corr = con.execute("""
    SELECT
        MAX(CASE WHEN open_rate_quartile = 1 THEN churn_rate_pct END) AS q1_churn,
        MAX(CASE WHEN open_rate_quartile = 4 THEN churn_rate_pct END) AS q4_churn
    FROM gold_email_churn_correlation
""").fetchone()

# MRR par partenaire (top 3)
top_partners = con.execute("""
    SELECT partner_name, ROUND(SUM(mrr), 2) AS total_mrr
    FROM gold_mrr_monthly
    GROUP BY partner_name
    ORDER BY total_mrr DESC
    LIMIT 3
""").fetchall()

print(f"""
  Revenue encaissé total     {total_revenue:>12,.2f} €
  MRR dernier mois ({latest_month[0].strftime('%b %Y')})   {latest_month[1]:>12,.2f} €

  Base convertie             {churn_stats[0]:>12,} abonnés
  Abonnés churned            {churn_stats[1]:>12,}
  Churn rate global          {churn_stats[2]:>11} %

  Meilleur canal LTV         {best_ltv[0]:>20}  →  avg LTV = {best_ltv[1]:.2f} € ({best_ltv[2]} abonnés)

  Email ↔ Churn :
    Q1 (faible engagement)   churn = {email_corr[0]:>5} %
    Q4 (fort engagement)     churn = {email_corr[1]:>5} %
    Différence               {round(email_corr[0] - email_corr[1], 2):>+5} pts  (positif = engagement protège)

  Top 3 partenaires par MRR :""")

for i, (name, mrr) in enumerate(top_partners, 1):
    print(f"    {i}. {name:<20}  {mrr:>10,.2f} €")

print()
print("  Pipeline terminé — toutes les couches exportées en Parquet.")
print(f"  bronze/ → silver/ → gold/  |  run : {INGESTED_AT[:19]} UTC")
print("=" * 60)
