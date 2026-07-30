# data-engineering-portfolio

Pipeline data engineering "walking skeleton" sur les données open data Vélib' Métropole (GBFS), avec une stack qui monte en puissance milestone après milestone : dlt → DuckDB (local) → dbt Core → Airflow, bascule BigQuery prévue plus tard.

## M1 — Ingestion GBFS Vélib' → DuckDB

Récupère les données GBFS (sans clé API) et les charge dans `data/velib.duckdb` :

- `station_information` (photo courante des stations : id, nom, lat/lon, capacité) — rechargée entièrement à chaque run.
- `station_status` (disponibilité temps réel : vélos/docks disponibles, statut) — accumulée à chaque run avec un timestamp d'ingestion, pour construire un historique.

Les URLs des flux sont déduites dynamiquement du document de découverte GBFS (`gbfs.json`), pas codées en dur.

Lancer l'ingestion :

```bash
uv run python ingestion/velib_gbfs.py
```

Chaque run affiche le nombre de stations, le total de vélos disponibles pour ce run, et le nombre de lignes cumulées dans l'historique `station_status`. Relancer le script plusieurs fois (ex. toutes les quelques minutes) fait grossir l'historique sans écraser les stations.

## M2 — Source météo (Open-Meteo)

Deuxième source du projet, pour pouvoir à terme croiser météo et disponibilité des vélos ("la météo influence-t-elle l'usage du Vélib' ?"). Récupère les prévisions horaires (température, vent, précipitations) pour Paris via l'API Open-Meteo (sans clé), et les charge dans le même fichier `data/velib.duckdb`, dans un dataset dédié `meteo_raw`.

Lancer l'ingestion :

```bash
uv run python ingestion/meteo.py
```

**Choix de design (et pourquoi) :**

- **Prévision (`/v1/forecast`), assumée comme telle — pas de la météo constatée.** Open-Meteo confirme dans sa doc que même `past_days` et `current` restent des sorties de modèle, jamais une mesure réelle (station météo, satellite). L'alternative sérieuse était l'API Historical Weather (`/v1/archive`, réanalyse ERA5/ERA5-Land), qui consolide de vraies observations mais avec ~5 jours de délai — inutilisable pour corréler avec des snapshots Vélib' récents. Décision retenue : garder la prévision, parce que l'objectif analytique du projet ("la météo influence-t-elle l'usage du Vélib'") s'intéresse autant à la météo que les gens **anticipent** avant de sortir qu'à celle réellement constatée — une prévision consultée est un signal comportemental légitime, pas seulement un proxy imparfait de la réalité. Limite assumée : ce n'est pas une source de vérité météorologique, à ne pas confondre avec une étude climatique.
- **`write_disposition="append"` + colonne `ingested_at`** (et non `merge`). Une même heure est une sortie de modèle **recalculée** à chaque appel (donc potentiellement différente d'un run à l'autre) — `merge` écraserait silencieusement une prévision par la suivante, perdant toute trace de son évolution. `append` avec un timestamp d'ingestion (même pattern que `station_status`) construit à la place un historique des prévisions successives pour une même heure — on peut reconstituer "ce que le modèle prédisait pour 14h, tel que capté le 29/07" vs "tel que capté le 30/07". Pas de `primary_key` : plusieurs lignes légitimes partagent désormais le même `time`.
- **Ingestion brute, alignement plus tard.** Le rapprochement horaire avec `station_status` (arrondi à l'heure via `date_trunc`) se fait en aval (SQL exploratoire aujourd'hui, dbt en Phase 3) — cohérent avec une logique ELT : la donnée brute reste la source de vérité, jamais retapée à l'API pour corriger un alignement.
- **Dataset séparé (`meteo_raw` vs `velib_raw`)**, un fichier par source (`meteo.py` vs `velib_gbfs.py`) — même principe de séparation par fonctionnalité. Un dataset distinct n'empêche pas les jointures : `velib_raw.station_status` et `meteo_raw.weather` se croisent normalement en qualifiant les tables.
- **Transformation des tableaux parallèles.** L'API Open-Meteo renvoie ses variables horaires en tableaux parallèles (`hourly.time`, `hourly.temperature_2m`, ...), pas en liste d'objets — reconstruits en une liste de dicts (une ligne par heure) via `zip()`.

**Limite connue et mesurée (couverture temporelle avec `station_status`)** : l'appel Open-Meteo ne demande aucun `past_days` — la fenêtre météo est une fenêtre **glissante** (aujourd'hui + 7 jours), alors que l'historique Vélib' s'accumule et vieillit. Une jointure par existence de correspondance (`EXISTS`, pour éviter la multiplication de lignes maintenant que plusieurs prévisions partagent une même heure) ne trouve une météo que pour les tout derniers snapshots Vélib' : 1518 correspondances sur 21252 lignes `station_status` au moment du test (~7%) — un chiffre qui varie à chaque run et ne fera que se dégrader si les deux pipelines ne tournent pas à la même fréquence. À corriger en Phase 3 : ajouter `past_days` à l'ingestion météo (approche hybride forecast + historique), et utiliser un `LEFT JOIN` depuis `station_status` plutôt qu'un `INNER JOIN` pour ne jamais perdre de lignes Vélib' silencieusement.