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

**Couverture temporelle — corrigée et mesurée.** Sans `past_days`, la fenêtre météo était une fenêtre glissante (aujourd'hui + 7 jours) alors que l'historique Vélib' s'accumule et vieillit : seulement ~7% des lignes `station_status` trouvaient une météo correspondante (1518/21252). Fix : `past_days=4` sur l'appel Open-Meteo — cohérent avec la décision ci-dessus (on reste sur de la donnée modèle/prévisionnelle, juste sur une fenêtre plus large en arrière, pas un compromis vers de l'observation réelle). Résultat mesuré après le fix : **100% de couverture** (22770/22770 lignes `station_status` trouvent une météo à l'heure correspondante, via une jointure par existence `EXISTS`).

## Phase 3 — Transformation avec dbt

dbt transforme la donnée brute (chargée par `dlt`) **dans** DuckDB — aucun déplacement de données, juste du SQL exécuté sur place et matérialisé en vue/table (ELT, par opposition à l'ETL classique). Choix de version : `dbt-core` (ligne classique 1.x) + `dbt-duckdb`, pas la nouvelle génération Fusion/dbt 2.0 (alpha/bêta mi-2026, support DuckDB encore bêta) — priorité à la stabilité en dev local.

Configuration : `dbt/dbt_project.yml` (versionné) + `~/.dbt/profiles.yml` (hors repo, pointe vers `data/velib.duckdb`).

Lancer :

```bash
uv run dbt run --project-dir dbt      # construit tous les modèles
uv run dbt test --project-dir dbt     # exécute les tests de données
uv run dbt docs generate --project-dir dbt && uv run dbt docs serve --project-dir dbt  # DAG visuel
```

**Architecture en couches :**
- **staging** (`stg_*`, vues) : un modèle par table source, renommage léger, colonnes explicites. `stg_station_status` calcule `snapshot_hour` (`date_trunc('hour', ingested_at)`), nécessaire à toute jointure horaire en aval.
- **intermediate** (`int_station_variation`) : la variation de vélos par station entre deux snapshots (`LAG`, logique réutilisée d'une session SQL antérieure), avec `ingested_at` en second critère de tri — nécessaire car plusieurs ingestions peuvent tomber dans la même heure arrondie (égalités réelles mesurées sur les données).
- **marts** (tables) :
  - `mart_correlation_meteo_usage` : le mart phare. `LEFT JOIN` depuis `stg_station_status` (jamais `INNER`, pour ne pas perdre de lignes Vélib' silencieusement). La météo est dédupliquée à 1 ligne/heure avant la jointure (`ROW_NUMBER` + `QUALIFY`, la prévision la plus ancienne captée pour cette heure — cohérente avec la décision "les gens réagissent à la prévision vue avant de sortir"). Clé de substitution `station_snapshot_id` basée sur `ingested_at` (pas `snapshot_hour` : le vrai grain de ce mart est "une station, un run d'ingestion précis", découvert via un test d'unicité qui échouait sur la clé composite naïve).
  - `mart_disponibilite_station` : agrégats simples (moyennes/max de vélos et docks disponibles) par station et par heure.

**Tests dbt** (nature différente des tests `pytest` : ceux-ci vérifient la **donnée** dans l'entrepôt, pas le code) :
- Génériques (YAML) : `not_null` + `unique` sur `station_snapshot_id`, `relationships` (intégrité référentielle) entre `mart_disponibilite_station.station_id` et `stg_station_information.station_id`.
- Singulier (`tests/assert_bikes_available_coherent.sql`) : `num_bikes_available` ne doit être ni négatif ni supérieur à `capacity`. Résultat réel : **86 violations**, uniquement des dépassements de capacité (jamais de négatif) — phénomène réel des systèmes de vélos en libre-service (rééquilibrage, usagers qui reposent un vélo sur une station "pleine"), pas un bug de pipeline. Configuré en `severity='warn'` : informatif, non bloquant, mais documenté comme cas réel du terrain.

## Tests

```bash
uv add --dev pytest    # déjà fait, dépendance de dev uniquement
uv run pytest -v
```

`pytest` (dépendance de dev, jamais nécessaire en production) découvre automatiquement les fichiers `tests/test_*.py`. Le chemin `ingestion/` est ajouté au `pythonpath` via `[tool.pytest.ini_options]` dans `pyproject.toml`, pour que les tests puissent importer les modules d'ingestion directement (mêmes imports "voisins" qu'entre `meteo.py` et `http_util.py`).

Couverture actuelle (`tests/test_meteo.py`) :

- **Logique pure** (`parse_hourly_weather_data`) : reconstruction des tableaux parallèles Open-Meteo en liste de dicts, sur des données factices — aucun appel réseau.
- **Cas limite** : `zip()` tronque silencieusement à la plus courte liste si les tableaux parallèles ont des longueurs différentes (comportement réel de Python, pas une supposition) — documenté par un test dédié.
- **Mock de `fetch_json`** (fixture `monkeypatch`) : teste `get_weather()` (la resource `dlt` complète, avec l'ajout de `ingested_at`) sans jamais appeler Open-Meteo — rapide, déterministe, fonctionne hors-ligne. Ce test aurait détecté le bug corrigé en Partie 1 (`ingested_at` mal assigné) ; le test sur la logique pure seule ne l'aurait pas fait, puisqu'il ne couvre pas `get_weather`.

Volontairement pas de test d'intégration (vraie base DuckDB, vrai appel réseau) à ce stade : la logique pure et le mock couvrent l'essentiel du risque, pour un coût de maintenance bien plus faible.