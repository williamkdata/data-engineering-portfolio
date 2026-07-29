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

- **Ingestion brute, alignement plus tard.** Les snapshots Vélib' tombent à intervalles irréguliers (relances manuelles), la météo Open-Meteo est horaire pile. Plutôt que d'aligner les timestamps dès l'ingestion, on charge la météo horaire telle quelle — cohérent avec une logique ELT : la donnée brute reste la source de vérité, modifiable/réinterprétable sans jamais retaper l'API. Le rapprochement horaire (arrondi à l'heure via `date_trunc`) se fera dans la couche de transformation (dbt, Phase 3).
- **`write_disposition="merge"` avec `primary_key=["time"]`.** Une même heure peut être re-livrée par l'API à chaque run (Open-Meteo renvoie plusieurs jours à chaque appel). `merge` met à jour une heure déjà connue plutôt que de la dupliquer (contrairement à `append`), tout en laissant la table grossir au fil des runs avec les nouvelles heures rencontrées — un historique d'heures distinctes, pas de versions dupliquées d'une même heure.
- **Dataset séparé (`meteo_raw` vs `velib_raw`)**, un fichier par source (`meteo.py` vs `velib_gbfs.py`) — même principe de séparation par fonctionnalité. Un dataset distinct n'empêche pas les jointures : `velib_raw.station_status` et `meteo_raw.weather` se croisent normalement en qualifiant les tables.
- **Transformation des tableaux parallèles.** L'API Open-Meteo renvoie ses variables horaires en tableaux parallèles (`hourly.time`, `hourly.temperature_2m`, ...), pas en liste d'objets — reconstruits en une liste de dicts (une ligne par heure) via `zip()`.

**Limite connue (découverte en testant la jointure avec `station_status`)** : l'appel Open-Meteo actuel ne demande aucun jour passé (`past_days`), donc les snapshots Vélib' antérieurs au premier jour couvert par la météo n'ont pas de correspondance — un `INNER JOIN` entre les deux tables en perd silencieusement une partie (~31% des lignes `station_status` au moment du test). À corriger en Phase 3 : soit ajouter `past_days` à l'ingestion météo pour combler rétroactivement, soit utiliser un `LEFT JOIN` depuis `station_status` plutôt qu'un `INNER JOIN`.