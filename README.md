# TP4 — De Cassandra à Spark : analyse de relevés météo

**M2 Big Data & IA — Données distribuées**
**Technologies : Cassandra · PySpark · Jupyter · Spark UI**

## Sujet

Je travaille sur des relevés horaires de température à Paris. Ils sont stockés dans Cassandra et je les exploite avec PySpark : lecture, transformations, agrégations, puis observation du fonctionnement interne de Spark (partitions, Lazy Evaluation, Spark UI).

## Données

- **Source** : API Open-Meteo, chargée dans Cassandra lors du TP précédent.
- **Contenu** : relevés horaires de la température de l'air à 2 m pour Paris.

| Colonne | Type | Rôle |
|---|---|---|
| `ville_id` | text | ville concernée (clé de partition) |
| `observed_at` | timestamp | heure du relevé (clé de clustering) |
| `temperature_2m` | double | température en °C, valeur analysée |

## Keyspace et tables

- **Keyspace** : `meteo`
- **Tables** : `releves_temperature` (table analysée) et `villes` (`ville_id`, `nom`, `latitude`, `longitude`)

## Question métier

> Combien de relevés appartiennent à chaque niveau de température (Froid, Doux, Chaud), et quelles sont la moyenne, le minimum et le maximum de chaque niveau ?

## Chaîne de traitement

```text
Lecture Cassandra (cassandra-driver)
      ↓
DataFrame Spark (createDataFrame)
      ↓
Sélection (ville_id, observed_at, temperature_2m)
      ↓
Filtre (température > 15 °C, ville = paris)
      ↓
Colonnes calculées (ecart_moyenne, niveau)
      ↓
Agrégation (groupBy niveau : count, avg, min, max)
      ↓
Action (show, count)
      ↓
Spark UI / statusTracker
```

## Traitements réalisés

- **Connexion** : session Cassandra via `cassandra-driver`, SparkSession en `local[4]` avec `spark.sql.shuffle.partitions = 4`.
- **DataFrame** : les lignes lues depuis Cassandra sont converties avec `spark.createDataFrame`, puis je vérifie le schéma et le nombre de lignes.
- **Sélection** : je ne garde que les colonnes utiles.
- **Filtres métier** : relevés au-dessus de 15 °C, et relevés d'une ville donnée.
- **Colonnes calculées** :
  - `ecart_moyenne` : température du relevé moins la moyenne de la période (positif = plus chaud que la moyenne) ;
  - `niveau` : Froid (< 12 °C), Doux (12 à 16 °C), Chaud (> 16 °C).
- **Agrégation** : `groupBy("niveau")` avec `count`, `avg`, `min` et `max`, triée par nombre de relevés.
- **Partitions** : 4 partitions au départ, puis `repartition(2)` et `repartition(8)`.
- **Lazy Evaluation** : j'enchaîne `filter`, `select` et `orderBy` sans aucun calcul, puis `show()` déclenche l'exécution.

## Principales observations

- **Lecture Cassandra** : `session.execute(...)` s'exécute immédiatement côté Python. Spark ne devient paresseux qu'à partir de `createDataFrame`.
- **Transformations / actions** : `select`, `filter`, `withColumn`, `groupBy` et `agg` ne font que construire un plan. `show()` et `count()` lancent le calcul.
- **Jobs, Stages, Tasks** : le job de l'agrégation compte 2 stages (4 tasks, puis 1 task), alors qu'un job sans agrégation n'a qu'un stage de 4 tasks.
- **Shuffle** : le `groupBy` redistribue les données entre partitions. Le plan montre un `Exchange`, et la Spark UI doit afficher Shuffle Write puis Shuffle Read.
- **Spark UI** : l'interface ne répondait pas correctement dans mon environnement. J'ai complété avec `sc.statusTracker()` pour récupérer les jobs, stages et tasks.

## Structure du dépôt

```text
TP3-Cassandra-Spark/
│
├── README.md
├── notebook/
│   └── tp4_cassandra_spark.ipynb
```

## Captures

Les résultats sont dans le notebook, vu que spark UI ne réponds pas aucun intérêt de screen