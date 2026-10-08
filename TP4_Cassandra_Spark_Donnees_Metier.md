# TP4 — De Cassandra à Spark : traitement de vos données métier

**M2 Big Data & IA — Données distribuées**  
**Travail individuel — environ 3 h**  
**Technologies : Cassandra · PySpark · Jupyter · Spark UI**

---

## 1. Objectif du TP

Dans les TPs précédents, vous avez conçu votre modèle de données et chargé vos données métier dans Cassandra.

L'objectif de ce TP est maintenant de **faire le lien entre Cassandra et Spark** afin d'exploiter ces données avec PySpark.

```text
Vos données métier
       ↓
   Cassandra
       ↓
      Spark
       ↓
   DataFrame
       ↓
Transformations
       ↓
    Actions
       ↓
 Agrégations
       ↓
Lazy Evaluation
       ↓
   Spark UI
```

Vous devez travailler avec **les données de votre propre projet Cassandra**.

> Si votre table contient peu de données, vous pouvez réutiliser le script d'importation réalisé lors du TP Cassandra afin d'en charger davantage.

---

# 2. Supports à utiliser

Vous disposez de deux fichiers guides :

### Guide 1 — Découverte de Spark

Il vous permet de retrouver les manipulations concernant :

- SparkSession ;
- DataFrame ;
- sélection et filtrage ;
- partitions ;
- transformations et actions ;
- Lazy Evaluation ;
- Jobs, Stages et Tasks ;
- Spark UI.

### Guide 2 — Cassandra → Spark

Il vous permet de retrouver les manipulations concernant :

- la connexion entre Spark et Cassandra ;
- la lecture des données Cassandra ;
- la création d'un DataFrame Spark ;
- les transformations ;
- les agrégations ;
- les partitions ;
- la Lazy Evaluation ;
- le Shuffle ;
- la Spark UI.

**Le TP ne reprend volontairement pas les commandes détaillées.**

Vous devez vous appuyer sur les deux guides pour réaliser les manipulations.

---

# 3. Partie 1 — Connecter Spark à Cassandra

Préparez votre environnement à partir des deux guides.

Votre objectif est d'obtenir une connexion fonctionnelle :

```text
Cassandra
   │
   ▼
Spark / PySpark
```

Vous devez :

1. vérifier que votre cluster Cassandra fonctionne ;
2. vérifier que Spark peut communiquer avec Cassandra ;
3. ouvrir votre notebook ;
4. initialiser votre SparkSession ;
5. établir la connexion avec Cassandra ;
6. sélectionner votre keyspace et votre table métier.

### Résultat attendu

Vous devez être capable d'afficher depuis votre notebook :

- le nom de votre keyspace ;
- le nom de votre table ;
- la structure de votre table ;
- quelques lignes de vos données.

---

# 4. Partie 2 — Créer votre DataFrame Spark

À partir des données récupérées depuis Cassandra, créez un **DataFrame Spark**.

Vous devez ensuite :

- afficher quelques lignes ;
- afficher le schéma ;
- vérifier le nombre de lignes ;
- identifier les principales colonnes utilisées pour vos analyses.

Votre notebook doit clairement montrer le passage :

```text
Cassandra
   ↓
Données récupérées
   ↓
Spark DataFrame
```

### Travail demandé

Ajoutez une courte explication indiquant :

- quelles données vous utilisez ;
- quelles sont les colonnes importantes ;
- quelle information métier vous souhaitez analyser.

---

# 5. Partie 3 — Transformations Spark

Réalisez plusieurs traitements sur votre DataFrame.

## Sélection

Sélectionnez uniquement les colonnes utiles à votre analyse.

## Filtres

Réalisez **au moins deux filtres métier**.

Exemples :

**Transport / Vélib**
- stations ayant moins de 5 vélos disponibles ;
- stations ayant plus de 20 vélos disponibles.

**E-commerce**
- produits dont le stock est inférieur à un seuil ;
- commandes dont le montant dépasse une valeur donnée.

**Finance**
- transactions dépassant un montant ;
- opérations appartenant à une catégorie particulière.

**Météo**
- températures supérieures à un seuil ;
- observations appartenant à une ville donnée.

**Votre sujet reste prioritaire : choisissez des conditions cohérentes avec vos propres données.**

## Colonne calculée

Créez au moins **une nouvelle colonne calculée**.

Exemples :

```text
montant_total = quantité × prix
```

```text
taux_disponibilité = valeur_disponible / capacité
```

```text
écart = valeur_max - valeur_min
```

La nouvelle colonne doit avoir une **signification métier**.

---

# 6. Partie 4 — Actions et agrégations

Réalisez au minimum :

- une action d'affichage ;
- une action de comptage ;
- une agrégation avec `groupBy` ;
- une ou plusieurs fonctions d'agrégation : moyenne, minimum, maximum, somme ou comptage.

### Exemples de questions métier

**Vélib**

> Combien de stations appartiennent à chaque niveau de disponibilité ?

**E-commerce**

> Quel est le stock moyen par catégorie ?

**Finance**

> Quel est le montant moyen des transactions par catégorie ?

**Météo**

> Quelle est la température moyenne par ville ?

Formulez une question métier adaptée à votre propre jeu de données et utilisez Spark pour y répondre.

---

# 7. Partie 5 — Partitions et Lazy Evaluation

À partir des traitements réalisés précédemment, observez maintenant le fonctionnement interne de Spark.

Vous devez :

- observer le nombre de partitions de votre DataFrame ;
- modifier le nombre de partitions ;
- construire plusieurs transformations successives ;
- constater qu'une transformation seule ne déclenche pas immédiatement le calcul ;
- déclencher ensuite le calcul avec une action.

Vous devez être capables d'identifier :

```text
Transformation
       ↓
Transformation
       ↓
Transformation
       ↓
     Action
       ↓
  Exécution Spark
```

### Travail demandé

Expliquez brièvement :

> **Pourquoi Spark utilise-t-il la Lazy Evaluation ?**

---

# 8. Partie 6 — Observer l'exécution avec Spark UI

Utilisez la Spark UI pour observer l'exécution réelle de vos traitements.

À partir d'une action ou d'une agrégation réalisée précédemment, identifiez :

```text
Job
 ↓
Stages
 ↓
Tasks
```

Observez notamment :

- le nombre de Jobs ;
- le nombre de Stages ;
- le nombre de Tasks ;
- la durée d'exécution ;
- les informations relatives aux partitions.

Réalisez également une opération susceptible de provoquer une redistribution des données, par exemple une agrégation ou un tri.

Dans le plan d'exécution, observez la présence éventuelle de :

```text
Exchange
```

Puis recherchez dans la Spark UI :

```text
Shuffle Read
Shuffle Write
```

---

# 9. Partie 7 — Votre analyse métier

À partir de **votre propre table Cassandra**, construisez une chaîne de traitement comprenant au minimum :

```text
Lecture Cassandra
      ↓
DataFrame Spark
      ↓
Sélection
      ↓
Filtre
      ↓
Colonne calculée
      ↓
Agrégation
      ↓
Action
      ↓
Spark UI
```

Votre analyse doit répondre à **une vraie question métier**.

Vous devez être capables de justifier :

- pourquoi vous avez choisi ces données ;
- pourquoi vous avez choisi ces transformations ;
- pourquoi votre agrégation est pertinente ;
- ce qui déclenche réellement l'exécution ;
- ce que vous observez dans Spark UI.

---

# 10. Livrable

Déposez un dépôt Git contenant :

```text
TP3-Cassandra-Spark/
│
├── README.md
├── notebook/
│   └── tp4_cassandra_spark.ipynb
│
└── screenshots/
```

## Le notebook doit contenir

- la connexion à Cassandra ;
- vos données ;
- votre DataFrame ;
- vos transformations ;
- vos actions ;
- vos agrégations ;
- votre démonstration de Lazy Evaluation ;
- votre analyse des partitions ;
- votre observation de Spark UI.

## Captures minimales

Ajoutez des captures montrant :

- vos données dans le DataFrame ;
- les partitions ;
- un plan d'exécution ;
- un Job Spark ;
- les Stages / Tasks ;
- les informations liées au Shuffle lorsque disponibles.

## README

Présentez brièvement :

- votre sujet ;
- vos données ;
- votre keyspace et votre table ;
- votre question métier ;
- les traitements réalisés ;
- vos principales observations.
