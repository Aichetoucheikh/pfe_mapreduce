# Rapport de Projet Big Data — Sujet 2 : Analyse de Rentabilité via MapReduce Natif

**Établissement** : Université Libanaise Internationale (LIU) — Master Big Data 2026  
**Étudiante** : Aichetou Cheikh (Matricule : 12310014)  
**Encadrant** : Dr El Benany Med Mahmoud  

---

## 1. Introduction et Contexte Technique

L'utilisation moderne de frameworks comme Spark SQL ou l'abstraction des DataFrames masque la complexité réelle du mouvement des données à travers un cluster. Ce projet impose un retour aux fondements du calcul distribué en implémentant le paradigme **MapReduce pur** via l'API RDD (*Resilient Distributed Datasets*) d'Apache Spark. 

L'objectif est d'analyser la rentabilité horaire des taxis jaunes de New York (NYC Yellow Taxi) sur un volume massif de **3 066 766 lignes**, sans l'aide de l'optimiseur de requêtes Catalyst, en manipulant exclusivement des structures de paires Clé-Valeur.

La formule de rentabilité horaire retenue est une moyenne pondérée :
$$\text{Gain/km}(h) = \frac{\sum \text{fare\_amount}(h)}{\sum \text{trip\_distance}(h)}$$

---

## 2. Architecture de l'Environnement Local (Mac M3)

Pour contourner les couches d'émulation lourdes et complexes des conteneurs Docker (x86_64) sur l'architecture ARM64 Apple Silicon, l'environnement s'exécute directement sur l'hôte (*Bare-metal*) :
* **Processeur** : Puce Apple M3 (Architecture ARM64 native)
* **Moteur de calcul** : Apache Spark 3.5.0 en mode `local[*]` (allocation de tous les cœurs CPU disponibles)
* **Ingestion** : Couche `pyarrow` intégrée pour la lecture directe des fichiers natifs au format Parquet (orienté colonne), évitant la phase de conversion intermédiaire en fichiers CSV.

---

## 3. Implémentation Algorithmique des Phases MapReduce

L'algorithme s'articule autour des trois phases rigoureuses du paradigme :
[Fichiers Parquet Data]
│
▼
┌───────────┐
│ PHASE MAP │ ──► Extraction de l'heure & Filtrage des anomalies
└───────────┘     Émet : Clé = heure (int) | Valeur = (fare_amount, trip_distance, 1)
│
▼
┌───────────┐
│  SHUFFLE  │ ──► Regroupement physique par clé via HashPartitioner (R=24)
└───────────┘
│
▼
┌───────────┐
│   REDUCE  │ ──► Agrégation associative/commutative membre à membre
└───────────┘     Calcul final du ratio : Somme(fares) / Somme(distances)
│
▼
[results/rentabilite_horaire.json]

### A. Phase Map
Chaque enregistrement du dataset est lu. Les données aberrantes ou nulles (distances inférieures ou égales à 0, montants négatifs ou nuls) sont éliminées pour garantir la pureté statistique. 
* **Clé émise** : L'heure de prise en charge (`pickup_hour` extrait du timestamp sous forme d'entier de 0 à 23).
* **Valeur émise** : Un tuple complexe comprenant trois variables : `(fare_amount, trip_distance, 1)`. Le troisième élément fait office de compteur pour le calcul ultérieur du nombre total de courses.

### B. Phase Shuffle & Sort
Le framework Spark regroupe les données de même clé. Par défaut, le mécanisme s'appuie sur le `HashPartitioner`. Le numéro de la partition cible pour une clé donnée est déterminé par la formule :
$$\text{Partition} = \text{Key.hashCode()} \pmod R$$
Où $R$ représente le nombre de Reducers configuré. Dans notre architecture, nous avons fixé explicitement $R=24$ pour correspondre strictement aux 24 tranches horaires d'une journée, assurant qu'une partition traite l'intégralité des données d'une heure spécifique.

### C. Phase Reduce
L'agrégation applique une fonction associative et commutative. Pour éviter le piège mathématique classique de la "moyenne des moyennes" (qui fausserait le résultat en ignorant le poids réel de la distance de chaque trajet), le Reducer combine les tuples de même clé en faisant la somme membre à membre :
$$\text{Reduce}(V_1, V_2) = (fare_1 + fare_2, \text{distance}_1 + \text{distance}_2, \text{count}_1 + \text{count}_2)$$

Le ratio final (Gain au kilomètre) est évalué lors de la transformation terminale en divisant la somme totale des recettes accumulées par la somme totale des distances parcourues pour chaque heure.

---

## 4. Exigences de Réflexion Théorique

### A. Analyse Mathématique du Shuffle
Soit $N$ le nombre de lignes en entrée ($3\ 066\ 766$) et $R$ le nombre de Reducers ($24$).
* **Volume de données transitant sur le réseau** : En l'absence de pré-agrégation côté map (*Combiner*), le volume de paires Clé-Valeur transitant lors du Shuffle est d'ordre $\mathcal{O}(N)$. 
* **Impact de $R$ sur la parallélisation** : Si $R$ est trop petit (ex: $R=1$), l'ensemble du jeu de données est acheminé vers une unité unique, supprimant tout avantage du calcul distribué. Idéalement, $R$ doit s'aligner sur le nombre de clés distinctes et sur les capacités de parallélisme du processeur (cœurs du Mac M3).
* **Impact sur le temps de fusion (Merge)** : La phase de fusion au niveau des Reducers suit une complexité algorithmique de $\mathcal{O}(\frac{N}{R} \log(\frac{N}{R}))$. Augmenter le nombre de Reducers $R$ réduit la taille du bloc local à trier et fusionner par chaque unité, accélérant le temps de traitement individuel au prix d'une fragmentation accrue des fichiers de sortie.

### B. Gestion du Data Skew (Asymétrie des Données)
Si un événement particulier se produit (par exemple, si 80 % des trajets de taxi se concentrent à 8h du matin en raison de l'embauche), le système fait face à un phénomène critique de **Data Skew**.
* Le `HashPartitioner` enverra l'ensemble de ces 80 % de lignes vers le Reducer unique responsable de la clé `8`.
* **Impact direct** : Ce Reducer devra traiter à lui seul un volume disproportionné de données ($0.8 \times N$), provoquant un effet de goulet d'étranglement (*Straggler Effect*). Le job Spark global prendra autant de temps à se terminer que ce Reducer le plus lent, laissant les 23 autres cœurs du processeur M3 inactifs après avoir terminé prématurément leurs partitions légères. De plus, cela peut saturer la mémoire vive (OOM) et forcer l'écriture sur disque (*Spilling*), effondrant les performances.

---

## 5. Applications Métiers Orientées Contexte Mauritanien

La logique de traitement MapReduce mise en œuvre pour les Taxis de New York est directement transposable à des problématiques industrielles et logistiques clés en Mauritanie :

### A. Optimisation des Transports : Axe Nouakchott — Nouadhibou
Pour une entreprise de transport de passagers ou de marchandises opérant des trajets réguliers sur la Route Nationale 2 (RN2) reliant Nouakchott à Nouadhibou :
* **Phase Map** : Émettre comme clé la tranche horaire de départ (ou les conditions météo/vent) et comme valeur le tuple complexe `(recettes_tickets_MRU, consommation_carburant_litres)`.
* **Phase Reduce** : Consolider la consommation totale et les gains financiers pour calculer le ratio de rentabilité économique par litre de carburant sur cet axe de transport majeur. Cela permettrait d'identifier scientifiquement s'il faut restreindre les départs nocturnes (surconsommation liée à l'utilisation intensive des phares, climatisation ou ralentissements induits par l'ensablement de la chaussée).

### B. Analyse de la Charge Électrique : SOMELEC
Dans le cadre de la modernisation et du suivi du réseau de distribution électrique de la SOMELEC à Nouakchott :
* **Phase Map** : Chaque capteur ou compteur communicant déployé dans la ville transmet des mesures de consommation. Le script extrait l'identifiant du `transformateur_quartier` comme clé de routage, et émet la puissance appelée instantanée `(consommation_kVA, horodatage)` comme valeur.
* **Phase Shuffle** : Regroupe physiquement toutes les données de consommation d'un même secteur (ex: Tevragh Zeina, Ksar, Sebkha, El Mina) vers une unité de traitement dédiée.
* **Phase Reduce** : Le Reducer calcule la charge agrégée par secteur géolocalisé pour identifier en temps réel les pics critiques de consommation. Cette approche permet de prédire les risques de surchauffe des équipements électriques de quartier et de planifier intelligemment les opérations de maintenance ou de délestage ciblé.

---

## 6. Conclusion académique

Ce projet démontre l'efficacité et la robustesse du paradigme MapReduce pour les traitements de type "Batch" massifs. Si les abstractions de haut niveau (DataFrames/Spark SQL) s'avèrent plus rapides à l'exécution grâce aux optimisations de code générées dynamiquement par Catalyst, l'API RDD native offre une **prévisibilité totale du cycle de vie des données**, un contrôle total sur l'utilisation de la mémoire et une excellente tolérance aux pannes (recalcul d'une partition isolée en cas d'échec d'un nœud). C'est la solution par excellence pour la construction d'architectures d'ingestion critiques où la fiabilité géométrique du mouvement des données prévaut sur la vitesse brute.