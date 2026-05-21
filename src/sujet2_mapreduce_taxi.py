"""
PFE-M 2026 — Sujet 2 : MapReduce — Rentabilité Horaire NYC Taxi
Version : Mac M3 local (PySpark sans Docker)
Lancement : python3 src/sujet2_mapreduce_taxi.py
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp
import time, os, json

# ── CONFIG ────────────────────────────────────────────────────────────────────
conf = SparkConf() \
    .setAppName("PFE_MapReduce_Rentabilite") \
    .setMaster("local[*]") \
    .set("spark.driver.memory", "2g") \
    .set("spark.sql.shuffle.partitions", "24")

sc    = SparkContext(conf=conf)
spark = SparkSession(sc)
sc.setLogLevel("WARN")

print("=" * 60)
print("  PFE-M 2026 | Sujet 2 — MapReduce : Rentabilité Horaire")
print("=" * 60)

# ── CHEMINS ───────────────────────────────────────────────────────────────────
DATA_PATH    = "data/*.parquet"
RESULTS_PATH = "results/"
os.makedirs(RESULTS_PATH, exist_ok=True)

# ── 1. INGESTION ──────────────────────────────────────────────────────────────
print(f"\n[1/5] Chargement : {DATA_PATH}")
t0 = time.time()

df_raw = spark.read.parquet(DATA_PATH)

n_total = df_raw.count()
print(f"      → {n_total:,} lignes chargées en {time.time()-t0:.1f}s")
print(f"      → Colonnes : {df_raw.columns[:8]}...")

# ── 2. PRÉ-TRAITEMENT ─────────────────────────────────────────────────────────
print("\n[2/5] Extraction de l'heure de prise en charge...")

# Détection automatique du nom de colonne datetime
date_col = None
for candidate in ["tpep_pickup_datetime", "pickup_datetime", "lpep_pickup_datetime"]:
    if candidate in df_raw.columns:
        date_col = candidate
        break

if date_col is None:
    print(f"      Colonnes disponibles : {df_raw.columns}")
    raise ValueError("Colonne datetime introuvable !")

print(f"      → Colonne détectée : {date_col}")

df_prepared = df_raw.select(
    hour(to_timestamp(col(date_col))).alias("pickup_hour"),
    col("fare_amount").cast("double"),
    col("trip_distance").cast("double")
).filter(
    col("pickup_hour").isNotNull() &
    col("fare_amount").isNotNull() &
    col("trip_distance").isNotNull()
)

rdd_raw = df_prepared.rdd
print(f"      → RDD créé | {rdd_raw.getNumPartitions()} partitions")

# ── 3. PHASE MAP ──────────────────────────────────────────────────────────────
print("\n[3/5] Phase MAP...")

def map_function(row):
    h, fare, dist = row["pickup_hour"], row["fare_amount"], row["trip_distance"]
    if h is not None and fare is not None and dist is not None:
        if 0 < fare < 500 and 0.1 < dist < 200:
            yield (int(h), (fare, dist, 1))

rdd_mapped = rdd_raw.flatMap(map_function)
print("      → MAP terminé")

# ── 4. PHASE SHUFFLE & SORT ───────────────────────────────────────────────────
NUM_REDUCERS = 24
print(f"\n[4/5] Phase SHUFFLE — HashPartitioner (R={NUM_REDUCERS})...")

rdd_shuffled = rdd_mapped.partitionBy(NUM_REDUCERS, lambda k: k % NUM_REDUCERS)

partition_counts = rdd_shuffled \
    .mapPartitionsWithIndex(lambda i, it: [(i, sum(1 for _ in it))]) \
    .collect()

n_valides = sum(c for _, c in partition_counts)
max_part  = max(c for _, c in partition_counts)
avg_part  = n_valides / NUM_REDUCERS
skew      = max_part / avg_part if avg_part > 0 else 1

print(f"      → {n_valides:,} lignes valides")
print(f"      → Taux de rejet : {100*(n_total-n_valides)/n_total:.1f}%")
print(f"      → Data Skew : {skew:.2f}x {'⚠️  SKEW DÉTECTÉ' if skew > 3 else '✅ OK'}")

# ── 5. PHASE REDUCE ───────────────────────────────────────────────────────────
print("\n[5/5] Phase REDUCE...")
t_reduce = time.time()

def reduce_function(v1, v2):
    return (v1[0]+v2[0], v1[1]+v2[1], v1[2]+v2[2])

results = rdd_shuffled.reduceByKey(reduce_function).map(lambda kv: {
    "heure"        : kv[0],
    "gain_km"      : round(kv[1][0]/kv[1][1], 4) if kv[1][1] > 0 else 0,
    "fare_moyen"   : round(kv[1][0]/kv[1][2], 2) if kv[1][2] > 0 else 0,
    "dist_moyenne" : round(kv[1][1]/kv[1][2], 2) if kv[1][2] > 0 else 0,
    "nb_courses"   : kv[1][2]
}).collect()

results.sort(key=lambda x: x["heure"])
print(f"      → REDUCE terminé en {time.time()-t_reduce:.1f}s")

# ── RÉSULTATS ─────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  RÉSULTATS — Gain moyen par km ($) par heure")
print("=" * 60)
print(f"  {'H':<6} {'Gain/km':>9} {'Recette':>9} {'Distance':>10} {'Courses':>12}")
print("  " + "─" * 52)

max_gain = max(r["gain_km"] for r in results)
min_gain = min(r["gain_km"] for r in results)

for r in results:
    tag = " 🟢 PEAK"  if r["gain_km"] == max_gain else \
          (" 🔴 CREUX" if r["gain_km"] == min_gain else "")
    print(f"  {r['heure']:02d}h   "
          f"  {r['gain_km']:>8.3f}$"
          f"  {r['fare_moyen']:>8.2f}$"
          f"  {r['dist_moyenne']:>9.2f}km"
          f"  {r['nb_courses']:>10,}{tag}")

peak  = max(results, key=lambda x: x["gain_km"])
creux = min(results, key=lambda x: x["gain_km"])
print(f"\n  ✅ Plus rentable  : {peak['heure']:02d}h → ${peak['gain_km']}/km")
print(f"  ❌ Moins rentable : {creux['heure']:02d}h → ${creux['gain_km']}/km")

# Sauvegarde JSON
out = os.path.join(RESULTS_PATH, "rentabilite_horaire.json")
with open(out, "w") as f:
    json.dump(results, f, indent=2)
print(f"\n  💾 Résultats sauvegardés → {out}")

# ── ANALYSE SHUFFLE ───────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  ANALYSE — Volume réseau Shuffle selon R")
print("=" * 60)
S = 40
for R in [1, 4, 8, 24, 48]:
    vol = (n_valides * S) / (1024**2)
    print(f"  R={R:<4} → {vol:.0f} Mo total | {vol/R:.1f} Mo/reducer")

sc.stop()
print("\n" + "=" * 60)
print("  JOB TERMINÉ ✅")
print("=" * 60)