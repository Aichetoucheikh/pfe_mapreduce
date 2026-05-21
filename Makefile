# ─────────────────────────────────────────────────────────────────────────────
# PFE-M 2026 — Makefile | Mac M3 + Docker
# Usage : make <commande>
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: help up down status submit logs jupyter clean data

# Couleurs terminal
BLUE  = \033[0;34m
GREEN = \033[0;32m
RESET = \033[0m

help: ## Afficher l'aide
	@echo ""
	@echo "$(BLUE)PFE MapReduce — Commandes disponibles$(RESET)"
	@echo "─────────────────────────────────────────"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-12s$(RESET) %s\n", $$1, $$2}'
	@echo ""

# ── CLUSTER ──────────────────────────────────────────────────────────────────

up: ## Démarrer tout le cluster (master + 2 workers + jupyter)
	@echo "$(BLUE)Démarrage du cluster Spark...$(RESET)"
	docker compose up -d
	@echo "$(GREEN)✅ Cluster démarré !$(RESET)"
	@echo "   → Spark UI   : http://localhost:8080"
	@echo "   → Jupyter    : http://localhost:8888  (token: pfe2026)"
	@echo "   → Spark App  : http://localhost:4040  (pendant un job)"

down: ## Arrêter le cluster
	docker compose down
	@echo "$(GREEN)✅ Cluster arrêté$(RESET)"

status: ## Voir l'état des conteneurs
	docker compose ps

logs: ## Voir les logs du master
	docker compose logs -f spark-master

# ── JOB SPARK ────────────────────────────────────────────────────────────────

submit: ## Lancer le job MapReduce principal
	@echo "$(BLUE)Lancement du job MapReduce...$(RESET)"
	docker exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--executor-memory 1500m \
		--executor-cores 2 \
		--num-executors 2 \
		/app/src/sujet2_mapreduce_taxi.py
	@echo "$(GREEN)✅ Job terminé ! Résultats dans ./results/$(RESET)"

submit-local: ## Lancer en mode local (sans cluster, pour test rapide)
	@echo "$(BLUE)Lancement en mode local...$(RESET)"
	docker exec \
		-e SPARK_MASTER=local[*] \
		spark-master spark-submit \
		--master local[*] \
		/app/src/sujet2_mapreduce_taxi.py

# ── DONNÉES ──────────────────────────────────────────────────────────────────

data: ## Télécharger le dataset NYC Taxi (Jan 2023)
	@echo "$(BLUE)Téléchargement des données NYC Taxi...$(RESET)"
	@mkdir -p data
	@echo "Téléchargement janvier 2023..."
	curl -L -o data/yellow_taxi_2023-01.parquet \
		"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
	@echo "Téléchargement février 2023..."
	curl -L -o data/yellow_taxi_2023-02.parquet \
		"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet"
	@echo "$(GREEN)✅ Données téléchargées dans ./data/$(RESET)"
	@echo "   Conversion Parquet → CSV en cours..."
	docker exec spark-master spark-submit \
		--master local[*] \
		/app/src/convert_parquet_to_csv.py
	@echo "$(GREEN)✅ Conversion terminée$(RESET)"

# ── JUPYTER ──────────────────────────────────────────────────────────────────

jupyter: ## Ouvrir Jupyter dans le navigateur
	open http://localhost:8888

# ── NETTOYAGE ────────────────────────────────────────────────────────────────

clean: ## Supprimer les résultats (pas les données)
	rm -rf results/*
	@echo "$(GREEN)✅ Résultats supprimés$(RESET)"

clean-all: ## Supprimer tout (conteneurs + volumes Docker)
	docker compose down -v
	rm -rf results/*
	@echo "$(GREEN)✅ Nettoyage complet$(RESET)"

# ── SHELL ────────────────────────────────────────────────────────────────────

shell: ## Ouvrir un shell dans le master
	docker exec -it spark-master bash

shell-worker: ## Ouvrir un shell dans le worker 1
	docker exec -it spark-worker-1 bash
