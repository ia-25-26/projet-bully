# Projet Systèmes Distribués - Algorithme du Bully

## Description

Ce projet implémente l'**algorithme du Bully** pour l'élection de leader dans un système distribué. Il simule un cluster de nœuds communicants avec détection de pannes, simulation réseau (perte, latence, partition) et collecte de métriques.

### Fonctionnalités

- ✅ Implémentation complète de l'algorithme du Bully (ELECTION, OK, COORDINATOR)
- ✅ Détection de pannes par heartbeat/timeout
- ✅ Simulation réseau : perte de messages (0-50%), latence, partition
- ✅ Gestion des crashes (leader, non-leader, cascade)
- ✅ Métriques en temps réel (messages, temps de convergence)
- ✅ Logging JSON
- ✅ 6 scénarios de test prédéfinis

### Théorème CAP

Ce système est de type **CP** (Consistant et Tolérant aux Partitions) :
- En cas de partition réseau, le système peut élire plusieurs leaders (split-brain)
- La cohérence est prioritaire sur la disponibilité immédiate

## Prérequis

- Python 3.10+
- Docker (optionnel)
- Docker Compose (optionnel)

## Installation et Exécution

### Méthode 1 : Python direct

```bash
# Cloner le dépôt
git clone https://github.com/votre-repo/projet-bully.git
cd projet-bully

# Exécuter tous les scénarios
python src/main.py

# Ou exécuter un scénario spécifique
python -c "from src.main import scenario_A; scenario_A()"

###Méthode 2 : Docker
# Construction de l'image
docker-compose build

# Exécuter tous les scénarios
docker-compose up bully-simulator

# Exécuter un scénario spécifique
docker-compose up scenario-a
docker-compose up scenario-b
docker-compose up scenario-c
docker-compose up scenario-d
docker-compose up scenario-e
docker-compose up scenario-f

###Méthode 3 : Docker (commande simple)
# Construire l'image
docker build -t bully-algorithm .

# Exécuter
docker run --rm -v "$(pwd)/results:/app/results" bully-algorithm