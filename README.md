# Projet Systemes Distribues - Algorithme du Bully

## Description

Ce projet implemente une simulation Python de l'algorithme du Bully pour
l'election de leader dans un cluster distribue. Il utilise uniquement la
stdlib Python, des threads, des files `queue.Queue`, et un bus reseau simule.

Le simulateur couvre les elections, la detection de panne par heartbeat, les
crashes, les partitions reseau, la reconciliation apres partition, les logs
JSON, les metriques JSON et un monitoring console live.

## Fonctionnalites

- Algorithme Bully: `ELECTION`, `OK`, `COORDINATOR`
- Detection de panne par heartbeat et timeouts
- Deduplication des messages via `msg_id` avec expiration memoire
- Join dynamique avec `Cluster.add_node(node_id)`
- Restart de noeud crashe avec `Cluster.restart_node(node_id)`
- Simulation reseau: perte de paquets, latence, partition
- Split brain pendant partition et reconciliation apres restauration
- Leader final apres healing: plus grand ID vivant
- Monitoring console live via `Cluster.start_monitoring()`
- Logs JSON configurables avec `config.VERBOSE_LOGGING`
- Metriques JSON par scenario et par election
- Scale test automatique `N=20`, 10 elections successives
- Tests stdlib `unittest`

## Analyse CAP

Ce simulateur ne doit pas etre presente comme un systeme **CP strict**.

Comportement reel:

- Pendant une partition reseau, chaque sous-groupe peut continuer a fonctionner
  et peut elire son propre leader.
- Cela provoque volontairement un risque de **split brain**: plusieurs leaders
  peuvent exister en meme temps dans des partitions differentes.
- Donc la coherence globale forte n'est pas garantie pendant la partition.
- Apres restauration du reseau, `restore_partition()` lance une reconciliation:
  tous les noeuds vivants convergent vers un leader unique, choisi comme le plus
  grand ID vivant, conformement a l'esprit du Bully.

Conclusion CAP:

- Pendant partition, le simulateur privilegie la disponibilite locale des
  partitions plutot qu'une coherence globale stricte.
- Apres healing, il fournit une convergence eventuelle vers un leader global
  unique.
- Il s'agit donc d'un modele pedagogique avec **coherence eventuelle apres
  reconciliation**, pas d'un systeme CP linearisable.

## Prerequis

- Python 3.14 recommande par le cahier des charges
- Aucune dependance externe Python
- Docker et Docker Compose optionnels

Sur Windows/PowerShell, utilisez `PYTHONIOENCODING=utf-8` pour eviter les
problemes d'affichage console avec certains caracteres Unicode.

## Execution Python directe

Depuis la racine du projet:

```powershell
$env:PYTHONIOENCODING="utf-8"
python src/main.py
```

Executer un scenario specifique depuis la racine:

```powershell
$env:PYTHONPATH="src"
$env:PYTHONIOENCODING="utf-8"
python -c "from main import scenario_A; scenario_A()"
```

Scenarios disponibles:

```powershell
python -c "from main import scenario_A; scenario_A()"
python -c "from main import scenario_B; scenario_B()"
python -c "from main import scenario_C; scenario_C()"
python -c "from main import scenario_D; scenario_D()"
python -c "from main import scenario_E; scenario_E()"
python -c "from main import scenario_F; scenario_F()"
```

Scale test `N=20`, 10 elections:

```powershell
$env:PYTHONPATH="src"
$env:PYTHONIOENCODING="utf-8"
python -c "from main import scenario_G_scale_test; scenario_G_scale_test()"
```

## Tests

La suite de tests utilise uniquement `unittest`:

```powershell
python -B -m unittest discover -s tests -v
```

Tests principaux:

- `test_message_format`
- `test_bully_election`
- `test_duplicate_messages`
- `test_crash_leader`
- `test_restart_node`
- `test_dynamic_join`
- `test_partition`
- `test_partition_recovery`
- `test_metrics`
- `test_scale_scenario`

Verification de couverture avec la stdlib `trace`:

```powershell
$traceDir = Join-Path $env:TEMP "bully_trace_cover"
if (Test-Path -LiteralPath $traceDir) { Remove-Item -LiteralPath $traceDir -Recurse -Force }
python -B -m trace --count --summary --coverdir $traceDir --module unittest discover -s tests
```

## Docker

Construire l'image:

```powershell
docker compose build
```

Executer tous les scenarios A-F:

```powershell
docker compose up bully-simulator
```

Executer un scenario specifique:

```powershell
docker compose up scenario-a
docker compose up scenario-b
docker compose up scenario-c
docker compose up scenario-d
docker compose up scenario-e
docker compose up scenario-f
```

Commande Docker simple:

```powershell
docker build -t bully-algorithm .
docker run --rm bully-algorithm
```

## Metriques et logs

Les scenarios exportent des fichiers JSON:

- `metrics_A.json`
- `metrics_B.json`
- `metrics_C.json`
- `metrics_D.json`
- `metrics_E.json`
- `metrics_F.json`
- `metrics_G_scale.json`
- `logs.json`

Les metriques incluent notamment:

- messages par type
- total elections
- elections reussies
- elections echouees
- taux de succes
- convergence moyenne
- details par election

Le logging peut etre active/desactive a chaud:

```python
import config
config.VERBOSE_LOGGING = False
config.VERBOSE_LOGGING = True
```

## Monitoring live

Exemple d'utilisation:

```python
from main import Cluster

cluster = Cluster(5)
cluster.start()
cluster.start_monitoring(interval=1)
```

Le monitoring affiche:

- ID du noeud
- etat (`FOLLOWER`, `CANDIDATE`, `LEADER`, `DEAD`)
- `leader_id`
- vivant/mort
- election en cours
- heartbeats manques
- perte de paquets
- latence
- partition active ou non
