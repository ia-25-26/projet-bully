HEARTBEAT_INTERVAL = 2    # Secondes entre chaque heartbeat
ELECTION_TIMEOUT = 6       # Timeout élection (secondes)
MAX_MISSED_HB = 2         # Heartbeats manqués avant élection
COORDINATOR_WAIT = 3 * ELECTION_TIMEOUT

DEFAULT_NODE_COUNT = 5
 
MESSAGE_LOSS_RATE = 0.0    # Taux de perte (0.0 à 0.5)
NETWORK_LATENCY_MS = 0     # Latence simulée (ms)
RANDOM_SEED = 42          


