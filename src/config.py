HEARTBEAT_INTERVAL = 2    # Secondes entre chaque heartbeat
ELECTION_TIMEOUT = 6       # Timeout élection (secondes)
MAX_MISSED_HB = 5         # Heartbeats manqués avant élection
COORDINATOR_WAIT = 3 * ELECTION_TIMEOUT
MESSAGE_DEDUP_TTL = 5 * COORDINATOR_WAIT

DEFAULT_NODE_COUNT = 5
 
MESSAGE_LOSS_RATE = 0.0    # Taux de perte (0.0 à 0.5)
NETWORK_LATENCY_MS = 0     # Latence simulée (ms)
RANDOM_SEED = 42          
VERBOSE_LOGGING = True
MONITOR_INTERVAL = 1
