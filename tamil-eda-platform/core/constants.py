#Translation------------------------------------------------------------
MAX_TRANSLATION_CHARS = 5000            # max charactore send to any translation API
MAX_BATCH_SIZE = 100            # max texts in a single batch translation call
MIN_CONFIDENCE_SCORE = 0.60         # below this -> human review queue
MIN_TAMIL_SCRIPT_RATIO = 0.50           #Translated text must be 50%+ tamil script
TRANSLATION_TIMEOUT_SECONDS = 30            #API call timeout

#Pipeline-----------------------------------------------------------------
MAX_RETRIES = 3         #Max retry attempts before dead letter queue
RETRY_BACKOFF_BASE = 2.0            #Exponential backoff base (seconds)
RETRY_BACKOFF_MAX = 0.50            # cap backoff at 60 seconds
DEBOUNCE_SECONDS = 2.0          #file trigger waits this long after detection


#Queue-------------------------------------------------------------------
MAX_QUEUE_SIZE = 10_000         # quque depth before backpressure kicks in
QUEUE_POLL_INTERVAL = 0.1           #dispatcher polls every 100ms

#Storage----------------------------------------------------------------
PARQUET_ROW_GROUP_SIZE = 100_000           #rows per Parquet row group
MAX_DATASET_RECORDS = 10_000_000            # merge prunes above this
MIN_SHARD_RECORDS = 1_000           #tiny last shared merged into previous
RECORDS_PER_SHARD = 100_000         #target records per training shard
MAX_SHARD_BYTES = 500_000_000           #500MB hard cap per shard

#Security----------------------------------------------------------------
SECRET_CACHE_TTL_SECONDS = 300          # vault secret cached for 5 minutes
MAX_INPUT_LENGTH = 10_000           # sanitizer rejects text longer than this
RATE_LIMIT_TRANSLATE = 100          # max translate calls per hour per user
RATE_LIMIT_SUBMIT_JOB = 1_000           # max_job submissions per day per user

#Language----------------------------------------------------------------
MIN_DETECTION_CONFIDENCE = 0.85         # below this -> uncertain -> human review
MIN_SENTENCE_LENTGH = 3         # skip sentence shorter than 3 character
MAX_SENTENCE_LENGTH = 1_000         # skip sentences longer than 1000 characters

#Observability----------------------------------------------------------------
LOG_MAX_BYTES = 100_000_000         #100 MB log file before rotation
LOG_BACKUP_COUNT = 7            # keep 7 days of record logs
HEALTH_CHECK_INTERVAL = 30          # healthcheck runs every 30 seconds

#Worker pool--------------------------------------------------------------
DEFAULT_WORKER_COUNT = 4            # concurrent pipeline workers
MAX_WORKER_COUNT = 20           # hard cap regardless of config

CURRENT_SCHEMA_VERSION = "1.2"
SUPPORTED_SCHEMA_VERSIONS = {"1.0", "1.1", "1.2"}

