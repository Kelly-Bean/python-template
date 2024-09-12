from pathlib import Path


THIS_DIR = Path(__file__).resolve().parent
RESOURCES_DIR = THIS_DIR.parent.parent / "resources"
DATA_DIR = RESOURCES_DIR / "data"
GITIGNORED_CACHE_DIR = RESOURCES_DIR / "cachedir"

GCP_PROJECT_ID = "bskb-data-prod"
GCP_BUCKET_ID = "bskb-data-prod"
