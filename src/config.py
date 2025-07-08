from pathlib import Path

# Automatically resolve the project root
PROJ_ROOT = Path(__file__).resolve().parent.parent

REFERENCES_DIR = PROJ_ROOT / "references"
DATA_DIR = PROJ_ROOT / "data"

INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"
RAW_DIR = DATA_DIR / "raw"


# Paths for components
LAW_TYPE_PATTERN_PATH = (
    PROJ_ROOT / "src/preprocessing/components/law_type_patterns.yaml"
)

# Checkpoint path
PREPROCESSED_CSV_PATH = INTERIM_DIR / "preprocessed_decisions.csv"
CHECKPOINT_CSV_PATH = INTERIM_DIR / "infosoud_checkpoint.csv"


# Base URLs
INFO_SOUD_BASE_URL = "https://infosoud.justice.cz/InfoSoud/public/search.do"


IMPORTANT_EVENTS = {
    "Zahájení řízení",
    "Vydání rozhodnutí",
    "Vyřízení věci",
    "Nařízení jednání",
    "Datum pravomocného ukončení věci",
    "Skončení věci",
}
