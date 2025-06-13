from pathlib import Path

# Automatically resolve the project root
PROJ_ROOT = Path(__file__).resolve().parent

# Paths for components
LAW_TYPE_PATTERN_PATH = (
    PROJ_ROOT / "preprocessing" / "components" / "law_type_patterns.yaml"
)
