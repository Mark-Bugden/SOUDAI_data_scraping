# src/preprocessing/components/law_type.py
import re

from config import LAW_TYPE_PATTERN_PATH
from preprocessing.components.load_patterns import load_patterns

LAW_TYPE_REGEXES = load_patterns(LAW_TYPE_PATTERN_PATH)


def extract_law_type(text: str) -> str | None:
    for label, patterns in LAW_TYPE_REGEXES.items():
        for pattern in patterns:
            if re.search(pattern, text, flags=re.IGNORECASE):
                return label
    return None
