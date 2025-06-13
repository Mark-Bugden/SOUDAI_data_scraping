# src/preprocessing/components/paragraf.py
import re


def extract_paragraf(text: str) -> str | None:
    match = re.search(r"§\s*([0-9]{1,4}[a-z]?)", text, flags=re.IGNORECASE)
    return match.group(1) if match else None
