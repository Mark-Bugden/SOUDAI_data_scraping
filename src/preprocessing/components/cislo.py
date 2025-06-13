import re


def extract_cislo(text: str) -> str | None:
    match = re.search(r"č\.\s*([0-9]{1,4}/[0-9]{4})\s*Sb\.", text)
    return match.group(1) if match else None
