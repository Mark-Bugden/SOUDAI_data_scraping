from collections import OrderedDict

from preprocessing.components.cislo import extract_cislo
from preprocessing.components.law_type import extract_law_type
from preprocessing.components.paragraf import extract_paragraf


def parse_reference(text: str) -> OrderedDict:
    return OrderedDict(
        [
            ("law_type", extract_law_type(text)),
            ("cislo", extract_cislo(text)),
            ("paragraf", extract_paragraf(text)),
            ("odstavec", None),
            ("pismeno", None),
        ]
    )
