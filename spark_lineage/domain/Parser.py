from enum import Enum


class Parser(Enum):
    INFER_PRODUCED = 'infer_produced_parser'
    VANILLA_PRODUCED = '__parse_produced_columns'