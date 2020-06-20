from spark_lineage.Lineage import Lineage
from spark_lineage.domain.Parser import Parser

class LineageFactory:
    def __init__(self, produced_parser=Parser.PRODUCED_INFER_PARSER, required_parser = Parser.REQUIRED_STRING_PARSER):
        self.produced_parser = produced_parser
        self.required_parser = required_parser
    
    def lineage(self, is_extractor=False, description=None):
        def wrapper(func):
            def _wrapper(*args, **kwargs):
                return Lineage(func.__name__, func, is_extractor, description,  self.produced_parser, self.required_parser, *args, **kwargs)
            return _wrapper
        return wrapper