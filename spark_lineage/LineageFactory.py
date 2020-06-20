from spark_lineage.Lineage import Lineage
from spark_lineage.domain.Parser import Parser

class LineageFactory:
    def __init__(self, parser: Parser):
        self.parser = Parser
    
    def lineage(self, is_extractor=False, description=None):
        def wrapper(func):
            def _wrapper(*args, **kwargs):
                return Lineage(func.__name__, func, is_extractor, description,  self.parser, *args, **kwargs)
            return _wrapper
        return wrapper