from spark_lineage.Lineage import Lineage
from spark_lineage.LineagerWrapper import LineageWrapper
from spark_lineage.parser import ProducedInfer, RequiredSparkQueryExecution

class LineageFactory:
    lineage: Lineage
    def __init__(self, produced_parser= ProducedInfer.ProducedInfer, required_parser = RequiredSparkQueryExecution.RequiredSparkQueryExecution):
        self.produced_parser = produced_parser
        self.required_parser = required_parser
    
    def lineage(self, is_extractor=False, description=None):
        def wrapper(func):
            def _wrapper(*args, **kwargs):
                lineage_wrapper = LineageWrapper(func, description, *args, **kwargs)
                return Lineage(lineage_wrapper, self.produced_parser, self.required_parser)
            return _wrapper
        return wrapper