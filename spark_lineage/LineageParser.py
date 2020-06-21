import abc
from spark_lineage.LineagerWrapper import LineageWrapper

class LineageParser:
    def __init__(self, lineage: LineageWrapper):
        self.lineage = lineage

    @abc.abstractmethod
    def parse(self):
        return