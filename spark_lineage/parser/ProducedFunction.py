import re
import inspect
from spark_lineage.LineageParser import LineageParser


class ProducedFunction(LineageParser):
    def parse(self):
        produced_cols = []
        s=inspect.getsource(self.lineage.func).replace('\n', ' ').replace('\r', '').replace(" ", "")
        with_columns = re.findall(r'colName=\'(.*?)\'',s)
        return with_columns