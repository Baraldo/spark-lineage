import re
import inspect
from spark_lineage.LineageParser import LineageParser


class RequiredFunction(LineageParser):
    def parse(self):
        required_cols = []
        s=inspect.getsource(self.lineage.func).replace('\n', ' ').replace('\r', '').replace(" ", "")
        req_columns = re.findall(r'col\((.*?)\)', s)
        return req_columns