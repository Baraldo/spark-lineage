from spark_lineage.LineageParser import LineageParser

class ProducedInfer(LineageParser):
    def parse(self):
        columns = []
        for l_object in self.lineage.lineage:
            columns = columns + l_object.dataframe.columns

        return [c for c in self.lineage.dataframe.columns if c not in columns]