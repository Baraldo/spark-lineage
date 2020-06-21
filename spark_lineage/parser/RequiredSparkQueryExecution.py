import json
from spark_lineage.LineageParser import LineageParser

class RequiredSparkQueryExecution(LineageParser):
    def parse(self):
        query_execution = self.lineage.dataframe._jdf.queryExecution()
        json_data = json.loads(query_execution.logical().prettyJson())
        required = []
        for j in json_data:
            if j.get('class'):
                if j['class'] == 'org.apache.spark.sql.catalyst.plans.logical.Project':
                    if j.get('projectList'):
                        for layer in j['projectList']:
                            for element in layer:
                                if element.get('class'):
                                    if element['class'] == 'org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute':
                                        required.append(element.get('nameParts'))
        return list(set(required))