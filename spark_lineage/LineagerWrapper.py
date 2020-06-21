from pyspark.sql import DataFrame
from typing import Callable

import spark_lineage
from spark_lineage.Exceptions import LinageException


class LineageWrapper:
    func: Callable
    description: str
    dataframe: DataFrame
    lineage: list

    def __init__(self, func, description, *args, **kwargs):
        self.func = func
        self.description = description
        self.__run(args, kwargs)
            

    # def __required_tables_spark_execution_parser(self):
    #     query_execution = self.dataframe._jdf.queryExecution()
    #     logical_dict_list = json.loads(query_execution.logical().prettyJson())
    #     required = []
    #     for x in logical_dict_list:
    #         if x.get('tableIdentifier'):
    #             required.append(x['tableIdentifier'].get('table'))
    #     self.require = required

    def __run(self, *args, **kwargs):
        lineage_list = []
        arg_list = list(args[0])
        for num, arg in enumerate(arg_list):
            if isinstance(arg, spark_lineage.Lineage.Lineage):
                arg_list[num] = arg.dataframe
                lineage_list.append(arg)
        r_args = tuple(arg_list)

        for key, value in kwargs.items():
            if type(value) == Lineage:
                kwargs[key] = value.dataframe
                lineage_list.append(value)
        r_kwargs=kwargs

        self.dataframe = self.func(*r_args, **r_kwargs)
        self.lineage = lineage_list