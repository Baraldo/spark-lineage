from pyspark.sql import DataFrame
from spark_lineage.Exceptions import LinageException 
import inspect
import re


class Lineage:
    def __init__(self, name, func, is_extractor, description, *args, **kwargs):
        self.name = name
        self.func = func
        self.description = description
        self.__run(args, kwargs)
        
        if is_extractor:
            '''It is expected to have a path kwarg here to indentify the source.'''
            self.require = [kwargs.get('path')]
            self.produced = self.dataframe.columns
        else:
            self.__parse_required_columns()
            self.__infer_produced_columns()


    def __parse_required_columns(self):
        required_cols = []
        s=inspect.getsource(self.func).replace('\n', ' ').replace('\r', '').replace(" ", "")
        req_columns = re.findall(r'col\((.*?)\)', s)
        self.require = req_columns


    def __parse_produced_columns(self):
        produced_cols = []
        s=inspect.getsource(self.func).replace('\n', ' ').replace('\r', '').replace(" ", "")
        with_columns = re.findall(r'colName=\'(.*?)\'',s)
        self.produce = with_columns


    def __infer_produced_columns(self):
        columns = []
        for l_object in self.lineage:
            columns = columns + l_object.dataframe.columns

        self.produce = [c for c in self.dataframe.columns if c not in columns]


    def __run(self, *args, **kwargs):
        lineage_list = []
        arg_list = list(args[0])
        for num, arg in enumerate(arg_list):
            if isinstance(arg, Lineage):
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
    

    @staticmethod
    def lineage(is_extractor=False, description=None):
        def wrapper(func):
            def _wrapper(*args, **kwargs):
                return Lineage(func.__name__, func, is_extractor, description, *args, **kwargs)
            return _wrapper
        return wrapper