from pyspark.sql import DataFrame
from spark_lineage.Exceptions import LinageException 
import inspect
import re
from functools import wraps


class Lineage:
    dataframe: DataFrame


class Extract(Lineage):
    def __init__(self, name, dataframe, alias):
        self.name = name
        self.alias = alias
        self.dataframe = dataframe


class Produce(Lineage):
    def __init__(self, name, produced, required, lineage: Lineage):
        self.name = name
        self.produced = produced
        self.required = required
        self.lineage = lineage


def extract(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        alias = kwargs.get('alias')
        df = func(*args, **kwargs).alias(alias)
        return Extract(func.__name__,df, alias)
    return wrapper

def produce(func):
    def _get_required_columns(func):
        required_cols = []
        s=inspect.getsource(func).replace('\n', ' ').replace('\r', '').replace(" ", "")
        req_columns = re.findall(r'col\((.*?)\)', s)
        return req_columns

    def _get_produced_columns(func):
        produced_cols = []
        s=inspect.getsource(func).replace('\n', ' ').replace('\r', '').replace(" ", "")
        with_columns = re.findall(r'colName=\'(.*?)\'',s)
        return with_columns
    
    def _collect_lineage(*args, **kwargs):
        lineage_list = []
        arg_list = list(args[0])
        for num, arg in enumerate(arg_list):
            if issubclass(type(arg), Lineage):
                arg_list[num] = arg.dataframe
                lineage_list.append(arg)
        r_args = tuple(arg_list)

        for key, value in kwargs.items():
            if type(value) == Lineage:
                kwargs[key] = value.dataframe
                lineage_list.append(value)
        r_kwargs=kwargs

        return r_args, r_kwargs, lineage_list

    @wraps(func)
    def wrapper(*args, **kwargs):
        produced_columns = _get_produced_columns(func)
        required_columns = _get_required_columns(func)
        r_args, r_kwargs, lineage_list = _collect_lineage(args, kwargs)
        df = func(*r_args, **r_kwargs)
        return Produce(func.__name__,produced_columns,required_columns,lineage_list)
    return wrapper


def transform(func):
    pass


def enrich(func):
    pass