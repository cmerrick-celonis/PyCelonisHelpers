"""
A set of functions to make uploading data via the PyCelonis API easier
"""

from pycelonis.service.integration.service import ColumnTransport
from pandas import DataFrame
from typing import List, Optional
from utils import numpy_dtype_to_celonis_coltype

def get_column_config(df:DataFrame, pk_field:Optional[str]=None, decimal_places:int=2)->List[ColumnTransport]:
    """
    Autogenerates the column config array from the data frame
    you are trying to upload.

    A column config contains:
        column_name -> this is the column name in the df
        column_type -> the pandas dtype is converted into a string
            representing an acceptable celonis data type
        field_length -> max length from the df column is used if string
            None otherwise
        decimal_places -> provided by the user default is 2 d.p
        pk_field_name -> provided by the user, represented in `ColumnTransport` by a boolean

    params:
        df: the dataframe you are trying to upload
        pk_field: the column representing the primary key
    
    returns:
        an array of `ColumnTransport` objects
    """
    if pk_field is not None and pk_field not in df.columns:
        raise ValueError(f'pk_field {pk_field} not in columns')
    
    column_names = df.columns.to_list()
    column_types = list(map(numpy_dtype_to_celonis_coltype, df.dtypes))
    _field_lengths = df.map(lambda x: len(str(x))).max(axis=0).to_list()
    field_lengths = [_field_lengths[l] if column_types[l] == 'STRING' else None for l in range(len(_field_lengths))]
    decimals = [decimal_places if col_type == 'FLOAT' else None for col_type in column_types]
    pk_field_identifiers = list(map(lambda x: x==pk_field, column_names))

    configs = zip(column_names, column_types, field_lengths, decimals, pk_field_identifiers)
    return [ColumnTransport(column_name=config[0], column_type=config[1], field_length=config[2], decimals=config[3], pk_field=config[4]) for config in configs]




    



