"""
A set of functions to make uploading data via the PyCelonis API easier
"""

from pycelonis.service.integration.service import ColumnTransport
from pandas import DataFrame
from pandas.api.types import infer_dtype
from pyarrow import ArrowInvalid
from typing import List, Optional, Union
from utils import numpy_dtype_to_celonis_coltype
import regex as re

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

def extract_arrow_invalid_error_values(msg:str) -> dict:
    """
    Extracts the column, column type, error value, and the to and from cast 
    types from the `ArrowInvalid` error msg.
    """
    return {
        'column': re.search(r"column\s(.+)\swith", msg).group(1),
        'column_type': re.search(r".*\s(.+)$", msg).group(1),
        'error_value': re.search(r"convert\s'(.+)'\swith", msg).group(1),
        'type_from': re.search(r"with\stype\s(.+):", msg).group(1),
        'type_to': re.search(r"convert to\s(.+)\sC", msg).group(1),
    }

def catch_arrow_invalid_errors(df:DataFrame) -> Union[DataFrame, None]:
    """
    Catches arrow invlaid errors and reports them back in a dataframe
    Helps the user understand if their dataframe is fit for converison
    to parquest

    params:
        df: the dataframe to check
    
    returns:
        a dataframe containing the incorrect colums and the converison
        error or None if no error

    """
    errors_df = DataFrame(columns=['column', 'column_type', 'error_value', 'type_from', 'type_to'])
    for col in df.columns:
        try:
            df[[col]].to_parquet(engine="pyarrow", use_deprecated_int96_timestamps=True, index=False)
        except ArrowInvalid as e:
            msg = " ".join(e.args)
            error_values = extract_arrow_invalid_error_values(msg)
            errors_df.loc[len(errors_df), :] = error_values
    
    if len(errors_df)==0:
        return None
    return errors_df



            





    



