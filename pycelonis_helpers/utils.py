"""
utility functions for the PyCelonisHelpers wrapper
"""
import pandas as pd
import numpy as np
from pycelonis.service.integration.service import ColumnType

def numpy_dtype_to_celonis_coltype(dtype:np.dtype) -> str:
    """
    converts a numpy dtype object into a string that matches
    an acceptable data type in Celonis
    """
    if pd.api.types.is_string_dtype(dtype):
        return ColumnType.STRING
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return ColumnType.DATETIME
    elif pd.api.types.is_float_dtype(dtype):
        return ColumnType.FLOAT
    elif pd.api.types.is_integer_dtype(dtype):
        return ColumnType.INTEGER
    elif pd.api.types.is_bool_dtype(dtype):
        return ColumnType.BOOLEAN
    else:
        raise ValueError(f'numpy {dtype} cannot be converted to Celonis type')
    

        

