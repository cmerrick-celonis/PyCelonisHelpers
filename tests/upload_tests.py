from unittest import TestCase
import upload
import datetime as dt

class TestGetColumnConfig(TestCase):
    """
    A set of tests to ensure the `get_column_config` function
    works as expected.
    """
    
    def setUp(self) -> None:
        #df containing all types
        self.df = upload.DataFrame(data={
            'id':[f'a{i}' for i in range(1,10)],
            'date_stamp':[dt.datetime(2024,1, i) for i in range(1,10)],
            'gross_value':[i for i in range(1,10)],
            'net_value':[i + 0.05 for i in range(1,10)],
            'priority':[True for _ in range(1,6)] + [False for _ in range(1,5)]
        })
        self.expected_configs_pk_specified = [
            upload.ColumnTransport(column_name='id', column_type='STRING', field_length=2, decimals=None, pk_field=True),
            upload.ColumnTransport(column_name='date_stamp', column_type='DATETIME', field_length=None, decimals=None, pk_field=False),
            upload.ColumnTransport(column_name='gross_value', column_type='INTEGER', field_length=None, decimals=None, pk_field=False),
            upload.ColumnTransport(column_name='net_value', column_type='FLOAT', field_length=None, decimals=2, pk_field=False),
            upload.ColumnTransport(column_name='priority', column_type='BOOLEAN', field_length=None, decimals=None, pk_field=False),
        ]
        self.expected_configs_pk_not_specified = [
            upload.ColumnTransport(column_name='id', column_type='STRING', field_length=2, decimals=None, pk_field=False),
            upload.ColumnTransport(column_name='date_stamp', column_type='DATETIME', field_length=None, decimals=None, pk_field=False),
            upload.ColumnTransport(column_name='gross_value', column_type='INTEGER', field_length=None, decimals=None, pk_field=False),
            upload.ColumnTransport(column_name='net_value', column_type='FLOAT', field_length=None, decimals=2, pk_field=False),
            upload.ColumnTransport(column_name='priority', column_type='BOOLEAN', field_length=None, decimals=None, pk_field=False),
        ]
        return super().setUp()
    
    def test_get_column_config_incorrect_pk_field(self):
        """
        If the specificed pk_field is not in the dataframe columns 
        we expect a `ValueError`
        """
        self.assertRaises(ValueError, upload.get_column_config, df=self.df, pk_field='some_invalid_string')
    
    def test_get_column_config_correct_pk_field(self):
        """
        ensures the `get_column_config` function returns an array
        of `ColumnTransport` objects.

        Ensures these objects are correctly specified - including the pk_field set as True
        for the correct column config
        """
        column_configs = upload.get_column_config(df=self.df, pk_field='id')
        self.assertListEqual(self.expected_configs_pk_specified, column_configs)


    def test_get_column_config_no_pk_field(self):
        """
        ensures the `get_column_config` function returns an array
        of `ColumnTransport` objects.

        Ensures these objects are correctly specified - including the pk_field set as False
        for all the column configs
        """
        column_configs = upload.get_column_config(df=self.df)
        self.assertListEqual(self.expected_configs_pk_not_specified, column_configs)
    
