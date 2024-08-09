from unittest import TestCase
import datetime as dt
import utils


class TestUtils(TestCase):

    def setUp(self) -> None:
        utils
        return super().setUp()
    
    def test_dtype_conversion(self):
        """
        testing the numpy to celonis datatype conversion
        """
        df = utils.pd.DataFrame(data={
            'int_col':[1, 2, 3, 4, 5],
            'float_col':[1.23, 2.34, 3.45, 4.56, 5.67],
            'date_col':[dt.datetime(2023,1,1), dt.datetime(2023,1,2), dt.datetime(2023,1,3), dt.datetime(2023,1,4), dt.datetime(2023,1,5)],
            'string_col':['string1', 'string2', 'string3', 'string4', 'string5',],
            'bool_col':[True, False, True, False, True],
            }
        )

        self.assertEqual(utils.numpy_dtype_to_celonis_coltype(df['int_col'].dtype), utils.ColumnType.INTEGER)
        self.assertEqual(utils.numpy_dtype_to_celonis_coltype(df['float_col'].dtype), utils.ColumnType.FLOAT)
        self.assertEqual(utils.numpy_dtype_to_celonis_coltype(df['date_col'].dtype), utils.ColumnType.DATETIME)
        self.assertEqual(utils.numpy_dtype_to_celonis_coltype(df['string_col'].dtype), utils.ColumnType.STRING)
        self.assertEqual(utils.numpy_dtype_to_celonis_coltype(df['bool_col'].dtype), utils.ColumnType.BOOLEAN)



        
