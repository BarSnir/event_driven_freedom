import random
from datetime import date
from pyflink.table.udf import udf
from pyflink.table import DataTypes
from libs.utils.udfs.maps import maps_helper

class FlinkUDFs:

    @staticmethod
    @udf(result_type=DataTypes.BIGINT())
    def calc_gemetria(text):
        total = 0
        clean_text = text.replace(' ','') \
        .replace('-','') \
        .replace('&', 'and') \
        .replace('.','') \
        .replace('(', '') \
        .replace(')', '') \
        .replace('/', '') \
        .lower()
        abc_gemetria = maps_helper.get('gemetria')
        for letter in clean_text:
            num = abc_gemetria.get(letter, None)
            if num is None:
                total = total + int(letter)
                continue
            total = total + num
        return total
    
    @staticmethod
    @udf(result_type=DataTypes.TINYINT())
    def get_mysql_boolean(col_condition_name, col_condition_value, dict_to_import=None):
        col_condition_dict = maps_helper.get(dict_to_import)
        col_condition = col_condition_dict.get(col_condition_name, None)
        if col_condition is not None and col_condition > col_condition_value:
            return 0
        return random.randrange(0, 2)
    
    @staticmethod
    @udf(result_type=DataTypes.INT())
    def get_int_range(text, dict_to_import=None):
        col_range_dict = maps_helper.get(dict_to_import)
        col_range = col_range_dict.get(text, 0)
        value = random.randrange(
            col_range.get('low'), 
            col_range.get('high')
        )
        return value
    
    @staticmethod 
    @udf(result_type=DataTypes.STRING())
    def generate_test_date():
        today = str(date.today())
        curr_year = today[:4]
        return curr_year+'-'+str(random.randrange(1,12))+'-'+str(random.randrange(1,29))