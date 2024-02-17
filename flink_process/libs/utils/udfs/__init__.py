import random, string
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
    

    @staticmethod
    @udf(result_type=DataTypes.STRING())
    def get_site_token(length):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))
    
    @staticmethod
    @udf(result_type=DataTypes.INT())
    def get_num_of_slots():
        return random.randrange(1,5)
    
    @staticmethod
    @udf(result_type=DataTypes.STRING())
    def get_join_date():
        year = str(random.randrange(2020,2024))
        month = str(random.randrange(1,12))
        day = str(random.randrange(1,29))
        return year+'-'+month+'-'+day

    @staticmethod
    @udf(result_type=DataTypes.STRING())
    def generate_password(length):
        letters = string.ascii_lowercase
        return ''.join(random.choice(letters) for i in range(length))
    
    @staticmethod
    @udf(result_type=DataTypes.STRING())
    def get_customer_type_text(dict_type, id):
        suspended_dict = maps_helper.get('suspended_dict')
        customer_type_dict = maps_helper.get('customer_type_dict')
        abstract_dict = locals()[f'{dict_type}_dict']
        return abstract_dict.get(str(id))
    
    @staticmethod
    @udf(result_type=DataTypes.STRING())
    def get_stage_level_text(id):
        stage_level_dict = maps_helper.get('improves_stage_level')
        return stage_level_dict.get(id, None)

    @staticmethod
    @udf(result_type=DataTypes.INT())
    def get_stage_level_id(id):
        stage_level_dict = {
            8: 1,
            9: 2,
            10: 3,
        }
        return stage_level_dict.get(id, None)
    
    @staticmethod
    @udf(result_type=DataTypes.STRING())
    def get_improved_parts(id):
        improve_dict = maps_helper.get('improves')
        return str(improve_dict.get(id))
    
    @staticmethod
    @udf(result_type=DataTypes.STRING())
    def get_slots_type():
        index_usb_slot = random.randrange(0,2)
        slots_type_list = ['B', 'C']
        return slots_type_list[index_usb_slot]
    
    @staticmethod   
    @udf(result_type=DataTypes.STRING())
    def get_status_text(id):
        status_dict = maps_helper.get('status_text')
        return status_dict.get(str(id))
    