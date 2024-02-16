
class ListUtils:

    @staticmethod
    def str_to_list(list_str: str, special_erases: list=None, separator: str=',')-> list:
        temp = list_str.replace(' ', '')
        if special_erases:
            for item in special_erases:
                temp = temp.replace(item, '')

        return temp.split(separator)