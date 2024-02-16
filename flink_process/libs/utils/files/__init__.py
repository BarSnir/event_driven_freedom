import json

class FileUtils:

    @staticmethod
    def get_json_file(path):
        with open(path) as json_file:
            return json.load(json_file)