from pyflink.table import ( 
    EnvironmentSettings, 
    TableEnvironment
)
from libs.utils.files import FileUtils

class FlinkStreamingEnvironment:

    def __init__(self, job_name):
        self.job_name = job_name
        self.env_settings = self.get_env_settings()
        self.jar_path = "file:///opt/flink/opt/"
        self.job_configs_path = "/opt/flink/ops/configs/jobs.json"
        self.job_config = self.get_job_config(self.job_name)

    def get_env_settings(self):
        return EnvironmentSettings.in_streaming_mode()

    def get_table_streaming_environment(self)-> TableEnvironment:
        table_env = TableEnvironment.create(self.env_settings)
        table_env.get_config() \
        .set('pipeline.jars', self.get_jars_full_path()) \
        .set('python.fn-execution.bundle.time', '100000') \
        .set('python.fn-execution.bundle.size', '10') \
        .set('parallelism.default', '2')
        return table_env

    def get_job_config(self, job_name):
        return FileUtils.get_json_file(self.job_configs_path).get(job_name)

    def get_jars_full_path(self):
        comma = ';'
        full_str = ''
        jars = self.job_config.get('jars')
        for jar in jars:
            index = jars.index(jar) + 1
            if index == len(jars):
                comma = ''
            full_str = f'{full_str}{self.jar_path}{jar}{comma}'
        return full_str